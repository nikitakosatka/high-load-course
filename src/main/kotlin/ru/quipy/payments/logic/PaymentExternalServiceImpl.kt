package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.*
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        const val MAX_RETRY_ATTEMPTS = 1000
        const val HEDGED_DELAY_MS = 2400L // Задержка для hedged-запроса
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = LeakingBucketRateLimiter(
        rate = if (rateLimitPerSec > 3) rateLimitPerSec.toLong() - 1 else 1,
        window = Duration.ofSeconds(1),
        bucketSize = if (rateLimitPerSec > 3) rateLimitPerSec - 2 else 1
    )

    private val ongoingWindow = NonBlockingOngoingWindow(
        maxWinSize = parallelRequests,
    )

    private val client = OkHttpClient.Builder().build()

    private val executor = Executors.newCachedThreadPool()
    private val scheduler = Executors.newScheduledThreadPool(1)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        startRetry(paymentId, transactionId, paymentStartedAt, amount, deadline)
    }

    private data class RequestResult(val success: Boolean, val message: String?)

    private fun sendRequest(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        deadline: Long
    ): RequestResult {
        if (isDeadlineExceeded(deadline)) {
            val reason = "Request timeout."
            logger.error("[$accountName] Deadline exceeded for payment $paymentId, txId: $transactionId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason)
            }
            return RequestResult(false, reason)
        }

        delay()

        while (true) {
            if (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Success) {
                break
            }
        }

        try {
            val request = createRequest(paymentId, transactionId, amount)
            client.newCall(request).execute().use { response ->
                val (success, message) = processResponse(response, transactionId, paymentId)
                return RequestResult(success, message)
            }
        } catch (e: Exception) {
            val message = when (e) {
                is SocketTimeoutException -> "Request timeout."
                else -> e.message ?: "Unknown error"
            }
            return RequestResult(false, message)
        } finally {
            ongoingWindow.releaseWindow()
        }
    }

    private fun createRequest(paymentId: UUID, transactionId: UUID, amount: Int): Request {
        return Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
            build()
        }
    }

    private fun processResponse(response: Response, transactionId: UUID, paymentId: UUID): Pair<Boolean, String> {
        val body = try {
            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        val success = response.code in 200..299 && body.result
        val message = body.message ?: if (success) "Success" else "Failed"

        return Pair(success, message)
    }

    private fun startRetry(paymentId: UUID, transactionId: UUID, paymentStartedAt: Long, amount: Int, deadline: Long): Boolean {
        var attempt = 0

        while (attempt++ < MAX_RETRY_ATTEMPTS) {
            if (isDeadlineExceeded(deadline)) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                return false
            }

            val processed = AtomicBoolean(false)
            val mainFuture = CompletableFuture<RequestResult>()
            val hedgedFuture = CompletableFuture<RequestResult>()

            // Отправка основного запроса
            executor.submit {
                try {
                    val result = sendRequest(paymentId, transactionId, amount, deadline)
                    mainFuture.complete(result)
                } catch (e: Exception) {
                    mainFuture.completeExceptionally(e)
                }
            }

            // Планирование hedged-запроса
            scheduler.schedule({
                if (!mainFuture.isDone) {
                    executor.submit {
                        try {
                            val result = sendRequest(paymentId, transactionId, amount, deadline)
                            hedgedFuture.complete(result)
                        } catch (e: Exception) {
                            hedgedFuture.completeExceptionally(e)
                        }
                    }
                } else {
                    hedgedFuture.cancel(false)
                }
            }, HEDGED_DELAY_MS, TimeUnit.MILLISECONDS)

            try {
                val result = CompletableFuture.anyOf(mainFuture, hedgedFuture).get() as RequestResult
                if (processed.compareAndSet(false, true)) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(result.success, now(), transactionId, reason = result.message)
                    }
                    if (result.success) {
                        mainFuture.cancel(true)
                        hedgedFuture.cancel(true)
                        return true
                    }
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Error processing payment $paymentId: ${e.message}")
            }

            mainFuture.cancel(true)
            hedgedFuture.cancel(true)
        }

        return false
    }

    private fun isDeadlineExceeded(deadline: Long): Boolean {
        return LocalDateTime.now().isAfter(
            Instant.ofEpochMilli(deadline).atZone(ZoneId.systemDefault()).toLocalDateTime()
        )
    }

    private fun delay() {
        while (!rateLimiter.tick()) {
            Thread.sleep(10)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()