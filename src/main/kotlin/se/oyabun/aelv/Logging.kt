package se.oyabun.aelv

import org.slf4j.LoggerFactory
import org.slf4j.Logger as Slf4jLogger

internal inline fun Slf4jLogger.trace(msg: () -> String) { if (isTraceEnabled) trace(msg()) }
internal inline fun Slf4jLogger.debug(msg: () -> String) { if (isDebugEnabled) debug(msg()) }
internal inline fun Slf4jLogger.info(msg: () -> String) { if (isInfoEnabled) info(msg()) }
internal inline fun Slf4jLogger.warn(msg: () -> String) { if (isWarnEnabled) warn(msg()) }
internal inline fun Slf4jLogger.warn(cause: Throwable, msg: () -> String) { if (isWarnEnabled) warn(msg(), cause) }
internal inline fun Slf4jLogger.error(cause: Throwable, msg: () -> String) { if (isErrorEnabled) error(msg(), cause) }
internal inline fun Slf4jLogger.debug(cause: Throwable, msg: () -> String) { if (isDebugEnabled) debug(msg(), cause) }
internal class Log(private val slf4j: Slf4jLogger) {

    val stream = Stream()
    val operator = Operator()
    val subscription = Subscription()

    inner class Stream {
        fun subscribing(name: String) = slf4j.debug { "$name subscribing" }
        fun completed(name: String) = slf4j.debug { "$name completed" }
        fun cancelled(name: String) = slf4j.debug { "$name cancelled" }
        fun error(name: String, cause: Throwable) = slf4j.warn(cause) { "$name terminated with error" }
    }

    inner class Operator {
        fun emitting(op: String, value: Any?) = slf4j.trace { "[$op] emitting $value" }
        fun dropping(op: String, value: Any?) = slf4j.trace { "[$op] dropping $value" }
        fun error(op: String, cause: Throwable) = slf4j.warn(cause) { "[$op] error" }
        fun retrying(op: String, attempt: Long, cause: Throwable) = slf4j.debug(cause) { "[$op] retrying (attempt $attempt)" }
        fun retryExhausted(op: String, cause: Throwable) = slf4j.warn(cause) { "[$op] retries exhausted" }
    }

    inner class Subscription {
        fun requested(name: String, n: Long) = slf4j.trace { "[$name] request($n)" }
        fun cancelled(name: String) = slf4j.trace { "[$name] cancel()" }
        fun backpressure(name: String) = slf4j.debug { "[$name] backpressure — downstream slow" }
    }
}

internal object Logging {
    inline fun <reified T : Any> of(): Log = Log(LoggerFactory.getLogger(T::class.java))
}
