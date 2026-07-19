/*
 * Copyright 2026 Oyabun AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        fun subscribing(name: String) = slf4j.trace { "$name subscribing" }
        fun completed(name: String) = slf4j.trace { "$name completed" }
        fun cancelled(name: String) = slf4j.trace { "$name cancelled" }
        fun error(name: String, cause: Throwable) = slf4j.warn(cause) { "$name terminated with error" }
    }

    inner class Operator {
        fun emitting(op: String, value: Any?) = slf4j.trace { "[$op] emitting $value" }
        fun dropping(op: String, value: Any?) = slf4j.trace { "[$op] dropping $value" }
        fun error(op: String, cause: Throwable) = slf4j.warn(cause) { "[$op] error" }
        fun retrying(op: String, attempt: Long, cause: Throwable) = slf4j.debug(cause) { "[$op] retrying (attempt $attempt)" }
        fun retryExhausted(op: String, cause: Throwable) = slf4j.warn(cause) { "[$op] retries exhausted" }
        fun sideEffectThrew(op: String, cause: Throwable) = slf4j.warn(cause) { "[$op] side-effect threw — ignoring" }
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
