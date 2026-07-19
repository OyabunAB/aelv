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

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.runBlocking
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class Verify<T : Any, S : Observable<T, S>> private constructor(
    private val source:   S,
    private val pipeline: Observable<T, *>,
    private val context:  CoroutineContext = EmptyCoroutineContext,
) {

    fun assertNext(assertion: (T) -> Unit): Verify<T, S> =
        Verify(
            source,
            pipeline.toMany()
                .firstMaybe()
                .or { error("expected at least one item but stream was empty") }
                .map { value -> assertion(value); value }
                .toMany(),
            context,
        )

    fun emitsNext(vararg values: T): Verify<T, S> =
        Verify(
            source,
            pipeline.toMany()
                .take(values.size.toLong())
                .fold(emptyList<T>()) { acc, item -> acc + item }
                .map { actual ->
                    check(actual == values.toList()) { "expected ${values.toList()} but got $actual" }
                    actual
                }
                .flatMapMany { Many.empty() },
            context,
        )

    fun emitsCount(count: Long): Verify<T, S> =
        Verify(
            source,
            pipeline.toMany()
                .fold(0L) { acc, _ -> acc + 1 }
                .map { actual -> check(actual == count) { "expected $count items but got $actual" }; actual }
                .flatMapMany { Many.empty() },
            context,
        )

    private fun terminatesWith(expected: Signal.Terminal, within: Duration) {
        var signal: Signal.Terminal? = null
        val observed: Observable<T, *> = source.doFinally { signal = it }
        val result = runBlocking(context + CoroutineName("verify")) {
            if (expected is Signal.Downstream.Cancel) {
                observed.toMany().take(0).discard().thenReturn(Unit).await(within)
            } else {
                observed.discard().thenReturn(Unit).await(within)
            }
        }
        when (expected) {
            is Signal.Upstream.Error -> if (result is Success) throw AssertionError("expected error but stream completed normally")
            else -> result.fold(
                onLeft  = { throw AssertionError("expected normal termination but got error: ${it.message}", it) },
                onRight = { },
            )
        }
        check(signal == expected) { "expected terminal $expected but got: $signal" }
    }

    fun completed(within: Duration = DEFAULT_TIMEOUT) = terminatesWith(Signal.completed, within)
    fun aborted(within: Duration = DEFAULT_TIMEOUT)   = terminatesWith(Signal.cancelled, within)

    fun failed(within: Duration = DEFAULT_TIMEOUT) {
        val result = runBlocking(context + CoroutineName("verify")) {
            source.discard().thenReturn(Unit).await(within)
        }
        if (result is Success) throw AssertionError("expected error but stream completed normally")
    }

    inline fun <reified X : Exception> failedWith(within: Duration = DEFAULT_TIMEOUT, noinline assertions: (X) -> Unit = {}) =
        failedWith(X::class.java, within, assertions)

    @Suppress("UNCHECKED_CAST")
    fun <X : Exception> failedWith(type: Class<X>, within: Duration = DEFAULT_TIMEOUT, assertions: (X) -> Unit = {}) {
        val result = runBlocking(context + CoroutineName("verify")) {
            source.discard().thenReturn(Unit).await(within)
        }
        if (result is Success) throw AssertionError("expected error but stream completed normally")
        val cause = (result as Failure).value
        if (!type.isInstance(cause)) throw AssertionError("expected ${type.simpleName} but got ${cause::class.simpleName}: ${cause.message}")
        assertions(type.cast(cause))
    }

    fun timesOut(within: Duration = DEFAULT_TIMEOUT) = failedWith<TimeoutException>(within)

    fun completesEmpty(within: Duration = DEFAULT_TIMEOUT) = runBlocking(context + CoroutineName("verify")) {
        source.toMany()
            .flatMapNone { value: T -> None.error<T>(IllegalStateException("expected empty but got: $value")) }
            .thenReturn(Unit)
            .await(within)
            .fold(
                onLeft  = { throw AssertionError("expected empty completion but got error: ${it.message}", it) },
                onRight = { },
            )
    }

    fun completesNormally(within: Duration = DEFAULT_TIMEOUT) = runBlocking(context + CoroutineName("verify")) {
        pipeline.discard().thenReturn(Unit).await(within).fold(
            onLeft  = { throw AssertionError("expected normal completion but got error: ${it.message}", it) },
            onRight = { },
        )
    }

    fun verify(within: Duration = DEFAULT_TIMEOUT) = completesNormally(within)

    companion object {
        val DEFAULT_TIMEOUT = 5.seconds

        fun <T : Any, S : Observable<T, S>> that(
            source:  S,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T, S> = Verify(source, source, context)
    }
}
