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

class Verify<T : Any> private constructor(
    private val pipeline: Many<T>,
    private val context:  CoroutineContext = EmptyCoroutineContext,
    private val terminal: (Many<T>) -> One<Unit> = { many ->
        many.discard().thenReturn(Unit)
    },
) {

    fun assertNext(assertion: (T) -> Unit): Verify<T> =
        Verify(
            pipeline.firstMaybe()
                .or { error("expected at least one item but stream was empty") }
                .map { value -> assertion(value); value }
                .toMany(),
            context,
            terminal,
        )

    fun emitsNext(vararg values: T): Verify<T> =
        Verify(
            pipeline.take(values.size.toLong())
                .fold(emptyList<T>()) { acc, item -> acc + item }
                .map { actual ->
                    check(actual == values.toList()) { "expected ${values.toList()} but got $actual" }
                    actual
                }
                .flatMapMany { Many.empty() },
            context,
            terminal,
        )

    fun emitsCount(count: Long): Verify<T> =
        Verify(
            pipeline.fold(0L) { acc, _ -> acc + 1 }
                .map { actual -> check(actual == count) { "expected $count items but got $actual" }; actual }
                .flatMapMany { Many.empty() },
            context,
            terminal,
        )

    fun thenCancels(): Verify<T> = Verify(pipeline.take(0), context, terminal)

    private fun terminatesWith(expected: Signal.Terminal, within: Duration) {
        var signal: Signal.Terminal? = null
        val observed = pipeline.doFinally { signal = it }.let {
            if (expected is Signal.Downstream.Cancel) it.take(0) else it
        }
        val result = runBlocking(context + CoroutineName("verify")) {
            terminal(Verify(observed, context, terminal).pipeline).await(within)
        }
        when (expected) {
            is Signal.Upstream.Error   -> check(result is Failure) { "expected error but stream completed normally" }
            else                       -> check(result is Success) { "expected normal completion but got error: ${(result as Failure).value.message}" }
        }
        check(signal == expected) { "expected terminal $expected but got: $signal" }
    }

    fun completed(within: Duration = DEFAULT_TIMEOUT) = terminatesWith(Signal.completed, within)
    fun aborted(within: Duration = DEFAULT_TIMEOUT)   = terminatesWith(Signal.cancelled, within)

    fun failed(within: Duration = DEFAULT_TIMEOUT) {
        val result = runBlocking(context + CoroutineName("verify")) {
            terminal(pipeline).await(within)
        }
        if (result is Success) throw AssertionError("expected error but stream completed normally")
    }

    inline fun <reified X : Exception> failedWith(within: Duration = DEFAULT_TIMEOUT, noinline assertions: (X) -> Unit = {}) =
        failedWith(X::class.java, within, assertions)

    @Suppress("UNCHECKED_CAST")
    fun <X : Exception> failedWith(type: Class<X>, within: Duration = DEFAULT_TIMEOUT, assertions: (X) -> Unit = {}) {
        val result = runBlocking(context + CoroutineName("verify")) {
            terminal(pipeline).await(within)
        }
        if (result is Success) throw AssertionError("expected error but stream completed normally")
        val cause = (result as Failure).value
        if (!type.isInstance(cause)) throw AssertionError("expected ${type.simpleName} but got ${cause::class.simpleName}: ${cause.message}")
        assertions(type.cast(cause))
    }

    fun completesEmpty(within: Duration = DEFAULT_TIMEOUT) = runBlocking(context + CoroutineName("verify")) {
        pipeline
            .flatMapNone { value: T -> None.error<T>(IllegalStateException("expected empty but got: $value")) }
            .thenReturn(Unit)
            .await(within)
            .fold(
                onLeft  = { throw AssertionError("expected empty completion but got error: ${it.message}", it) },
                onRight = { },
            )
    }

    fun completesNormally(within: Duration = DEFAULT_TIMEOUT) = runBlocking(context + CoroutineName("verify")) {
        terminal(pipeline).await(within).fold(
            onLeft  = { throw AssertionError("expected normal completion but got error: ${it.message}", it) },
            onRight = { },
        )
    }

    fun verify(within: Duration = DEFAULT_TIMEOUT) = completesNormally(within)

    companion object {
        val DEFAULT_TIMEOUT = 5.seconds

        fun <T : Any> that(
            pipeline: Many<T>,
            context:  CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(pipeline, context)

        fun <T : Any> that(
            one:     One<T>,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(
            pipeline = one.toMany(),
            context  = context,
            terminal = { _ -> one.map { Unit } },
        )

        fun <T : Any> that(
            maybe:   Maybe<T>,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(
            pipeline = maybe.toMany(),
            context  = context,
            terminal = { pipeline ->
                One.generate { emit ->
                    val assertResult = pipeline.discard().thenReturn(Unit).await()
                    if (assertResult is Failure) { emit(Signal.Upstream.Error(assertResult.value)); emit(Signal.Upstream.Complete); return@generate }
                    var completed = false
                    var error: Exception? = null
                    maybe.source(
                        { Signal.Downstream.Request },
                        { completed = true },
                        { issue -> error = issue },
                    )
                    when {
                        error != null -> { emit(Signal.Upstream.Error(error)); emit(Signal.Upstream.Complete) }
                        completed     -> { emit(Signal.Upstream.Next(Unit)); emit(Signal.Upstream.Complete) }
                        else          -> emit(Signal.Upstream.Error(IllegalStateException("Maybe completed without signalling onComplete")))
                    }
                }
            },
        )

        fun <T : Any> that(
            none:    None<T>,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(
            pipeline = none.toMany(),
            context  = context,
            terminal = { _ -> none.thenReturn(Unit) },
        )
    }
}
