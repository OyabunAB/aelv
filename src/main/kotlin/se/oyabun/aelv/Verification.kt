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

/**
 * Pipeline-based verifier for aelv publishers.
 *
 * Each step returns a new [Verify] wrapping a transformed pipeline.
 * Terminal methods subscribe through the original publisher type so that
 * missing terminal signals (e.g. a [Maybe] that never calls onComplete) are
 * detected rather than masked by type conversions.
 *
 * ```kotlin
 * Verify.that(pipeline)
 *     .assertNext { assertEquals(1, it) }
 *     .completesNormally(within = 100.milliseconds)
 * ```
 */
class Verify<T : Any> private constructor(
    private val pipeline: Many<T>,
    private val context:  CoroutineContext = EmptyCoroutineContext,
    // Executes the terminal subscription and returns a One<Unit> that resolves
    // on completion or fails with the stream error.  Defaults to the Many pipeline;
    // overridden for One/Maybe/None so their terminal signals are tested on the
    // source type directly rather than through a type-conversion wrapper.
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

    fun completesWithError(within: Duration = DEFAULT_TIMEOUT): Exception = runBlocking(context + CoroutineName("verify")) {
        terminal(pipeline).await(within).fold(
            onLeft  = { it },
            onRight = { throw AssertionError("expected error but stream completed normally") },
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
                // Step 1: run intermediate pipeline assertions via the Many pipeline.
                // Step 2: re-subscribe to the Maybe directly to verify its terminal signal.
                // Cold sources produce fresh state on each subscription, so this is correct.
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
