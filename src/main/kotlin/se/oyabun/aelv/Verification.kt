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
 * The terminal methods require an explicit [within] duration — how long
 * this specific assertion is expected to complete in.
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
) {

    fun assertNext(assertion: (T) -> Unit): Verify<T> =
        Verify(
            pipeline.firstMaybe()
                .or { error("expected at least one item but stream was empty") }
                .map { value -> assertion(value); value }
                .toMany(),
            context,
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
        )

    fun emitsCount(count: Long): Verify<T> =
        Verify(
            pipeline.fold(0L) { acc, _ -> acc + 1 }
                .map { actual -> check(actual == count) { "expected $count items but got $actual" }; actual }
                .flatMapMany { Many.empty() },
            context,
        )

    fun thenCancels(): Verify<T> = Verify(pipeline.take(0), context)

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
        pipeline.discard().thenReturn(Unit).await(within).fold(
            onLeft  = { throw AssertionError("expected normal completion but got error: ${it.message}", it) },
            onRight = { },
        )
    }

    fun completesWithError(within: Duration = DEFAULT_TIMEOUT): Exception = runBlocking(context + CoroutineName("verify")) {
        pipeline.discard().thenReturn(Unit).await(within).fold(
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
        ): Verify<T> = Verify(one.toMany(), context)

        fun <T : Any> that(
            maybe:   Maybe<T>,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(maybe.toMany(), context)

        fun <T : Any> that(
            none:    None<T>,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(none.toMany(), context)
    }
}


