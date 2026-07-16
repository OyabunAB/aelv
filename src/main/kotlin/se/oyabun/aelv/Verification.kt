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
 * The terminals ([completesNormally], [completesWithError], [completesEmpty])
 * subscribe via [runBlocking] — the only blocking point.
 */
class Verify<T : Any> private constructor(
    private val pipeline: Many<T>,
    private val timeout:  Duration        = 5.seconds,
    private val context:  CoroutineContext = EmptyCoroutineContext,
) {

    fun assertNext(assertion: (T) -> Unit): Verify<T> =
        Verify(
            pipeline.firstMaybe()
                .or { error("expected at least one item but stream was empty") }
                .map { value -> assertion(value); value }
                .toMany(),
            timeout, context,
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
            timeout, context,
        )

    fun emitsCount(count: Long): Verify<T> =
        Verify(pipeline.take(count), timeout, context)

    fun thenCancels(): Verify<T> =
        Verify(pipeline.take(0), timeout, context)

    fun completesEmpty() = runBlocking(context + CoroutineName("verify")) {
        pipeline
            .flatMapNone { value: T -> None.error<T>(IllegalStateException("expected empty but got: $value")) }
            .await()
            .getOrThrow()
    }

    fun completesNormally() = runBlocking(context + CoroutineName("verify")) {
        pipeline.discard().await().getOrThrow()
    }

    fun completesWithError(): Exception = runBlocking(context + CoroutineName("verify")) {
        pipeline.discard().await().fold(
            onLeft  = { it },
            onRight = { error("expected error but stream completed normally") },
        )
    }

    fun verify() = completesNormally()

    companion object {
        fun <T : Any> that(
            pipeline: Many<T>,
            timeout:  Duration        = 5.seconds,
            context:  CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(pipeline, timeout, context)

        fun <T : Any> that(
            one:     One<T>,
            timeout: Duration        = 5.seconds,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(one.toMany(), timeout, context)

        fun <T : Any> that(
            maybe:   Maybe<T>,
            timeout: Duration        = 5.seconds,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(maybe.toMany(), timeout, context)

        fun <T : Any> that(
            none:    None<T>,
            timeout: Duration        = 5.seconds,
            context: CoroutineContext = EmptyCoroutineContext,
        ): Verify<T> = Verify(none.toMany(), timeout, context)
    }
}

fun <T : Any> Verify<T>.isAbsent() = completesEmpty()
