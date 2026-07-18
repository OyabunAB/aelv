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
@file:Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")
@file:OptIn(ExperimentalTypeInference::class)
package se.oyabun.aelv

import kotlin.experimental.ExperimentalTypeInference
import kotlin.internal.LowPriorityInOverloadResolution
import kotlin.time.Duration
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import org.reactivestreams.Publisher

private val log = Logging.of<None<*>>()

/** Recovers from an error by substituting a fallback [None] pipeline. */
fun <T : Any> None<T>.recover(fallback: (Exception) -> None<T>): None<T> =
    None.generate {
        val result = await()
        if (result is Failure) fallback(result.value).await().let { if (it is Failure) throw it.value }
    }

/** Suspend variant of [recover][None.recover]. */
@LowPriorityInOverloadResolution
fun <T : Any> None<T>.recover(fallback: suspend (Exception) -> None<T>): None<T> =
    None.generate {
        val result = await()
        if (result is Failure) fallback(result.value).await().let { if (it is Failure) throw it.value }
    }

/**
 * Sequences this [None] with a [One] producer: awaits completion of the [None], then subscribes
 * to the [One] returned by [producer].
 *
 * If this [None] errors, [producer] is never called and the error is forwarded.  This is the
 * primary way to chain a fire-and-forget step before a value-producing step without nesting.
 */
@OverloadResolutionByLambdaReturnType
fun <T : Any, R : Any> None<T>.then(producer: () -> One<R>): One<R> =
    One.generate { emit ->
        val result = await()
        if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
        producer().source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [then] returning [One]. */
@LowPriorityInOverloadResolution
@OverloadResolutionByLambdaReturnType
fun <T : Any, R : Any> None<T>.then(producer: suspend () -> One<R>): One<R> =
    One.generate { emit ->
        val result = await()
        if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
        producer().source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Sequences this [None] with a [Maybe] producer.  The [Maybe] is only subscribed if this [None]
 * completes without error; an error in the [None] is forwarded and [producer] is skipped.
 */
@OverloadResolutionByLambdaReturnType
fun <T : Any, R : Any> None<T>.then(producer: () -> Maybe<R>): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        val result = await()
        if (result is Failure) { onError(result.value); return@Maybe }
        producer().source(onNext, onComplete, onError)
    }

/** Suspend variant of [then] returning [Maybe]. */
@LowPriorityInOverloadResolution
@OverloadResolutionByLambdaReturnType
fun <T : Any, R : Any> None<T>.then(producer: suspend () -> Maybe<R>): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        val result = await()
        if (result is Failure) { onError(result.value); return@Maybe }
        producer().source(onNext, onComplete, onError)
    }

/**
 * Sequences this [None] with a [Many] producer.  The [Many] is only subscribed if this [None]
 * completes without error; an error in the [None] terminates the stream without subscribing to
 * [producer].
 */
@OverloadResolutionByLambdaReturnType
fun <T : Any, R : Any> None<T>.then(producer: () -> Many<R>): Many<R> =
    Many.generate { emit ->
        val result = await()
        if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
        producer().source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [then] returning [Many]. */
@LowPriorityInOverloadResolution
@OverloadResolutionByLambdaReturnType
fun <T : Any, R : Any> None<T>.then(producer: suspend () -> Many<R>): Many<R> =
    Many.generate { emit ->
        val result = await()
        if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
        producer().source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Sequences two [None]s: awaits this one, then awaits the [None] returned by [producer].
 * Any error from either step is rethrown, short-circuiting the second step if the first fails.
 */
@OverloadResolutionByLambdaReturnType
fun <T : Any, R : Any> None<T>.then(producer: () -> None<R>): None<R> =
    None.generate {
        val result = await()
        if (result is Failure) throw result.value
        producer().await().let { if (it is Failure) throw it.value }
    }

/** Suspend variant of [then] returning [None]. */
@LowPriorityInOverloadResolution
@OverloadResolutionByLambdaReturnType
fun <T : Any, R : Any> None<T>.then(producer: suspend () -> None<R>): None<R> =
    None.generate {
        val result = await()
        if (result is Failure) throw result.value
        producer().await().let { if (it is Failure) throw it.value }
    }

fun <T : Any> None<T>.subscribeOn(context: CoroutineContext): None<T> =
    None.generate {
        withContext(currentCoroutineContext() + context) {
            val result = await()
            if (result is Failure) throw result.value
        }
    }

fun <T : Any> None<T>.publishOn(context: CoroutineContext): None<T> =
    toMany().publishOn(context).discard()

fun <T : Any, R : Any> None<T>.thenReturn(value: R): One<R>  = then { One.single(value) }

/**
 * Delays subscription to this [None] by [delay] before subscribing to the source.
 * The source is subscribed only after the delay has elapsed.
 */
fun <T : Any> None<T>.delaySubscription(delay: Duration): None<T> =
    None.generate {
        kotlinx.coroutines.delay(delay)
        val result = await()
        if (result is Failure) throw result.value
    }

/**
 * Delays subscription to this [None] until the [trigger] publisher emits an item or completes.
 * The trigger's first signal starts the subscription; the trigger itself is then cancelled.
 * If the trigger errors, the error is forwarded and this source is never subscribed.
 */
fun <T : Any> None<T>.delaySubscription(trigger: Publisher<*>): None<T> =
    None.generate {
        var triggerFailed: Exception? = null
        Many.from(trigger).source(
            { Signal.Downstream.Cancel },
            { },
            { cause -> triggerFailed = cause },
        )
        triggerFailed?.let { throw it }
        val result = await()
        if (result is Failure) throw result.value
    }
