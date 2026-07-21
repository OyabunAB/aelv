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
import kotlinx.coroutines.CancellationException

fun <T : Any, R : Any> Maybe<T>.map(transform: (T) -> R): Maybe<R> {
    val currentFusion = fusion
    return Maybe.fromStep(Step.Map(step, transform), if (currentFusion is Fusion.Available) MapFusion(currentFusion, transform) else Fusion.None)
}

@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Maybe<T>.map(transform: suspend (T) -> R): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> onNext(transform(value)) },
            onComplete,
            onError,
        )
    }

/** Keeps the value if [predicate] returns true, otherwise produces an empty [Maybe]. */
fun <T : Any> Maybe<T>.filter(predicate: (T) -> Boolean): Maybe<T> {
    val currentFusion = fusion
    return Maybe.fromStep(Step.Filter(step, predicate), if (currentFusion is Fusion.Available) FilterFusion(currentFusion, predicate) else Fusion.None)
}

@LowPriorityInOverloadResolution
fun <T : Any> Maybe<T>.filter(predicate: suspend (T) -> Boolean): Maybe<T> =
    Maybe { onNext, onComplete, onError ->
        var emitComplete = false
        source(
            { value -> if (predicate(value)) onNext(value) else { emitComplete = true; Signal.Downstream.Cancel } },
            onComplete,
            onError,
        )
        // Call onComplete after source() returns, not from inside the onNext callback — RS §1.3.
        if (emitComplete) onComplete()
    }

fun <T : Any, R : Any> Maybe<T>.flatMap(transform: suspend (T) -> Maybe<R>): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> transform(value).source(onNext, onComplete, onError); Signal.Downstream.Cancel },
            onComplete,
            onError,
        )
    }

/**
 * Maps the present value to a [Many] and subscribes to it; if this [Maybe] is empty the result
 * completes empty without invoking [transform].
 *
 * The absent case propagates as an empty [Many] rather than an error, so callers cannot
 * distinguish between "Maybe was empty" and "inner Many was empty" at the output level — both
 * yield a [Many] that completes with zero items.
 */
fun <T : Any, R : Any> Maybe<T>.flatMapMany(transform: suspend (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
        source(
            { value ->
                transform(value).source(
                    { inner -> emit(Signal.Upstream.Next(inner)) },
                    { emit(Signal.Upstream.Complete) },
                    { issue -> emit(Signal.Upstream.Error(issue)) },
                )
                Signal.Downstream.Cancel
            },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Maps the present value to a [None] and awaits completion; if this [Maybe] is empty, completes
 * immediately without invoking [transform].
 *
 * Useful for fire-and-forget side effects that should be skipped when no value is present.
 */
fun <T : Any> Maybe<T>.flatMapNone(transform: suspend (T) -> None<T>): None<T> =
    toMany().flatMapNone(transform)

/**
 * Provides a fallback value when this [Maybe] is empty, producing a [One].
 *
 * If this [Maybe] emits a value, that value is forwarded. If it completes empty,
 * [fallback] is invoked and its result is emitted.
 */
fun <T : Any> Maybe<T>.or(fallback: suspend () -> T): One<T> =
    One.generate { emit ->
        var emitted = false
        source(
            { value -> emitted = true; emit(Signal.Upstream.Next(value)) },
            {
                if (!emitted) {
                    emit(Signal.Upstream.Next(fallback()))
                }
                emit(Signal.Upstream.Complete)
            },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Switches to [fallback] stream when this [Maybe] is empty.
 *
 * If this [Maybe] emits a value, that value is forwarded and [fallback] is never subscribed.
 * If it completes empty, [fallback] is subscribed and its items are forwarded.
 */
fun <T : Any> Maybe<T>.orMany(fallback: suspend () -> Many<T>): Many<T> =
    Many.generate { emit ->
        var emitted = false
        source(
            { value -> emitted = true; emit(Signal.Upstream.Next(value)) },
            {
                if (!emitted) {
                    fallback().source(
                        { inner -> emit(Signal.Upstream.Next(inner)) },
                        { emit(Signal.Upstream.Complete) },
                        { issue -> emit(Signal.Upstream.Error(issue)) },
                    )
                } else {
                    emit(Signal.Upstream.Complete)
                }
            },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Converts to a [One], throwing [NoElementException] if this [Maybe] is empty.
 *
 * Use [or] when the empty case is expected and a fallback is available.
 */
fun <T : Any> Maybe<T>.toOne(): One<T> =
    One.defer {
        var result: T? = null
        collect { value -> result = value; Signal.Downstream.Cancel }
        result ?: throw NoElementException()
    }

suspend fun <T : Any> Maybe<T>.await(): Either<Exception, T?> {
    val currentFusion = fusion
    if (currentFusion is Fusion.Available) {
        val poll = currentFusion.create(kotlin.coroutines.EmptyCoroutineContext)
        if (poll != null) return try {
            poll.poll().right()
        } catch (e: CancellationException) { throw e } catch (e: Exception) { e.left() }
    }
    return Either.catching {
        var result: T? = null
        source(
            { value -> result = value; Signal.Downstream.Cancel },
            { },
            ::rethrow,
        )
        result
    }
}
