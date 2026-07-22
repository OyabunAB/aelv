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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.CancellationException
import kotlin.time.Duration

private typealias Cached<T> = Either<Unset, T>

fun <T : Any, R : Any> One<T>.map(transform: (T) -> R): One<R> {
    val currentFusion = fusion
    return One.fromStep(Step.Map(step, transform), if (currentFusion is Fusion.Available) MapFusion(currentFusion, transform) else Fusion.None)
}

@LowPriorityInOverloadResolution
fun <T : Any, R : Any> One<T>.map(transform: suspend (T) -> R): One<R> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(transform(value))) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Maps the single value to another [One] and subscribes to it, forwarding the result downstream.
 *
 * If this [One] errors, the error is forwarded without calling [transform].
 */
fun <T : Any, R : Any> One<T>.flatMap(transform: suspend (T) -> One<R>): One<R> =
    One.generate { emit ->
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
 * Maps the single value to a [Many] and subscribes to it, forwarding all items downstream.
 *
 * The result type widens from [One] to [Many] because the inner stream can emit zero or more items.
 * If the inner [Many] itself errors, the error propagates and no further items are emitted.
 */
fun <T : Any, R : Any> One<T>.flatMapMany(transform: suspend (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
        source(
            { value ->
                val result = transform(value).collect { inner -> emit(Signal.Upstream.Next(inner)) }
                if (result is Failure) emit(Signal.Upstream.Error(result.value))
                else emit(Signal.Upstream.Complete)
                Signal.Downstream.Cancel
            },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Maps the single value to a [Maybe], which may or may not emit a result.
 *
 * Use this when the mapping step can legitimately produce no value — the result is a [Maybe]
 * rather than a [One], reflecting that the downstream may complete empty.
 * If this [One] errors, the error is forwarded without calling [transform].
 */
fun <T : Any, R : Any> One<T>.flatMapMaybe(transform: suspend (T) -> Maybe<R>): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> transform(value).source(onNext, onComplete, onError); Signal.Downstream.Cancel },
            onComplete,
            onError,
        )
    }

/**
 * Maps the single value to a [None] and awaits its completion, discarding the result type.
 *
 * The return type is [None] because the entire chain produces no items — only a completion or
 * error signal.  Any error from the inner [None] is rethrown and terminates the outer stream.
 */
fun <T : Any> One<T>.flatMapNone(transform: suspend (T) -> None<*>): None<T> =
    flatMap { value -> transform(value).thenReturn(value) }.discard()

/**
 * Suspends until this [One] emits its value or signals an error.
 *
 * Returns [Either.Right] containing the value on success, or [Either.Left] containing the
 * [Exception] if the source errored or completed without emitting.
 */
suspend fun <T : Any> One<T>.await(): Either<Exception, T> {
    val currentFusion = fusion
    if (currentFusion is Fusion.Available) {
        val poll = currentFusion.create(kotlin.coroutines.EmptyCoroutineContext)
        if (poll != null) return try {
            (poll.poll() ?: throw NoElementException()).right()
        } catch (e: CancellationException) { throw e } catch (e: Exception) { e.left() }
    }
    var result: Either<Unset, T> = Unset.left()
    val outcome = collect { value -> result = value.right(); Signal.Downstream.Cancel }
    val final = result
    return when {
        final  is Success -> final.value.right()
        outcome is Failure -> outcome
        else                   -> NoElementException().left()
    }
}

/**
 * Suspends until this [One] emits its value or [timeout] elapses.
 *
 * Returns [Either.Right] with the value on success, or [Either.Left] with a
 * [ExceededTimeoutException] if the timeout elapsed before a value was emitted, or with the upstream
 * [Exception] if the source errored.
 */
suspend fun <T : Any> One<T>.await(timeout: Duration): Either<Exception, T> =
    Either.catching(timeout) { await().rightOrThrow() }

/**
 * Returns a [One] that executes the upstream source at most once and replays the result to every
 * subscriber.  The first subscriber triggers execution; subsequent subscribers receive the cached
 * result immediately without re-executing the source.
 *
 * Thread-safe: a [Mutex] ensures only one subscriber runs the source even under concurrent
 * subscriptions.
 */
fun <T : Any> One<T>.cache(): One<T> {
    val mutex  = Mutex()
    var cached: Cached<Outcome<T>> = Unset.left()
    return One.generate { emit ->
        val result: Outcome<T> = mutex.withLock {
            when (val cachedResult = cached) {
                is Failure  -> await().also { cached = it.right() }
                is Success -> cachedResult.value
            }
        }
        when (result) {
            is Success -> {
                if (emit(Signal.Upstream.Next(result.value)) != Signal.Downstream.Cancel)
                    emit(Signal.Upstream.Complete)
            }
            is Failure  -> emit(Signal.Upstream.Error(result.value))
        }
    }
}

fun <A : Any, B : Any, R : Any> One<A>.zipWith(other: One<B>, transform: (A, B) -> R): One<R> =
    zip(this, other, transform)

fun <T : Any> One<T>.concatWith(other: One<T>): Many<T> = concat(toMany(), other.toMany())
