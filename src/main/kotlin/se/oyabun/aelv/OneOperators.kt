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
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration

private val log = Logging.of<One<*>>()

fun <T : Any, R : Any> One<T>.map(transform: (T) -> R): One<R> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(transform(value))) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [map] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> One<T>.map(transform: suspend (T) -> R): One<R> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(transform(value))) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

fun <T : Any, R : Any> One<T>.flatMap(transform: (T) -> One<R>): One<R> =
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

/** Suspend variant of [flatMap] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
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
fun <T : Any, R : Any> One<T>.flatMapMany(transform: (T) -> Many<R>): Many<R> =
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

/** Suspend variant of [flatMapMany] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
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
fun <T : Any, R : Any> One<T>.flatMapMaybe(transform: (T) -> Maybe<R>): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> transform(value).source(onNext, onComplete, onError); Signal.Downstream.Cancel },
            onComplete,
            onError,
        )
    }

/** Suspend variant of [flatMapMaybe] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
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
fun <T : Any> One<T>.flatMapNone(transform: (T) -> None<*>): None<T> =
    flatMap { value -> transform(value).thenReturn(value) }.discard()

/** Suspend variant of [flatMapNone] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> One<T>.flatMapNone(transform: suspend (T) -> None<*>): None<T> =
    flatMap(transform = suspend { value: T -> transform(value).thenReturn(value) }).discard()

/** On error, emits the value returned by [fallback] and completes normally. */
fun <T : Any> One<T>.recover(fallback: (Exception) -> T): One<T> =
    One.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        if (result is Failure) {
            if (emit(Signal.Upstream.Next(fallback(result.value))) == Signal.Downstream.Cancel) return@generate
        }
        emit(Signal.Upstream.Complete)
    }

/** Suspend variant of [recover] — [fallback] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> One<T>.recover(fallback: suspend (Exception) -> T): One<T> =
    One.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        if (result is Failure) {
            if (emit(Signal.Upstream.Next(fallback(result.value))) == Signal.Downstream.Cancel) return@generate
        }
        emit(Signal.Upstream.Complete)
    }

/** Re-subscribes to the source on error, up to [times] times.  Defaults to unbounded retries. */
fun <T : Any> One<T>.retry(times: Long = Long.MAX_VALUE): One<T> =
    retry(Policy.retry().maxAttempts(times))

/**
 * Re-subscribes to the source on error according to [policy].
 * The policy controls the error filter, maximum attempt count, and backoff strategy.
 */
fun <T : Any> One<T>.retry(policy: Policy.Retry): One<T> =
    One.generate { emit ->
        var attempts = 0L
        while (true) {
            val result = collect { emit(Signal.Upstream.Next(it)) }
            when {
                result is Success                           -> break
                !policy.filter((result as Either.Left).value)  -> { emit(Signal.Upstream.Error(result.value)); return@generate }
                attempts >= policy.maxAttempts                  -> { log.operator.retryExhausted("retry", result.value); emit(Signal.Upstream.Error(result.value)); return@generate }
                else -> {
                    log.operator.retrying("retry", attempts, result.value)
                    val backoffDelay = policy.backoff.delayFor(attempts)
                    if (backoffDelay.isPositive()) delay(backoffDelay)
                    attempts++
                }
            }
        }
        emit(Signal.Upstream.Complete)
    }

fun <T : Any> One<T>.doOnNext(action: (T) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> runCatching { action(value) }.onFailure { e -> log.operator.sideEffectThrew("doOnNext", e) }; emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doOnNext] — [action] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> One<T>.doOnNext(action: suspend (T) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> runCatching { action(value) }.onFailure { e -> log.operator.sideEffectThrew("doOnNext", e) }; emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

fun <T : Any> One<T>.doOnError(action: (Exception) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> runCatching { action(issue) }.onFailure { e -> log.operator.sideEffectThrew("doOnError", e) }; emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doOnError] — [action] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> One<T>.doOnError(action: suspend (Exception) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> runCatching { action(issue) }.onFailure { e -> log.operator.sideEffectThrew("doOnError", e) }; emit(Signal.Upstream.Error(issue)) },
        )
    }

fun <T : Any> One<T>.doOnSubscribe(action: () -> Unit): One<T> =
    One.generate { emit ->
        runCatching { action() }.onFailure { e -> log.operator.sideEffectThrew("doOnSubscribe", e) }
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doOnSubscribe] for [One]. */
@LowPriorityInOverloadResolution
fun <T : Any> One<T>.doOnSubscribe(action: suspend () -> Unit): One<T> =
    One.generate { emit ->
        runCatching { action() }.onFailure { e -> log.operator.sideEffectThrew("doOnSubscribe", e) }
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

fun <T : Any> One<T>.doOnComplete(action: () -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { runCatching { action() }.onFailure { e -> log.operator.sideEffectThrew("doOnComplete", e) }; emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doOnComplete] for [One]. */
@LowPriorityInOverloadResolution
fun <T : Any> One<T>.doOnComplete(action: suspend () -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { runCatching { action() }.onFailure { e -> log.operator.sideEffectThrew("doOnComplete", e) }; emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Invokes [action] when the stream terminates for any reason: normal completion, error, or
 * downstream cancellation.
 */
fun <T : Any> One<T>.doFinally(action: (Signal.Terminal) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value ->
                val downstream = emit(Signal.Upstream.Next(value))
                if (downstream == Signal.Downstream.Cancel) action(Signal.Downstream.Cancel)
                downstream
            },
            { action(Signal.Upstream.Complete); emit(Signal.Upstream.Complete) },
            { issue -> action(Signal.Upstream.Error(issue)); emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doFinally] — [action] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> One<T>.doFinally(action: suspend (Signal.Terminal) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value ->
                val downstream = emit(Signal.Upstream.Next(value))
                if (downstream == Signal.Downstream.Cancel) action(Signal.Downstream.Cancel)
                downstream
            },
            { action(Signal.Upstream.Complete); emit(Signal.Upstream.Complete) },
            { issue -> action(Signal.Upstream.Error(issue)); emit(Signal.Upstream.Error(issue)) },
        )
    }

fun <T : Any> One<T>.publishOn(context: CoroutineContext): One<T> =
    One.generate { emit ->
        source(
            { value -> withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Next(value)) } },
            { withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Complete) } },
            { issue -> withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Error(issue)) } },
        )
    }

fun <T : Any> One<T>.subscribeOn(context: CoroutineContext): One<T> =
    One.generate { emit ->
        withContext(currentCoroutineContext() + context) {
            source(
                { value -> emit(Signal.Upstream.Next(value)) },
                { emit(Signal.Upstream.Complete) },
                { issue -> emit(Signal.Upstream.Error(issue)) },
            )
        }
    }

/**
 * Suspends until this [One] emits its value or signals an error.
 *
 * Returns [Either.Right] containing the value on success, or [Either.Left] containing the
 * [Exception] if the source errored or completed without emitting.
 */
suspend fun <T : Any> One<T>.await(): Either<Exception, T> {
    var result: Either<Unset, T> = Unset.left()
    val outcome = collect { value -> result = value.right(); Signal.Downstream.Cancel }
    val final = result
    return when {
        final  is Success -> final.value.right()
        outcome is Failure -> outcome
        else                   -> NoSuchElementException().left()
    }
}

/**
 * Suspends until this [One] emits its value or [timeout] elapses.
 *
 * Returns [Either.Right] with the value on success, or [Either.Left] with a
 * [TimeoutException] if the timeout elapsed before a value was emitted, or with the upstream
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
    var cached: Either<Unset, Either<Exception, T>> = Unset.left()
    return One.generate { emit ->
        val result: Either<Exception, T> = mutex.withLock {
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

fun <T : Any, R : Any> One<T>.thenReturn(value: R): One<R>   = map { value }
