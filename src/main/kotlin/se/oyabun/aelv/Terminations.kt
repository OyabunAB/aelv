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

package se.oyabun.aelv

import kotlin.internal.LowPriorityInOverloadResolution
import org.reactivestreams.Subscription

/**
 * A handle returned by [subscribe] and [drain] that allows cancelling an active subscription.
 */
interface Disposable {
    /** Cancels the subscription, stopping item delivery. */
    fun cancel()
}

private val log = Logging.of<Disposable>()

/**
 * Subscribes to this [Many] with backpressure replenishment.
 *
 * Requests [prefetch] items upfront, then re-requests `prefetch / 2` items each time that
 * threshold is consumed — keeping the pipeline full without unbounded buffering.
 *
 * @param prefetch Initial demand and replenishment batch size.  Must be positive.
 * @param onNext   Called for each item.
 * @param onError  Called on error.  Exceptions thrown by this callback are logged and swallowed.
 * @param onComplete Called on normal completion.
 * @return A [Disposable] that can be used to cancel the subscription.
 */
fun <T : Any> Many<T>.subscribe(
    prefetch: Long,
    onNext: (T) -> Unit,
    onError: (Exception) -> Unit,
    onComplete: () -> Unit = {},
): Disposable {
    require(prefetch > 0) { "prefetch must be positive, got $prefetch" }
    var subscription: SubscriptionState = SubscriptionState.Unbound
    var cancelled = false
    this.subscribe(object : org.reactivestreams.Subscriber<T> {
        private var consumed = 0L
        private val threshold = (prefetch / 2).coerceAtLeast(1L)

        override fun onSubscribe(s: Subscription) {
            subscription = SubscriptionState.Bound(s)
            if (cancelled) { s.cancel(); return }
            s.request(prefetch)
        }
        override fun onNext(t: T) {
            onNext(t)
            if (++consumed >= threshold) {
                consumed = 0L
                when (val state = subscription) {
                    is SubscriptionState.Bound   -> state.subscription.request(threshold)
                    is SubscriptionState.Unbound -> Unit
                }
            }
        }

        override fun onError(t: Throwable) {
            val error = if (t is Exception) t else RuntimeException(t)
            Either.catchingStrict { onError(error) }
                .onLeft { issue -> log.stream.error("subscriber.onError", issue) }
        }

        override fun onComplete() = onComplete()
    })
    return object : Disposable {
        override fun cancel() = when (val state = subscription) {
            is SubscriptionState.Bound   -> state.subscription.cancel()
            is SubscriptionState.Unbound -> { cancelled = true }
        }
    }
}

/**
 * Subscribes to this [Many] with unbounded demand — equivalent to `subscribe(Long.MAX_VALUE, ...)`.
 *
 * Use only when the source is known to be bounded or when backpressure is handled upstream.
 *
 * @param onNext     Called for each item.
 * @param onError    Called on error.
 * @param onComplete Called on normal completion.
 * @return A [Disposable] that can be used to cancel the subscription.
 */
fun <T : Any> Many<T>.drain(
    onNext: (T) -> Unit,
    onError: (Exception) -> Unit,
    onComplete: () -> Unit = {},
): Disposable = subscribe(
    prefetch = Long.MAX_VALUE,
    onNext = onNext,
    onError = onError,
    onComplete = onComplete,
)

/**
 * Accumulates all items into a single result by applying [accumulate] from [initial].
 * Returns a [One] that emits the final accumulated value.
 */
fun <T : Any, R : Any> Many<T>.fold(initial: R, accumulate: (R, T) -> R): One<R> =
    One.generate { emit ->
        val fused = collectInto(initial, accumulate)
        val result = fused ?: run {
            var accumulator = initial
            collect { value -> accumulator = accumulate(accumulator, value); Signal.Downstream.Request }
                .mapRight { accumulator }
        }
        when (result) {
            is Success  -> { emit(Signal.Upstream.Next(result.value)); emit(Signal.Upstream.Complete) }
            is Failure -> emit(Signal.Upstream.Error(result.value))
        }
    }

@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Many<T>.fold(initial: R, accumulate: suspend (R, T) -> R): One<R> =
    One.generate { emit ->
        var accumulator = initial
        val result = collect { value -> accumulator = accumulate(accumulator, value); Signal.Downstream.Request }
        when (result) {
            is Success  -> { emit(Signal.Upstream.Next(accumulator)); emit(Signal.Upstream.Complete) }
            is Failure -> emit(Signal.Upstream.Error(result.value))
        }
    }

/**
 * Reduces all items to a single value by applying [accumulate] pairwise.
 *
 * Returns a [One] that emits the reduced value, or signals [NoSuchElementException] if the
 * stream was empty, or propagates the upstream error if the stream errored.
 */
fun <T : Any> Many<T>.reduce(accumulate: (T, T) -> T): One<T> =
    One.generate { emit ->
        var accumulator: Either<Unset, T> = Unset.left()
        val result = collect { item ->
            accumulator = when (val current = accumulator) {
                is Failure  -> item.right()
                is Success -> accumulate(current.value, item).right()
            }
            Signal.Downstream.Request
        }
        val final = accumulator
        when (result) {
            is Failure  -> emit(Signal.Upstream.Error(result.value))
            is Success -> when (final) {
                is Failure  -> emit(Signal.Upstream.Error(NoSuchElementException()))
                is Success -> { emit(Signal.Upstream.Next(final.value)); emit(Signal.Upstream.Complete) }
            }
        }
    }

/** Collects all items into an immutable [List]. */
fun <T : Any> Many<T>.toList(): One<List<T>> =
    One.generate { emit ->
        val fused = collectInto(mutableListOf<T>()) { accumulator, item -> accumulator.also { it.add(item) } }
        val outcome = fused ?: run {
            val result = mutableListOf<T>()
            collect { value -> result.add(value); Signal.Downstream.Request }.mapRight { result }
        }
        when (outcome) {
            is Success  -> { emit(Signal.Upstream.Next(outcome.value.toList())); emit(Signal.Upstream.Complete) }
            is Failure -> emit(Signal.Upstream.Error(outcome.value))
        }
    }

/** Collects all items into an immutable [Set], removing duplicates. */
fun <T : Any> Many<T>.toSet(): One<Set<T>> =
    One.generate { emit ->
        val fused = collectInto(mutableSetOf<T>()) { accumulator, item -> accumulator.also { it.add(item) } }
        val outcome = fused ?: run {
            val result = mutableSetOf<T>()
            collect { value -> result.add(value); Signal.Downstream.Request }.mapRight { result }
        }
        when (outcome) {
            is Success  -> { emit(Signal.Upstream.Next(outcome.value.toSet())); emit(Signal.Upstream.Complete) }
            is Failure -> emit(Signal.Upstream.Error(outcome.value))
        }
    }

/**
 * Suspends until the first item is emitted then cancels the subscription.
 *
 * Returns [Either.Right] with the first item, or [Either.Left] with [NoSuchElementException]
 * if the stream was empty, or with the upstream error if the stream errored.
 */
fun <T : Any> Many<T>.first(): One<T> {
    val currentFusion = fusion
    return One.fromStep(
        step = Step.Suspend { onNext, onComplete, onError ->
            var result: Either<Unset, T> = Unset.left()
            val outcome = collect { value -> result = value.right(); Signal.Downstream.Cancel }
            val final = result
            when {
                final   is Success -> { onNext(final.value); onComplete() }
                outcome is Failure -> onError(outcome.value)
                else               -> onError(NoSuchElementException())
            }
        },
        fusion = if (currentFusion is Fusion.Available) TakeFusion(currentFusion, 1) else Fusion.None,
    )
}

fun <T : Any> Many<T>.last(): One<T> =
    One.generate { emit ->
        var result: Either<Unset, T> = Unset.left()
        val outcome = collect { value -> result = value.right(); Signal.Downstream.Request }
        val final = result
        when {
            final   is Success -> { emit(Signal.Upstream.Next(final.value)); emit(Signal.Upstream.Complete) }
            outcome is Failure -> emit(Signal.Upstream.Error(outcome.value))
            else               -> emit(Signal.Upstream.Error(NoSuchElementException()))
        }
    }
