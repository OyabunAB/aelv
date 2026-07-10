package se.oyabun.aelv

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
    onError: (AelvException) -> Unit,
    onComplete: () -> Unit = {},
): Disposable {
    require(prefetch > 0) { "prefetch must be positive, got $prefetch" }
    var subscription: Subscription? = null
    this.subscribe(object : org.reactivestreams.Subscriber<T> {
        private var consumed = 0L
        private val threshold = (prefetch / 2).coerceAtLeast(1L)

        override fun onSubscribe(s: Subscription) {
            subscription = s
            s.request(prefetch)
        }

        override fun onNext(t: T) {
            onNext(t)
            if (++consumed >= threshold) {
                consumed = 0L
                subscription?.request(threshold)
            }
        }

        override fun onError(t: Throwable) {
            val error = t as? AelvException ?: UpstreamErrorException(t)
            try {
                onError(error)
            } catch (e: Exception) {
                log.stream.error("subscriber.onError", e)
            }
        }

        override fun onComplete() = onComplete()
    })
    return object : Disposable {
        override fun cancel() = subscription?.cancel() ?: Unit
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
    onError: (AelvException) -> Unit,
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
            var acc = initial
            collect { value -> acc = accumulate(acc, value); Signal.Downstream.Request }
                .mapLeft { acc }
        }
        when (result) {
            is Either.Left  -> { emit(Signal.Upstream.Next(result.value)); emit(Signal.Upstream.Complete) }
            is Either.Right -> emit(Signal.Upstream.Error(result.value))
        }
    }

/**
 * Reduces all items to a single value by applying [accumulate] pairwise.
 *
 * Returns a [One] that emits `Either.Left<T>` on success, or `Either.Right<AelvException>`
 * if the stream was empty or errored.
 */
fun <T : Any> Many<T>.reduce(accumulate: (T, T) -> T): One<Either<T, AelvException>> =
    One.generate { emit ->
        var acc: Any = Unset
        val result = collect { item ->
            @Suppress("UNCHECKED_CAST")
            acc = if (acc === Unset) item else accumulate(acc as T, item)
            Signal.Downstream.Request
        }
        val value: Either<T, AelvException> = when {
            result is Either.Right -> result
            acc === Unset          -> NoSuchElementException().right()
            else                   -> {
                @Suppress("UNCHECKED_CAST")
                (acc as T).left()
            }
        }
        emit(Signal.Upstream.Next(value))
        emit(Signal.Upstream.Complete)
    }

/** Collects all items into an immutable [List]. */
fun <T : Any> Many<T>.toList(): One<List<T>> =
    One.generate { emit ->
        val fused = collectInto(mutableListOf<T>()) { acc, item -> acc.also { it.add(item) } }
        val outcome = fused ?: run {
            val result = mutableListOf<T>()
            collect { value -> result.add(value); Signal.Downstream.Request }.mapLeft { result }
        }
        when (outcome) {
            is Either.Left  -> { emit(Signal.Upstream.Next(outcome.value as List<T>)); emit(Signal.Upstream.Complete) }
            is Either.Right -> emit(Signal.Upstream.Error(outcome.value))
        }
    }

/** Collects all items into an immutable [Set], removing duplicates. */
fun <T : Any> Many<T>.toSet(): One<Set<T>> =
    One.generate { emit ->
        val fused = collectInto(mutableSetOf<T>()) { acc, item -> acc.also { it.add(item) } }
        val outcome = fused ?: run {
            val result = mutableSetOf<T>()
            collect { value -> result.add(value); Signal.Downstream.Request }.mapLeft { result }
        }
        when (outcome) {
            is Either.Left  -> { emit(Signal.Upstream.Next(outcome.value as Set<T>)); emit(Signal.Upstream.Complete) }
            is Either.Right -> emit(Signal.Upstream.Error(outcome.value))
        }
    }

/**
 * Suspends until the first item is emitted, then cancels the subscription.
 *
 * Returns [Either.Left] with the first item, or [Either.Right] with a [NoSuchElementException]
 * if the stream was empty, or with the upstream error if the stream errored.
 */
suspend fun <T : Any> Many<T>.first(): Either<T, AelvException> {
    var result: Any = Unset
    val outcome = collect { value ->
        result = value
        Signal.Downstream.Cancel
    }
    return when {
        result !== Unset        -> {
            @Suppress("UNCHECKED_CAST")
            (result as T).left()
        }
        outcome is Either.Right -> outcome
        else                    -> NoSuchElementException().right()
    }
}

/**
 * Suspends until the stream completes, then returns the last emitted item.
 *
 * Returns [Either.Left] with the last item, or [Either.Right] with a [NoSuchElementException]
 * if the stream was empty, or with the upstream error if the stream errored.
 */
suspend fun <T : Any> Many<T>.last(): Either<T, AelvException> {
    var result: Any = Unset
    val outcome = collect { value ->
        result = value
        Signal.Downstream.Request
    }
    return when {
        result !== Unset        -> {
            @Suppress("UNCHECKED_CAST")
            (result as T).left()
        }
        outcome is Either.Right -> outcome
        else                    -> NoSuchElementException().right()
    }
}
