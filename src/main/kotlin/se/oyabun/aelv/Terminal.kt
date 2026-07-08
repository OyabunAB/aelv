package se.oyabun.aelv

import org.reactivestreams.Subscription

interface Disposable {
    fun cancel()
}

private val log = Logging.of<Disposable>()

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
            } catch (e: Throwable) {
                log.stream.error("subscribe.onError handler threw", e)
            }
        }

        override fun onComplete() = onComplete()
    })
    return object : Disposable {
        override fun cancel() = subscription?.cancel() ?: Unit
    }
}

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

fun <T : Any, R : Any> Many<T>.fold(initial: R, accumulate: (R, T) -> R): One<R> =
    One.generate { emit ->
        var acc = initial
        val result = this.collect { acc = accumulate(acc, it) }
        when (result) {
            is Either.Left -> emit(acc)
            is Either.Right -> throw result.value
        }
    }

fun <T : Any> Many<T>.reduce(accumulate: (T, T) -> T): One<Either<T, AelvException>> =
    One.generate { emit ->
        var acc: Any = Unset
        val result = this.collect { item ->
            @Suppress("UNCHECKED_CAST")
            acc = if (acc === Unset) item else accumulate(acc as T, item)
        }
        when {
            result is Either.Right -> emit(result)
            acc === Unset -> emit(NoSuchElementException().right())
            else -> {
                @Suppress("UNCHECKED_CAST")
                emit((acc as T).left())
            }
        }
    }

fun <T : Any> Many<T>.toList(): One<List<T>> = fold(emptyList()) { acc, item -> acc + item }

fun <T : Any> Many<T>.toSet(): One<Set<T>> = fold(emptySet()) { acc, item -> acc + item }

suspend fun <T : Any> Many<T>.first(): Either<T, AelvException> {
    var result: Any = Unset
    val outcome = this.take(1).collect { result = it }
    return when {
        result !== Unset -> {
            @Suppress("UNCHECKED_CAST")
            (result as T).left()
        }
        outcome is Either.Right -> outcome
        else -> NoSuchElementException().right()
    }
}

suspend fun <T : Any> Many<T>.last(): Either<T, AelvException> {
    var result: Any = Unset
    val outcome = this.collect { result = it }
    return when {
        result !== Unset -> {
            @Suppress("UNCHECKED_CAST")
            (result as T).left()
        }
        outcome is Either.Right -> outcome
        else -> NoSuchElementException().right()
    }
}
