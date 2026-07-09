package se.oyabun.aelv

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.asFlow as publisherAsFlow
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.time.Duration
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber

/**
 * A cold, backpressure-first publisher of zero or more items of type [T].
 *
 * Each call to [subscribe] starts an independent execution of the source — there is no shared
 * state between subscribers.  Items are only produced when the subscriber has signalled demand
 * via `request(n)`, satisfying Reactive Streams §1.1.
 *
 * Obtain a [Many] via the factory functions on the companion object, or via operators on an
 * existing [Many], [One], or [Flow].
 *
 * Terminal consumption is done through [subscribe], [drain], [fold], [reduce], [toList],
 * [toSet], [first], or [last].
 */
class Many<T : Any> private constructor(
    internal val source: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
) : Publisher<T> {

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber, source)
        try {
            subscriber.onSubscribe(subscription)
        } catch (e: Throwable) {
            subscription.cancel()
            throw e
        }
    }

    /** Bridges this [Many] to a [Flow]. */
    fun asFlow(): Flow<T> = flow {
        var error: AelvException? = null
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> { emit(signal.value); Signal.Downstream.Request(1) }
                is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                is Signal.Upstream.Error    -> { error = signal.cause; Signal.Downstream.Cancel }
            }
        }
        error?.let { throw it }
    }

    internal suspend fun collect(
        action: suspend (T) -> Signal.Downstream,
    ): Either<Unit, AelvException> {
        var error: AelvException? = null
        try {
            source { signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> action(signal.value)
                    is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                    is Signal.Upstream.Error    -> { error = signal.cause; Signal.Downstream.Cancel }
                }
            }
        } catch (e: AelvException) {
            error = e
        } catch (e: Throwable) {
            error = UpstreamErrorException(e)
        }
        return error?.right() ?: Unit.left()
    }

    companion object {
        /**
         * Creates a [Many] that emits [items] in order then completes.
         * Cancellation is respected between items.
         */
        fun <T : Any> of(vararg items: T): Many<T> = Many { emit ->
            for (item in items) {
                if (emit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@Many
            }
            emit(Signal.Upstream.Complete)
        }

        /**
         * Creates a [Many] that emits all items from [iterable] in order then completes.
         * Cancellation is respected between items.
         */
        fun <T : Any> of(iterable: Iterable<T>): Many<T> = Many { emit ->
            for (item in iterable) {
                if (emit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@Many
            }
            emit(Signal.Upstream.Complete)
        }

        internal fun <T : Any> generate(
            block: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
        ): Many<T> = Many(block)

        /**
         * Bridges a [Flow] to a [Many].  The flow is collected on each subscription,
         * making this a cold source.
         */
        fun <T : Any> from(flow: Flow<T>): Many<T> = Many { emit ->
            try {
                coroutineScope {
                    flow.collect { item ->
                        if (emit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) cancel()
                    }
                }
                emit(Signal.Upstream.Complete)
            } catch (_: CancellationException) {}
        }

        /**
         * Bridges a Reactive Streams [Publisher] to a [Many].
         */
        fun <T : Any> from(publisher: Publisher<T>): Many<T> = from(publisher.publisherAsFlow())

        /** A [Many] that completes immediately without emitting any items. */
        fun <T : Any> empty(): Many<T> = Many { emit -> emit(Signal.Upstream.Complete) }

        /** A [Many] that immediately signals [cause] as an error. */
        fun <T : Any> error(cause: AelvException): Many<T> = Many { emit ->
            emit(Signal.Upstream.Error(cause))
        }

        /** A [Many] that never emits or completes.  Useful for testing and timeouts. */
        fun <T : Any> never(): Many<T> = Many { awaitCancellation() }

        /**
         * Emits an ever-increasing [Long] tick (0, 1, 2, …) every [period], starting after the
         * first period elapses.  Respects downstream demand: if the subscriber is slower than the
         * tick rate, emission suspends until demand arrives.
         */
        fun interval(period: Duration): Many<Long> = Many { emit ->
            var tick = 0L
            while (true) {
                delay(period)
                if (emit(Signal.Upstream.Next(tick++)) == Signal.Downstream.Cancel) return@Many
            }
        }
    }
}

/**
 * A cold, backpressure-first publisher of exactly one item of type [T].
 *
 * Semantically equivalent to a [Many] constrained to a single emission.  Each subscription
 * triggers an independent execution of the source.
 *
 * Suspend and await the result with [get], or chain further work with [map], [flatMap], and
 * the other operators defined on [One].
 */
class One<T : Any> private constructor(
    internal val source: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
) : Publisher<T> {

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber, source)
        try {
            subscriber.onSubscribe(subscription)
        } catch (e: Throwable) {
            subscription.cancel()
            throw e
        }
    }

    /** Bridges this [One] to a [Flow]. */
    fun asFlow(): Flow<T> = flow {
        var error: AelvException? = null
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> { emit(signal.value); Signal.Downstream.Request(1) }
                is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                is Signal.Upstream.Error    -> { error = signal.cause; Signal.Downstream.Cancel }
            }
        }
        error?.let { throw it }
    }

    /** Widens this [One] to a [Many] that emits the single value then completes. */
    fun asMany(): Many<T> = Many.generate { emit -> source { emit(it) } }

    internal suspend fun collect(
        action: suspend (T) -> Signal.Downstream,
    ): Either<Unit, AelvException> {
        var error: AelvException? = null
        try {
            source { signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> action(signal.value)
                    is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                    is Signal.Upstream.Error    -> { error = signal.cause; Signal.Downstream.Cancel }
                }
            }
        } catch (e: AelvException) {
            error = e
        } catch (e: Throwable) {
            error = UpstreamErrorException(e)
        }
        return error?.right() ?: Unit.left()
    }

    companion object {
        fun <T : Any> of(value: T): One<T> = One { emit ->
            if (emit(Signal.Upstream.Next(value)) != Signal.Downstream.Cancel)
                emit(Signal.Upstream.Complete)
        }

        /**
         * Creates a [One] that lazily evaluates [block] on each subscription and emits the result.
         * Exceptions thrown by [block] are propagated as [UpstreamErrorException].
         */
        fun <T : Any> defer(context: CoroutineContext? = null, block: suspend () -> T): One<T> = One { emit ->
            val value = if (context != null) withContext(currentCoroutineContext() + context) { block() } else block()
            if (emit(Signal.Upstream.Next(value)) != Signal.Downstream.Cancel)
                emit(Signal.Upstream.Complete)
        }

        internal fun <T : Any> generate(
            block: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
        ): One<T> = One(block)

        /**
         * Bridges a Reactive Streams [Publisher] to a [One] by taking its first emitted item.
         * Completes after the first item regardless of how many the publisher would produce.
         */
        fun <T : Any> from(publisher: Publisher<T>): One<T> = One { emit ->
            try {
                coroutineScope {
                    publisher.publisherAsFlow().collect { value ->
                        emit(Signal.Upstream.Next(value))
                        cancel()
                    }
                }
            } catch (_: CancellationException) {}
            emit(Signal.Upstream.Complete)
        }

        /** A [One] that immediately signals [cause] as an error. */
        fun <T : Any> error(cause: AelvException): One<T> = One { emit ->
            emit(Signal.Upstream.Error(cause))
        }

        /** A [One] that never emits or completes.  Useful for testing and timeouts. */
        fun <T : Any> never(): One<T> = One { awaitCancellation() }

        /**
         * Creates a [One] by invoking [block] with a callback pair: call [success] with a value
         * to emit it and complete, or call [failure] with a [Throwable] to signal an error.
         *
         * Designed for bridging callback-based async APIs (e.g. Kafka producer send):
         * ```kotlin
         * One.create { success, failure ->
         *     producer.send(record) { metadata, ex ->
         *         if (ex != null) failure(ex) else success(metadata.offset())
         *     }
         * }
         * ```
         *
         * The coroutine suspends until exactly one of [success] or [failure] is invoked.
         * Subsequent calls are ignored.  If the coroutine is cancelled before either callback
         * fires, cancellation propagates normally.
         */
        fun <T : Any> create(block: (success: (T) -> Unit, failure: (Throwable) -> Unit) -> Unit): One<T> =
            One.generate { emit ->
                val result = suspendCancellableCoroutine<Either<T, Throwable>> { cont ->
                    block(
                        { value -> cont.resume(value.left()) },
                        { cause -> cont.resume(cause.right()) },
                    )
                }
                when (result) {
                    is Either.Left  -> {
                        if (emit(Signal.Upstream.Next(result.value)) != Signal.Downstream.Cancel)
                            emit(Signal.Upstream.Complete)
                    }
                    is Either.Right -> emit(Signal.Upstream.Error(
                        result.value as? AelvException ?: UpstreamErrorException(result.value)
                    ))
                }
            }
    }
}

/**
 * A cold publisher that emits no items and signals only completion or error.
 *
 * Use [None] to represent async side-effects: operations that do work but produce no value.
 * Await completion with [await], or subscribe via the Reactive Streams [subscribe] method.
 */
class None<T : Any> private constructor(
    private val source: suspend () -> Unit,
) : Publisher<Nothing> {

    override fun subscribe(subscriber: Subscriber<in Nothing>) {
        val subscription = CompletionSubscription(subscriber, source)
        try {
            subscriber.onSubscribe(subscription)
        } catch (e: Throwable) {
            subscription.cancel()
            throw e
        }
    }

    /**
     * Suspends until this [None] completes or errors.
     *
     * Returns [Either.Left] on clean completion, or [Either.Right] containing the [AelvException]
     * if the source signalled an error.
     */
    suspend fun await(): Either<Unit, AelvException> = try {
        source()
        Unit.left()
    } catch (e: CancellationException) {
        throw e
    } catch (e: AelvException) {
        e.right()
    } catch (e: Throwable) {
        UpstreamErrorException(e).right()
    }

    companion object {

        /**
         * Creates a [None] that executes [block] on each subscription.
         * Exceptions thrown by [block] are propagated as errors.
         */
        fun <T : Any> defer(context: CoroutineContext? = null, block: suspend () -> Unit): None<T> = None {
            try {
                if (context != null) withContext(context) { block() } else block()
            } catch (e: CancellationException) {
                throw e
            } catch (e: AelvException) {
                throw e
            } catch (e: Throwable) {
                throw UpstreamErrorException(e)
            }
        }

        internal fun <T : Any> generate(block: suspend () -> Unit): None<T> = None(block)

        /**
         * Bridges a Reactive Streams [Publisher] to a [None] by draining all items and
         * completing when the publisher completes.
         */
        fun <T : Any> from(publisher: Publisher<T>): None<T> = None {
            publisher.publisherAsFlow().collect { }
        }

        /** A [None] that completes immediately. */
        fun <T : Any> complete(): None<T> = None { }

        /** A [None] that immediately signals [cause] as an error. */
        fun <T : Any> error(cause: AelvException): None<T> = None { throw cause }

        /** A [None] that never completes.  Useful for testing and timeouts. */
        fun <T : Any> never(): None<T> = None { awaitCancellation() }
    }
}
