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

internal sealed interface Fusion<out T : Any> {
    data object None : Fusion<Nothing>
    abstract class Available<T : Any> : Fusion<T> {
        abstract fun create(): Available<T>
        abstract fun poll(): T?
    }
}

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
    internal val source: suspend (
        onNext: suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError: suspend (AelvException) -> Unit,
    ) -> Unit,
    internal val fusion: Fusion<T> = Fusion.None,
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
        source(
            { value -> emit(value); Signal.Downstream.Request },
            { },
            { e -> error = e },
        )
        error?.let { throw it }
    }

    internal suspend fun collect(
        action: suspend (T) -> Signal.Downstream,
    ): Either<Unit, AelvException> {
        val f = fusion
        if (f is Fusion.Available) {
            val poll = f.create()
            return try {
                while (true) {
                    val item = poll.poll() ?: break
                    if (action(item) == Signal.Downstream.Cancel) return Unit.left()
                }
                Unit.left()
            } catch (e: AelvException) {
                e.right()
            } catch (e: Throwable) {
                UpstreamErrorException(e).right()
            }
        }
        var error: AelvException? = null
        try {
            source(
                { value -> action(value) },
                { },
                { e -> error = e },
            )
        } catch (e: AelvException) {
            error = e
        } catch (e: Throwable) {
            error = UpstreamErrorException(e)
        }
        return error?.right() ?: Unit.left()
    }

    internal fun <R : Any> collectInto(initial: R, accumulate: (R, T) -> R): Either<R, AelvException>? {
        val f = fusion
        if (f !is Fusion.Available) return null
        val poll = f.create()
        return try {
            var acc = initial
            while (true) acc = accumulate(acc, poll.poll() ?: break)
            acc.left()
        } catch (e: AelvException) {
            e.right()
        } catch (e: Throwable) {
            UpstreamErrorException(e).right()
        }
    }

    companion object {
        /**
         * Creates a [Many] that emits [items] in order then completes.
         * Cancellation is respected between items.
         */
        fun <T : Any> of(vararg items: T): Many<T> = fused(
            fusion = ArrayFusion(items),
            block = { onNext, onComplete, _ ->
                for (item in items) {
                    if (onNext(item) == Signal.Downstream.Cancel) return@fused
                }
                onComplete()
            },
        )

        /**
         * Creates a [Many] that emits all items from [iterable] in order then completes.
         * Cancellation is respected between items.
         */
        fun <T : Any> of(iterable: Iterable<T>): Many<T> = fused(
            fusion = IterableFusion(iterable),
            block = { onNext, onComplete, _ ->
                for (item in iterable) {
                    if (onNext(item) == Signal.Downstream.Cancel) return@fused
                }
                onComplete()
            },
        )

        internal fun <T : Any> generate(
            block: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
        ): Many<T> = build { onNext, onComplete, onError ->
            block { signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> onNext(signal.value)
                    is Signal.Upstream.Complete -> { onComplete(); Signal.Downstream.Cancel }
                    is Signal.Upstream.Error    -> { onError(signal.cause); Signal.Downstream.Cancel }
                }
            }
        }

        internal fun <T : Any> build(
            block: suspend (
                onNext: suspend (T) -> Signal.Downstream,
                onComplete: suspend () -> Unit,
                onError: suspend (AelvException) -> Unit,
            ) -> Unit,
        ): Many<T> = Many(block)

        internal fun <T : Any> fused(
            fusion: Fusion<T>,
            block: suspend (
                onNext: suspend (T) -> Signal.Downstream,
                onComplete: suspend () -> Unit,
                onError: suspend (AelvException) -> Unit,
            ) -> Unit,
        ): Many<T> = Many(block, fusion)

        /**
         * Bridges a [Flow] to a [Many].  The flow is collected on each subscription,
         * making this a cold source.
         */
        fun <T : Any> from(flow: Flow<T>): Many<T> = build { onNext, onComplete, _ ->
            try {
                coroutineScope {
                    flow.collect { item ->
                        if (onNext(item) == Signal.Downstream.Cancel) cancel()
                    }
                }
                onComplete()
            } catch (_: CancellationException) {}
        }

        /**
         * Bridges a Reactive Streams [Publisher] to a [Many].
         */
        fun <T : Any> from(publisher: Publisher<T>): Many<T> = from(publisher.publisherAsFlow())

        /** Emits integers from [start] (inclusive) to [start] + [count] (exclusive) using a primitive counter. */
        fun range(start: Int, count: Int): Many<Int> {
            require(count >= 0) { "count must be non-negative, got $count" }
            return fused(
                fusion = RangeFusion(start, count),
                block = { onNext, onComplete, _ ->
                    val end = start.toLong() + count
                    var i = start.toLong()
                    while (i < end) {
                        if (onNext(i.toInt()) == Signal.Downstream.Cancel) return@fused
                        i++
                    }
                    onComplete()
                },
            )
        }

        /** Emits the given items in order then completes — zero-allocation vararg overload. */
        fun <T : Any> just(vararg items: T): Many<T> = of(*items)

        /** A [Many] that completes immediately without emitting any items. */
        fun <T : Any> empty(): Many<T> = build { _, onComplete, _ -> onComplete() }

        /** A [Many] that immediately signals [cause] as an error. */
        fun <T : Any> error(cause: AelvException): Many<T> = build { _, _, onError ->
            onError(cause)
        }

        /** A [Many] that never emits or completes.  Useful for testing and timeouts. */
        fun <T : Any> never(): Many<T> = build { _, _, _ -> awaitCancellation() }

        /**
         * Emits an ever-increasing [Long] tick (0, 1, 2, …) every [period], starting after the
         * first period elapses.  Respects downstream demand: if the subscriber is slower than the
         * tick rate, emission suspends until demand arrives.
         */
        fun interval(period: Duration): Many<Long> = build { onNext, onComplete, _ ->
            var tick = 0L
            while (true) {
                delay(period)
                if (onNext(tick++) == Signal.Downstream.Cancel) return@build
            }
            onComplete()
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
    internal val source: suspend (
        onNext: suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError: suspend (AelvException) -> Unit,
    ) -> Unit,
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
        source(
            { value -> emit(value); Signal.Downstream.Request },
            { },
            { e -> error = e },
        )
        error?.let { throw it }
    }

    /** Widens this [One] to a [Many] that emits the single value then completes. */
    fun asMany(): Many<T> = Many.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
        )
    }

    internal suspend fun collect(
        action: suspend (T) -> Signal.Downstream,
    ): Either<Unit, AelvException> {
        var error: AelvException? = null
        try {
            source(
                { value -> action(value) },
                { },
                { e -> error = e },
            )
        } catch (e: AelvException) {
            error = e
        } catch (e: Throwable) {
            error = UpstreamErrorException(e)
        }
        return error?.right() ?: Unit.left()
    }

    companion object {
        fun <T : Any> of(value: T): One<T> = One { onNext, onComplete, _ ->
            if (onNext(value) != Signal.Downstream.Cancel)
                onComplete()
        }

        /**
         * Creates a [One] that lazily evaluates [block] on each subscription and emits the result.
         * Exceptions thrown by [block] are propagated as [UpstreamErrorException].
         */
        fun <T : Any> defer(context: CoroutineContext? = null, block: suspend () -> T): One<T> = One { onNext, onComplete, _ ->
            val value = if (context != null) withContext(currentCoroutineContext() + context) { block() } else block()
            if (onNext(value) != Signal.Downstream.Cancel)
                onComplete()
        }

        internal fun <T : Any> generate(
            block: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
        ): One<T> = One { onNext, onComplete, onError ->
            block { signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> onNext(signal.value)
                    is Signal.Upstream.Complete -> { onComplete(); Signal.Downstream.Cancel }
                    is Signal.Upstream.Error    -> { onError(signal.cause); Signal.Downstream.Cancel }
                }
            }
        }

        /**
         * Bridges a Reactive Streams [Publisher] to a [One] by taking its first emitted item.
         * Completes after the first item regardless of how many the publisher would produce.
         */
        fun <T : Any> from(publisher: Publisher<T>): One<T> = One { onNext, onComplete, _ ->
            try {
                coroutineScope {
                    publisher.publisherAsFlow().collect { value ->
                        onNext(value)
                        cancel()
                    }
                }
            } catch (_: CancellationException) {}
            onComplete()
        }

        /** A [One] that immediately signals [cause] as an error. */
        fun <T : Any> error(cause: AelvException): One<T> = One { _, _, onError ->
            onError(cause)
        }

        /** A [One] that never emits or completes.  Useful for testing and timeouts. */
        fun <T : Any> never(): One<T> = One { _, _, _ -> awaitCancellation() }

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

private class RangeFusion(private val start: Int, private val count: Int) : Fusion.Available<Int>() {
    private val end: Long = start.toLong() + count
    private var current: Long = start.toLong()
    override fun create(): Fusion.Available<Int> = RangeFusion(start, count)
    override fun poll(): Int? = if (current < end) current++.toInt() else null
}

private class ArrayFusion<T : Any>(private val items: Array<out T>) : Fusion.Available<T>() {
    private var index = 0
    override fun create(): Fusion.Available<T> = ArrayFusion(items)
    override fun poll(): T? = if (index < items.size) items[index++] else null
}

private class IterableFusion<T : Any>(private val iterable: Iterable<T>) : Fusion.Available<T>() {
    private var iterator: Iterator<T> = iterable.iterator()
    override fun create(): Fusion.Available<T> = IterableFusion(iterable)
    override fun poll(): T? = if (iterator.hasNext()) iterator.next() else null
}

internal class MapFusion<T : Any, R : Any>(
    private val upstream: Fusion.Available<T>,
    private val transform: (T) -> R,
) : Fusion.Available<R>() {
    override fun create(): Fusion.Available<R> = MapFusion(upstream.create(), transform)
    override fun poll(): R? = upstream.poll()?.let(transform)
}

internal class FilterFusion<T : Any>(
    private val upstream: Fusion.Available<T>,
    private val predicate: (T) -> Boolean,
) : Fusion.Available<T>() {
    override fun create(): Fusion.Available<T> = FilterFusion(upstream.create(), predicate)
    override fun poll(): T? {
        while (true) {
            val item = upstream.poll() ?: return null
            if (predicate(item)) return item
        }
    }
}

internal class TakeFusion<T : Any>(
    private val upstream: Fusion.Available<T>,
    private val limit: Long,
) : Fusion.Available<T>() {
    private var remaining = limit
    override fun create(): Fusion.Available<T> = TakeFusion(upstream.create(), limit)
    override fun poll(): T? {
        if (remaining == 0L) return null
        val item = upstream.poll() ?: return null
        remaining--
        return item
    }
}
