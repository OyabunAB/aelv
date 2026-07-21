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

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.resume
import kotlin.time.Duration
import java.util.concurrent.atomic.AtomicReference
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber

typealias OnNext<T>  = suspend (T)         -> Signal.Downstream
typealias OnComplete = suspend ()          -> Unit
typealias OnError    = suspend (Exception) -> Unit

internal sealed interface Fusion<out T : Any> {
    data object None : Fusion<Nothing>
    abstract class Available<T : Any> : Fusion<T> {
        abstract fun create(context: CoroutineContext = EmptyCoroutineContext): Available<T>?
        abstract fun poll(): T?
        open fun connectSource(upstream: Available<*>): Fusion<T> = None
    }
}

/**
 * A cold, backpressure-first publisher of zero or more items of type [T].
 *
 * The [Step] ADT node is evaluated by the heap-allocated trampoline interpreter,
 * giving O(1) stack depth for arbitrary operator chains. The [Fusion] fast path
 * is used when the entire chain is fused, bypassing the interpreter entirely.
 */
class Many<T : Any> private constructor(
    override val step: Step<T>,
    internal val fusion: Fusion<T> = Fusion.None,
) : Publisher<T>, Observable<T, Many<T>>() {

    override fun wrap(
        block: suspend (
            onNext:     OnNext<T>,
            onComplete: OnComplete,
            onError:    OnError,
        ) -> Unit,
    ): Many<T> = Many.fused(block = block)

    override fun toMany(): Many<T> = this
    override fun toMaybe(): Maybe<T> = Maybe.fromStep(step, fusion)

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber, ::source)
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    fun asFlow(): Flow<T> = flow {
        source(
            { value -> emit(value); Signal.Downstream.Request },
            { },
            ::rethrow,
        )
    }

    override internal suspend fun collect(
        action: OnNext<T>,
    ): Either<Exception, Unit> {
        val currentFusion = fusion
        if (currentFusion is Fusion.Available) {
            val poll = currentFusion.create(currentCoroutineContext())
            if (poll != null) return Either.catching {
                tailrec suspend fun drain() {
                    val value = poll.poll() ?: return
                    if (action(value) != Signal.Downstream.Cancel) drain()
                }
                drain()
            }
        }
        return when (val result = interpret(step, Frame.Collect(action))) {
            is Success -> Unit.right()
            is Failure -> result.value.left()
        }
    }

    internal fun <R : Any> collectInto(initial: R, accumulate: (R, T) -> R): Either<Exception, R>? {
        val currentFusion = fusion
        if (currentFusion !is Fusion.Available) return null
        val poll = currentFusion.create(EmptyCoroutineContext) ?: return null
        return Either.catchingStrict {
            tailrec fun drainInto(acc: R): R {
                val value = poll.poll() ?: return acc
                return drainInto(accumulate(acc, value))
            }
            drainInto(initial)
        }
    }

    companion object {

        fun <T : Any> items(vararg items: T): Many<T> =
            Many(Step.Items(items), ArrayFusion(items))

        fun <T : Any> from(iterable: Iterable<T>): Many<T> =
            Many(Step.FromIterable(iterable), IterableFusion(iterable))

        fun <T : Any> from(flow: Flow<T>): Many<T> =
            Many(Step.FromFlow(flow))

        fun <T : Any> from(publisher: Publisher<T>): Many<T> =
            Many(Step.FromPublisher(publisher))

        fun range(start: Int, count: Int): Many<Int> {
            require(count >= 0) { "count must be non-negative, got $count" }
            return Many(Step.Range(start, count), RangeFusion(start, count))
        }

        fun <T : Any> empty(): Many<T> = Many(Step.Empty)

        fun <T : Any> error(cause: Exception): Many<T> = Many(Step.Error(cause))

        fun <T : Any> never(): Many<T> = Many(Step.Never)

        fun <T : Any> defer(factory: () -> Many<T>): Many<T> = Many(Step.Defer(factory))

        fun <T : Any> defer(factory: suspend () -> Many<T>): Many<T> =
            fused { onNext, onComplete, onError ->
                factory().source(onNext, onComplete, onError)
            }

        fun <T : Any> pipelineFrom(): Many<T> = Many(Step.PipelineSource(), SourceFusion())

        fun interval(period: Duration): Many<Long> {
            require(period.isPositive()) { "interval period must be positive, got $period" }
            return fused { onNext, _, _ ->
                tailrec suspend fun tick(n: Long) {
                    delay(period)
                    if (onNext(n) != Signal.Downstream.Cancel) tick(n + 1)
                }
                tick(0L)
            }
        }

        internal fun <T : Any> fused(
            fusion: Fusion<T> = Fusion.None,
            block: suspend (
                onNext:     OnNext<T>,
                onComplete: OnComplete,
                onError:    OnError,
            ) -> Unit,
        ): Many<T> = Many(Step.Suspend(block), fusion)

        internal fun <T : Any> generate(
            block: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
        ): Many<T> = fused { onNext, onComplete, onError ->
            block { signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> onNext(signal.value)
                    is Signal.Upstream.Complete -> { onComplete(); Signal.Downstream.Cancel }
                    is Signal.Upstream.Error    -> { onError(signal.cause); Signal.Downstream.Cancel }
                }
            }
        }

        internal fun <T : Any> fromStep(step: Step<T>, fusion: Fusion<T> = Fusion.None): Many<T> =
            Many(step, fusion)

        @Suppress("UNCHECKED_CAST")
        internal fun <A : Any, B : Any> concurrentFlatMapSuspend(
            upstreamStep: Step<A>,
            concurrency: Int,
            transform: (A) -> Many<B>,
        ): suspend (
            onNext:     OnNext<B>,
            onComplete: OnComplete,
            onError:    OnError,
        ) -> Unit {
            val upstream = Many(upstreamStep)
            return { onNext, onComplete, onError ->
                val semaphore = Semaphore(concurrency)
                val mutex     = Mutex()
                var cancelled = false
                val outerError = AtomicReference<Any>(Unset)
                coroutineScope {
                    upstream.source(
                        { value ->
                            if (cancelled) return@source Signal.Downstream.Cancel
                            semaphore.withPermit {
                                transform(value).source(
                                    { inner ->
                                        mutex.withLock {
                                            if (cancelled) Signal.Downstream.Cancel
                                            else {
                                                val downstream = onNext(inner)
                                                if (downstream == Signal.Downstream.Cancel) cancelled = true
                                                downstream
                                            }
                                        }
                                    },
                                    {},
                                    { issue -> outerError.compareAndSet(Unset, issue) },
                                )
                            }
                            Signal.Downstream.Request
                        },
                        {},
                        { issue -> outerError.compareAndSet(Unset, issue) },
                    )
                }
                val error = outerError.get()
                when {
                    cancelled       -> {}
                    error.isError() -> onError(error.asError())
                    else            -> onComplete()
                }
            }
        }
    }
}

/**
 * A cold publisher of exactly one item of type [T].
 *
 * If the source emits zero items the subscriber receives only `onComplete` without `onNext`.
 * If it emits more than one item, all items after the first are silently consumed.
 */
class One<T : Any> private constructor(
    override val step: Step<T>,
    internal val fusion: Fusion<T> = Fusion.None,
) : Publisher<T>, Observable<T, One<T>>() {

    override fun wrap(
        block: suspend (
            onNext:     OnNext<T>,
            onComplete: OnComplete,
            onError:    OnError,
        ) -> Unit,
    ): One<T> = One(Step.Suspend(block))

    override fun toMany(): Many<T> = Many.fromStep(step, fusion)
    override fun toMaybe(): Maybe<T> = Maybe.fromStep(step, fusion)

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber, ::source)
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    fun asFlow(): Flow<T> = flow {
        source(
            { value -> emit(value); Signal.Downstream.Request },
            { },
            ::rethrow,
        )
    }

    fun asMany(): Many<T> = Many.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

    companion object {
        fun <T : Any> single(value: T): One<T> = One(Step.Suspend { onNext, onComplete, _ ->
            if (onNext(value) != Signal.Downstream.Cancel) onComplete()
        })

        /**
         * Creates a [One] that executes [closure] on each subscription, emitting its return value.
         * Exceptions thrown by [closure] are caught and routed to [onError].
         * Use [context] to shift execution to a specific [CoroutineContext].
         */
        fun <T : Any> defer(context: CoroutineContext? = null, closure: suspend () -> T): One<T> =
            One(Step.Suspend { onNext, onComplete, _ ->
                val value = if (context != null) withContext(currentCoroutineContext() + context) { closure() } else closure()
                if (onNext(value) != Signal.Downstream.Cancel) onComplete()
            })

        internal fun <T : Any> generate(
            block: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
        ): One<T> = One(Step.Suspend { onNext, onComplete, onError ->
            block { signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> onNext(signal.value)
                    is Signal.Upstream.Complete -> { onComplete(); Signal.Downstream.Cancel }
                    is Signal.Upstream.Error    -> { onError(signal.cause); Signal.Downstream.Cancel }
                }
            }
        })

        internal operator fun <T : Any> invoke(
            block: suspend (
                onNext:     OnNext<T>,
                onComplete: OnComplete,
                onError:    OnError,
            ) -> Unit,
        ): One<T> = One(Step.Suspend(block))

        @Suppress("UNCHECKED_CAST")
        fun <T : Any> from(publisher: Publisher<T>): One<T> = One(Step.Suspend { onNext, onComplete, onError ->
            when (publisher) {
                is Many<*> -> (publisher as Many<T>).source(
                    { value -> onNext(value); Signal.Downstream.Cancel },
                    onComplete,
                    onError,
                )
                is One<*>  -> (publisher as One<T>).source(onNext, onComplete, onError)
                else       -> { publisher.asFlow().collectCancelling { value -> onNext(value); false }; onComplete() }
            }
        })

        fun <T : Any> error(cause: Exception): One<T> = One(Step.Error(cause))

        fun <T : Any> never(): One<T> = One(Step.Never)

        fun <T : Any> pipelineFrom(): One<T> = One(Step.PipelineSource())

        internal fun <T : Any> fromStep(step: Step<T>, fusion: Fusion<T> = Fusion.None): One<T> = One(step, fusion)

        fun <T : Any> create(block: (success: (T) -> Unit, failure: (Exception) -> Unit) -> Unit): One<T> =
            One.generate { emit ->
                val result = suspendCancellableCoroutine<Either<Exception, T>> { continuation ->
                    var emitted = false
                    fun emit(value: Either<Exception, T>) {
                        check(!emitted) { "One.create: callback called more than once" }
                        emitted = true
                        continuation.resume(value)
                    }
                    block(
                        { value -> emit(value.right()) },
                        { cause -> emit(cause.left()) },
                    )
                }
                when (result) {
                    is Success -> {
                        if (emit(Signal.Upstream.Next(result.value)) != Signal.Downstream.Cancel)
                            emit(Signal.Upstream.Complete)
                    }
                    is Failure -> emit(Signal.Upstream.Error(result.value))
                }
            }
    }
}

/**
 * A cold publisher of zero or one items of type [T].
 *
 * The contract: [source] calls [onNext] at most once, then calls [onComplete].
 * If no value is available it calls [onComplete] directly without calling [onNext].
 * On error it calls [onError] instead of [onComplete].
 */
class Maybe<T : Any> private constructor(
    override val step: Step<T>,
    internal val fusion: Fusion<T> = Fusion.None,
) : Publisher<T>, Observable<T, Maybe<T>>() {

    override fun wrap(
        block: suspend (
            onNext:     OnNext<T>,
            onComplete: OnComplete,
            onError:    OnError,
        ) -> Unit,
    ): Maybe<T> = Maybe(Step.Suspend(block))

    override fun toMany(): Many<T> = Many.fromStep(step, fusion)

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber, ::source)
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    companion object {

        fun <T : Any> present(value: T): Maybe<T> = Maybe(Step.Suspend { onNext, onComplete, _ ->
            if (onNext(value) != Signal.Downstream.Cancel) onComplete()
        })

        fun <T : Any> empty(): Maybe<T> = Maybe(Step.Empty)

        fun <T : Any> error(cause: Exception): Maybe<T> = Maybe(Step.Error(cause))

        fun <T : Any> never(): Maybe<T> = Maybe(Step.Never)

        /**
         * Creates a [Maybe] that executes [closure] on each subscription.
         * If [closure] returns a non-null value, that value is emitted and the stream completes.
         * If [closure] returns null, the stream completes empty.
         * Exceptions are caught and routed to [onError].
         */
        fun <T : Any> defer(closure: suspend () -> T?): Maybe<T> = Maybe(Step.Suspend { onNext, onComplete, onError ->
            try {
                val value = closure()
                if (value != null) {
                    if (onNext(value) != Signal.Downstream.Cancel) onComplete()
                } else {
                    onComplete()
                }
            } catch (exception: Exception) {
                onError(exception)
            }
        })

        fun <T : Any> from(publisher: Publisher<T>): Maybe<T> = Maybe(Step.Suspend { onNext, onComplete, onError ->
            var emitted = false
            try {
                publisher.asFlow().collectCancelling { value ->
                    if (!emitted) {
                        emitted = true
                        onNext(value) == Signal.Downstream.Cancel
                    } else {
                        true
                    }
                }
                onComplete()
            } catch (exception: Exception) {
                onError(exception)
            }
        })

        internal fun <T : Any> generate(
            block: suspend (emit: suspend (Signal.Upstream<T>) -> Signal.Downstream) -> Unit,
        ): Maybe<T> = Maybe(Step.Suspend { onNext, onComplete, onError ->
            block { signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> onNext(signal.value)
                    is Signal.Upstream.Complete -> { onComplete(); Signal.Downstream.Cancel }
                    is Signal.Upstream.Error    -> { onError(signal.cause); Signal.Downstream.Cancel }
                }
            }
        })

        internal operator fun <T : Any> invoke(
            block: suspend (
                onNext:     OnNext<T>,
                onComplete: OnComplete,
                onError:    OnError,
            ) -> Unit,
        ): Maybe<T> = Maybe(Step.Suspend(block))

        internal fun <T : Any> fromStep(step: Step<T>, fusion: Fusion<T> = Fusion.None): Maybe<T> =
            Maybe(step, if (fusion is Fusion.Available) TakeFusion(fusion, 1) else fusion)
    }
}

/**
 * A cold publisher of no items of type [T].
 *
 * The type parameter [T] exists only for type-system compatibility and carries no values.
 * Use `None<Unit>` when the type is irrelevant.
 */
class None<T : Any> private constructor(
    override val step: Step<T>,
) : Publisher<Nothing>, Observable<T, None<T>>() {

    override fun wrap(
        block: suspend (
            onNext:     OnNext<T>,
            onComplete: OnComplete,
            onError:    OnError,
        ) -> Unit,
    ): None<T> = None(Step.Suspend(block))

    override fun subscribe(subscriber: Subscriber<in Nothing>) {
        val subscription = CompletionSubscription(subscriber) {
            val result = await()
            if (result is Failure) throw result.value
        }
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    companion object {
        /**
         * Creates a [None] that executes [closure] as a side-effect on each subscription.
         * Exceptions thrown by [closure] are caught and routed to [onError].
         * Use [context] to shift execution to a specific [CoroutineContext].
         */
        fun <T : Any> defer(context: CoroutineContext? = null, closure: suspend () -> Unit): None<T> =
            None(Step.Suspend { _, onComplete, onError ->
                try {
                    if (context != null) withContext(currentCoroutineContext() + context) { closure() } else closure()
                    onComplete()
                } catch (e: Exception) {
                    onError(e)
                }
            })

        internal fun <T : Any> generate(closure: suspend () -> Unit): None<T> =
            None(Step.Suspend { _, onComplete, onError ->
                try { closure(); onComplete() } catch (e: Exception) { onError(e) }
            })

        fun <T : Any> from(publisher: Publisher<T>): None<T> =
            None(Step.Suspend { _, onComplete, onError ->
                try { publisher.asFlow().collect { }; onComplete() } catch (e: Exception) { onError(e) }
            })

        fun <T : Any> complete(): None<T> = None(Step.Empty)
        fun <T : Any> error(cause: Exception): None<T> = None(Step.Error(cause))
        fun <T : Any> never(): None<T> = None(Step.Never)

        fun <T : Any> pipelineFrom(): None<T> = None(Step.PipelineSource())

        internal operator fun <T : Any> invoke(closure: suspend () -> Unit): None<T> =
            generate(closure)
    }

    suspend fun await(): Either<Exception, Unit> = collect { Signal.Downstream.Request }
}


private class RangeFusion(private val start: Int, private val count: Int) : Fusion.Available<Int>() {
    private val end: Long = start.toLong() + count
    private var current: Long = start.toLong()
    override fun create(context: CoroutineContext): Fusion.Available<Int> = RangeFusion(start, count)
    override fun poll(): Int? = if (current < end) current++.toInt() else null
}

private class ArrayFusion<T : Any>(private val items: Array<out T>) : Fusion.Available<T>() {
    private var index = 0
    override fun create(context: CoroutineContext): Fusion.Available<T> = ArrayFusion(items)
    override fun poll(): T? = if (index < items.size) items[index++] else null
}

private class IterableFusion<T : Any>(private val iterable: Iterable<T>) : Fusion.Available<T>() {
    private var iterator: Iterator<T> = iterable.iterator()
    override fun create(context: CoroutineContext): Fusion.Available<T> = IterableFusion(iterable)
    override fun poll(): T? = if (iterator.hasNext()) iterator.next() else null
}

internal class MapFusion<T : Any, R : Any>(
    internal val upstream: Fusion.Available<T>,
    internal val transform: (T) -> R,
) : Fusion.Available<R>() {
    override fun create(context: CoroutineContext): Fusion.Available<R>? =
        upstream.create(context)?.let { MapFusion(it, transform) }
    override fun poll(): R? = upstream.poll()?.let(transform)
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<R> {
        val connected = this.upstream.connectSource(upstream)
        return if (connected is Fusion.Available) MapFusion(connected, transform) else Fusion.None
    }
}

internal class FilterFusion<T : Any>(
    internal val upstream: Fusion.Available<T>,
    internal val predicate: (T) -> Boolean,
) : Fusion.Available<T>() {
    override fun create(context: CoroutineContext): Fusion.Available<T>? =
        upstream.create(context)?.let { FilterFusion(it, predicate) }
    override fun poll(): T? {
        tailrec fun next(): T? {
            val value = upstream.poll() ?: return null
            return if (predicate(value)) value else next()
        }
        return next()
    }
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<T> {
        val connected = this.upstream.connectSource(upstream)
        return if (connected is Fusion.Available) FilterFusion(connected, predicate) else Fusion.None
    }
}

internal class TakeFusion<T : Any>(
    internal val upstream: Fusion.Available<T>,
    internal val limit: Long,
) : Fusion.Available<T>() {
    private var remaining = limit
    override fun create(context: CoroutineContext): Fusion.Available<T>? =
        upstream.create(context)?.let { TakeFusion(it, limit) }
    override fun poll(): T? {
        if (remaining == 0L) return null
        val value = upstream.poll() ?: return null
        remaining--
        return value
    }
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<T> {
        val connected = this.upstream.connectSource(upstream)
        return if (connected is Fusion.Available) TakeFusion(connected, limit) else Fusion.None
    }
}

internal class SourceFusion<T : Any> : Fusion.Available<T>() {
    override fun create(context: CoroutineContext): Fusion.Available<T>? = null
    override fun poll(): T? = error("SourceFusion.poll() called on unresolved pipeline")
    @Suppress("UNCHECKED_CAST")
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<T> = upstream as Fusion<T>
}
