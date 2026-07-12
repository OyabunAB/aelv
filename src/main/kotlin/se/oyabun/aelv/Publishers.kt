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

internal sealed interface Fusion<out T : Any> {
    data object None : Fusion<Nothing>
    abstract class Available<T : Any> : Fusion<T> {
        abstract fun create(context: CoroutineContext = EmptyCoroutineContext): Available<T>?
        abstract fun poll(): Either<Unset, T>
        open fun connectSource(upstream: Available<*>): Fusion<T> = None
    }
}

/**
 * A cold, backpressure-first publisher of zero or more items of type [T].
 *
 * Internally represented as a [Step] ADT node — a pure description of the
 * computation.  The [Interpreter] evaluates the [Step] tree using a
 * heap-allocated work-deque, so any depth of operator chaining or recursive
 * [flatMap]/[concatMap] definitions runs in O(1) JVM call-stack depth.
 *
 * The [Fusion] fast path is preserved: when the entire chain is fused,
 * [collect] bypasses the interpreter and uses the existing O(1)-allocation
 * `poll()` loop.
 */
class Many<T : Any> private constructor(
    internal val step: Step<T>,
    internal val fusion: Fusion<T> = Fusion.None,
) : Publisher<T> {

    internal suspend fun source(
        onNext: suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError: suspend (Exception) -> Unit,
    ) {
        when (val result = interpret(step, Frame.Collect(onNext))) {
            is Either.Right -> if (result.value) onComplete()
            is Either.Left  -> onError(result.value)
        }
    }

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber) { on, oc, oe -> source(on, oc, oe) }
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    fun asFlow(): Flow<T> = flow {
        source(
            { value -> emit(value); Signal.Downstream.Request },
            { },
            ::rethrow,
        )
    }

    /**
     * Drives the stream, calling [action] for each item.
     *
     * Uses the fusion poll loop when the chain is fully fused; falls back to
     * the interpreter (stack-safe) otherwise.
     */
    internal suspend fun collect(
        action: suspend (T) -> Signal.Downstream,
    ): Either<Exception, Unit> {
        val currentFusion = fusion
        if (currentFusion is Fusion.Available) {
            val coroutineContext  = currentCoroutineContext()
            val poll = currentFusion.create(coroutineContext)
            if (poll != null) return Either.catching {
                while (true) {
                    when (val polled = poll.poll()) {
                        is Either.Left  -> break
                        is Either.Right -> if (action(polled.value) == Signal.Downstream.Cancel) break
                    }
                }
            }
        }
        return when (val result = interpret(step, Frame.Collect(action))) {
            is Either.Right -> Unit.right()   // both completed and cancelled are "ok" for collect
            is Either.Left  -> result.value.left()
        }
    }

    internal fun <R : Any> collectInto(initial: R, accumulate: (R, T) -> R): Either<Exception, R>? {
        val currentFusion = fusion
        if (currentFusion !is Fusion.Available) return null
        val poll = currentFusion.create(EmptyCoroutineContext) ?: return null
        return Either.catchingStrict {
            var accumulator = initial
            while (true) {
                when (val polled = poll.poll()) {
                    is Either.Left  -> break
                    is Either.Right -> accumulator = accumulate(accumulator, polled.value)
                }
            }
            accumulator
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

        fun <T : Any> pipelineFrom(): Many<T> = Many(Step.PipelineSource(), SourceFusion())

        fun interval(period: Duration): Many<Long> = fused { onNext, onComplete, _ ->
            var tick = 0L
            while (true) {
                delay(period)
                if (onNext(tick++) == Signal.Downstream.Cancel) return@fused
            }
            @Suppress("UNREACHABLE_CODE")
            onComplete()
        }


        internal fun <T : Any> fused(
            fusion: Fusion<T> = Fusion.None,
            block: suspend (
                onNext: suspend (T) -> Signal.Downstream,
                onComplete: suspend () -> Unit,
                onError: suspend (Exception) -> Unit,
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
            onNext: suspend (B) -> Signal.Downstream,
            onComplete: suspend () -> Unit,
            onError: suspend (Exception) -> Unit,
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
                    cancelled    -> {}
                    error.isError() -> onError(error.asError())
                    else          -> onComplete()
                }
            }
        }
    }
}


class One<T : Any> private constructor(
    internal val source: suspend (
        onNext: suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError: suspend (Exception) -> Unit,
    ) -> Unit,
) : Publisher<T> {

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber, source)
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

    internal suspend fun collect(
        action: suspend (T) -> Signal.Downstream,
    ): Either<Exception, Unit> = Either.catching {
        source(
            { value -> action(value) },
            { },
            ::rethrow,
        )
    }

    companion object {
        fun <T : Any> single(value: T): One<T> = One { onNext, onComplete, _ ->
            if (onNext(value) != Signal.Downstream.Cancel) onComplete()
        }

        fun <T : Any> defer(context: CoroutineContext? = null, closure: suspend () -> T): One<T> = One { onNext, onComplete, _ ->
            val value = if (context != null) withContext(currentCoroutineContext() + context) { closure() } else closure()
            if (onNext(value) != Signal.Downstream.Cancel) onComplete()
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

        fun <T : Any> from(publisher: Publisher<T>): One<T> = One { onNext, onComplete, _ ->
            publisher.asFlow().collectCancelling { value -> onNext(value); false }
            onComplete()
        }

        fun <T : Any> error(cause: Exception): One<T> = One { _, _, onError -> onError(cause) }

        fun <T : Any> never(): One<T> = One { _, _, _ -> awaitCancellation() }

        fun <T : Any> pipelineFrom(): One<T> = One { onNext, onComplete, onError ->
            @Suppress("UNCHECKED_CAST")
            val source = currentCoroutineContext()[SourceSlot]?.publisher as? One<T>
                ?: error("One.pipelineFrom() executed without a bound source — use applyTo() or then()")
            source.source(onNext, onComplete, onError)
        }

        fun <T : Any> create(block: (success: (T) -> Unit, failure: (Exception) -> Unit) -> Unit): One<T> =
            One.generate { emit ->
                val result = suspendCancellableCoroutine<Either<Exception, T>> { continuation ->
                    block(
                        { value -> continuation.resume(value.right()) },
                        { cause -> continuation.resume(cause.left()) },
                    )
                }
                when (result) {
                    is Either.Right -> {
                        if (emit(Signal.Upstream.Next(result.value)) != Signal.Downstream.Cancel)
                            emit(Signal.Upstream.Complete)
                    }
                    is Either.Left -> emit(Signal.Upstream.Error(result.value))
                }
            }
    }
}

class None<T : Any> private constructor(
    private val source: suspend () -> Unit,
) : Publisher<Nothing> {

    override fun subscribe(subscriber: Subscriber<in Nothing>) {
        val subscription = CompletionSubscription(subscriber, source)
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    suspend fun await(): Either<Exception, Unit> = Either.catching { source() }

    companion object {
        fun <T : Any> defer(context: CoroutineContext? = null, closure: suspend () -> Unit): None<T> = None {
            if (context != null) withContext(context) { closure() } else closure()
        }

        internal fun <T : Any> generate(closure: suspend () -> Unit): None<T> = None(closure)

        fun <T : Any> from(publisher: Publisher<T>): None<T> = None {
            publisher.asFlow().collect { }
        }

        fun <T : Any> complete(): None<T> = None { }
        fun <T : Any> error(cause: Throwable): None<T> = None { throw cause }
        fun <T : Any> never(): None<T> = None { awaitCancellation() }

        fun <T : Any> pipelineFrom(): None<T> = None.generate {
            @Suppress("UNCHECKED_CAST")
            val source = currentCoroutineContext()[SourceSlot]?.publisher as? None<T>
                ?: error("None.pipelineFrom() executed without a bound source — use applyTo() or then()")
            val result = source.await()
            if (result is Either.Left) throw result.value
        }
    }
}


private class RangeFusion(private val start: Int, private val count: Int) : Fusion.Available<Int>() {
    private val end: Long = start.toLong() + count
    private var current: Long = start.toLong()
    override fun create(context: CoroutineContext): Fusion.Available<Int> = RangeFusion(start, count)
    override fun poll(): Either<Unset, Int> = if (current < end) current++.toInt().right() else Unset.left()
}

private class ArrayFusion<T : Any>(private val items: Array<out T>) : Fusion.Available<T>() {
    private var index = 0
    override fun create(context: CoroutineContext): Fusion.Available<T> = ArrayFusion(items)
    override fun poll(): Either<Unset, T> = if (index < items.size) items[index++].right() else Unset.left()
}

private class IterableFusion<T : Any>(private val iterable: Iterable<T>) : Fusion.Available<T>() {
    private var iterator: Iterator<T> = iterable.iterator()
    override fun create(context: CoroutineContext): Fusion.Available<T> = IterableFusion(iterable)
    override fun poll(): Either<Unset, T> = if (iterator.hasNext()) iterator.next().right() else Unset.left()
}

internal class MapFusion<T : Any, R : Any>(
    internal val upstream: Fusion.Available<T>,
    internal val transform: (T) -> R,
) : Fusion.Available<R>() {
    override fun create(context: CoroutineContext): Fusion.Available<R>? =
        upstream.create(context)?.let { MapFusion(it, transform) }
    override fun poll(): Either<Unset, R> = when (val polled = upstream.poll()) {
        is Either.Left  -> polled
        is Either.Right -> transform(polled.value).right()
    }
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
    override fun poll(): Either<Unset, T> {
        while (true) {
            when (val polled = upstream.poll()) {
                is Either.Left  -> return polled
                is Either.Right -> if (predicate(polled.value)) return polled
            }
        }
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
    override fun poll(): Either<Unset, T> {
        if (remaining == 0L) return Unset.left()
        return when (val polled = upstream.poll()) {
            is Either.Left  -> polled
            is Either.Right -> { remaining--; polled }
        }
    }
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<T> {
        val connected = this.upstream.connectSource(upstream)
        return if (connected is Fusion.Available) TakeFusion(connected, limit) else Fusion.None
    }
}

internal class SourceFusion<T : Any> : Fusion.Available<T>() {
    override fun create(context: CoroutineContext): Fusion.Available<T>? = null
    override fun poll(): Either<Unset, T> = error("SourceFusion.poll() called on unresolved pipeline")
    @Suppress("UNCHECKED_CAST")
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<T> = upstream as Fusion<T>
}
