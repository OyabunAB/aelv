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

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import se.oyabun.aelv.BufferEvent
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.experimental.ExperimentalTypeInference
import kotlin.internal.LowPriorityInOverloadResolution
import kotlin.time.Duration
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit


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

    private val log = Logging.of<Many<*>>()

    override fun wrap(
        block: suspend (
            onNext:     OnNext<T>,
            onComplete: OnComplete,
            onError:    OnError,
            onRequest:  Demand,
        ) -> Unit,
    ): Many<T> = fused(block = block)

    override fun toMany(): Many<T> = this

    override fun toMaybe(): Maybe<T> = Maybe.fromStep(step, fusion)

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber, ::source)
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    /**
     * Subscribes to this [Many] with backpressure replenishment.
     *
     * Requests [prefetch] items upfront, then re-requests `prefetch / 2` items each time that
     * threshold is consumed — keeping the pipeline full without unbounded buffering.
     *
     * @param prefetch Initial demand and replenishment batch size.  Must be positive.  Defaults to [Aelv.prefetch].
     * @param onNext   Called for each item.
     * @param onError  Called on error.  Exceptions thrown by this callback are logged and swallowed.
     * @param onComplete Called on normal completion.
     * @return A [Disposable] that can be used to cancel the subscription.
     */
    fun subscribe(
        prefetch: Long = Aelv.prefetch,
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
                    .onLeft { issue -> log.stream.errorCallbackFailed("subscriber.onError", issue) }
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
     * Subscribes to this [Many] with unbounded demand — the source is never asked to slow down.
     *
     * Use only when the source is known to be bounded or when backpressure is handled upstream.
     * For bounded-demand subscriptions use [subscribe] instead.
     *
     * @param onNext     Called for each item.
     * @param onError    Called on error.
     * @param onComplete Called on normal completion.
     * @return A [Disposable] that can be used to cancel the subscription.
     */
    fun drain(
        onNext: (T) -> Unit,
        onError: (Exception) -> Unit,
        onComplete: () -> Unit = {},
    ): Disposable = subscribe(
        prefetch = UNBOUNDED,
        onNext = onNext,
        onError = onError,
        onComplete = onComplete,
    )

    /**
     * Accumulates all items into a single result by applying [accumulate] from [initial].
     * Returns a [One] that emits the final accumulated value.
     */
    fun <R : Any> fold(initial: R, accumulate: (R, T) -> R): One<R> =
        One.generate { emit, onRequest ->
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
    fun <R : Any> fold(initial: R, accumulate: suspend (R, T) -> R): One<R> =
        One.generate { emit, onRequest ->
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
     * Returns a [One] that emits the reduced value, or signals [NoElementException] if the
     * stream was empty, or propagates the upstream error if the stream errored.
     */
    fun reduce(accumulate: (T, T) -> T): One<T> =
        One.generate { emit, onRequest ->
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
                    is Failure  -> emit(Signal.Upstream.Error(NoElementException()))
                    is Success -> { emit(Signal.Upstream.Next(final.value)); emit(Signal.Upstream.Complete) }
                }
            }
        }

    fun toList(): One<List<T>> =
        One.generate { emit, onRequest ->
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
    fun toSet(): One<Set<T>> =
        One.generate { emit, onRequest ->
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
     * Returns [Either.Right] with the first item, or [Either.Left] with [NoElementException]
     * if the stream was empty, or with the upstream error if the stream errored.
     */
    fun first(): One<T> {
        val currentFusion = fusion
        return One.fromStep(
            step = Step.Suspend { onNext, onComplete, onError, _ ->
                var result: Either<Unset, T> = Unset.left()
                val outcome = collect { value -> result = value.right(); Signal.Downstream.Cancel }
                val final = result
                when {
                    final   is Success -> { onNext(final.value); onComplete() }
                    outcome is Failure -> onError(outcome.value)
                    else               -> onError(NoElementException())
                }
            },
            fusion = if (currentFusion is Fusion.Available) TakeFusion(currentFusion, 1) else Fusion.None,
        )
    }

    fun last(): One<T> =
        One.generate { emit, onRequest ->
            var result: Either<Unset, T> = Unset.left()
            val outcome = collect { value -> result = value.right(); Signal.Downstream.Request }
            val final = result
            when {
                final   is Success -> { emit(Signal.Upstream.Next(final.value)); emit(Signal.Upstream.Complete) }
                outcome is Failure -> emit(Signal.Upstream.Error(outcome.value))
                else               -> emit(Signal.Upstream.Error(NoElementException()))
            }
        }

    override suspend fun collect(
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
        return when (val result = interpret(step, Frame.Collect(action), Demand())) {
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

    fun <R : Any> map(transform: (T) -> R): Many<R> {
        val currentFusion = fusion
        return fromStep(
            Step.Map(step, transform),
            if (currentFusion is Fusion.Available) MapFusion(currentFusion, transform)
            else Fusion.None
        )
    }

    @LowPriorityInOverloadResolution
    fun <R : Any> map(transform: suspend (T) -> R): Many<R> = fused { onNext, onComplete, onError, onRequest ->
        source(
            { value -> onNext(transform(value)) },
            onComplete,
            onError,
            onRequest,
        )
    }

    /**
     * Applies [transform] to each item and emits the result only when it is non-null.
     * Null results are silently dropped and demand is replenished from upstream.
     */
    fun <R : Any> mapNotNull(transform: suspend (T) -> R?): Many<R> = fused {
        onNext,
        onComplete,
        onError,
        onRequest -> source(
            { value -> transform(value)?.let { onNext(it) } ?: Signal.Downstream.Request },
            onComplete,
            onError,
            onRequest,
        )
    }

    fun filter(predicate: (T) -> Boolean): Many<T> {
        val currentFusion = fusion
        return fromStep(
            Step.Filter(step, predicate),
            if (currentFusion is Fusion.Available) FilterFusion(currentFusion, predicate)
            else Fusion.None
        )
    }

    @LowPriorityInOverloadResolution
    fun filter(predicate: suspend (T) -> Boolean): Many<T> = fused {
        onNext,
        onComplete,
        onError,
        onRequest -> source(
            { value -> if (predicate(value)) onNext(value) else Signal.Downstream.Request },
            onComplete,
            onError,
            onRequest,
        )
    }

    /** Emits at most [n] items then completes.  Requires `n >= 0`. */
    fun take(n: Long): Many<T> {
        require(n >= 0) { "take count must be non-negative, got $n" }
        val currentFusion = fusion
        return fromStep(
            Step.Take(step, n),
            if (currentFusion is Fusion.Available) TakeFusion(currentFusion, n)
            else Fusion.None
        )
    }

    fun takeWhile(predicate: (T) -> Boolean): Many<T> =
        fused { onNext, onComplete, onError, onRequest ->
            var predicateFailed = false
            source(
                { value ->
                    if (predicate(value)) onNext(value)
                    else { predicateFailed = true; Signal.Downstream.Cancel }
                },
                onComplete,
                onError,
                onRequest,
            )
            if (predicateFailed) onComplete()
        }

    /** Drops the first [n] items then emits the rest.  Requires `n >= 0`. */
    fun skip(n: Long): Many<T> {
        require(n >= 0) { "skip count must be non-negative, got $n" }
        return Many.fromStep(Step.Skip(step, n))
    }

    fun skipWhile(predicate: (T) -> Boolean): Many<T> =
        fused { onNext, onComplete, onError, onRequest ->
            var skipping = true
            source(
                { value ->
                    if (skipping && predicate(value)) Signal.Downstream.Request
                    else { skipping = false; onNext(value) }
                },
                onComplete,
                onError,
                onRequest,
            )
        }

    fun distinct(): Many<T> =
        Many.fused { onNext, onComplete, onError, onRequest ->
            val seen = HashSet<T>()
            source({ value -> if (seen.add(value)) onNext(value) else Signal.Downstream.Request }, onComplete, onError, onRequest)
        }

    /**
     * Accumulates state across items, emitting the running state after each element.
     *
     * Unlike [fold], which produces a single terminal value, [scan] emits intermediate
     * states as they are produced. The [initial] state is not emitted; the first emission
     * is the result of applying [accumulate] to [initial] and the first upstream item.
     *
     * Example — running sum:
     * ```kotlin
     * Many.items(1, 2, 3).scan(0) { sum, n -> sum + n }
     * // emits: 1, 3, 6
     * ```
     */
    fun <S : Any> scan(initial: S, accumulate: (S, T) -> S): Many<S> =
        fused { onNext, onComplete, onError, onRequest ->
            var state = initial
            source(
                { value ->
                    state = accumulate(state, value)
                    onNext(state)
                },
                onComplete,
                onError,
                onRequest,
            )
        }

    /** Suppresses consecutive duplicate items; non-adjacent duplicates are still emitted. */
    fun distinctUntilChanged(): Many<T> =
        fused { onNext, onComplete, onError, onRequest ->
            var last: Any = Unset
            source(
                { value -> if (value != last) { last = value; onNext(value) } else Signal.Downstream.Request },
                onComplete,
                onError,
                onRequest,
            )
        }

    fun <K : Any> distinctUntilChangedBy(key: (T) -> K): Many<T> =
        fused { onNext, onComplete, onError, onRequest ->
            var lastKey: Any = Unset
            source(
                { value ->
                    val itemKey = key(value)
                    if (itemKey != lastKey) { lastKey = itemKey; onNext(value) } else Signal.Downstream.Request
                },
                onComplete,
                onError,
                onRequest,
            )
        }


    /**
     * Maps each item to a [Many] and merges the results concurrently.
     *
     * [concurrency] limits the number of simultaneously active inner subscriptions.
     * Defaults to 256, matching Reactor's default prefetch for unbounded operators.
     * Use `concurrency = 1` for sequential processing — equivalent to [concatMap].
     */
    fun <R : Any> flatMap(
        concurrency: Int = 256,
        transform: (T) -> Many<R>,
    ): Many<R> = if (concurrency == 1) fromStep(Step.ConcatMap(step, transform))
    else fromStep(Step.FlatMap(step, concurrency, transform))

    @LowPriorityInOverloadResolution
    fun <R : Any> flatMap(
        concurrency: Int = 256,
        transform: suspend (T) -> Many<R>,
    ): Many<R> = generate { emit, onRequest ->
        val semaphore  = Semaphore(concurrency)
        val queue      = ConcurrentLinkedQueue<R>()
        val wip        = AtomicInteger(0)
        val cancelled  = AtomicBoolean(false)
        val outerError = AtomicReference<Any>(Unset)
        suspend fun drain() {
            do {
                while (true) {
                    if (cancelled.get()) { queue.clear(); break }
                    val item = queue.poll() ?: break
                    val downstream = emit(Signal.Upstream.Next(item))
                    if (downstream == Signal.Downstream.Cancel) {
                        cancelled.set(true); queue.clear(); break
                    }
                }
            } while (wip.decrementAndGet() != 0)
        }
        coroutineScope {
            source(
                { value ->
                    if (cancelled.get()) return@source Signal.Downstream.Cancel
                    this@coroutineScope.launch(start = CoroutineStart.UNDISPATCHED) {
                        semaphore.acquire()
                        try {
                            transform(value).source(
                                { inner ->
                                    queue.add(inner)
                                    if (wip.getAndIncrement() == 0) drain()
                                    if (cancelled.get()) Signal.Downstream.Cancel else Signal.Downstream.Request
                                },
                                { },
                                { issue -> outerError.compareAndSet(Unset, issue) },
                                onRequest,
                            )
                        } finally {
                            semaphore.release()
                        }
                    }
                    Signal.Downstream.Request
                },
                { },
                { issue -> outerError.compareAndSet(Unset, issue) },
                onRequest,
            )
        }
        when {
            cancelled.get()            -> { /* do nothing */ }
            outerError.get().isError() -> emit(Signal.Upstream.Error(outerError.get().asError()))
            else                       -> emit(Signal.Upstream.Complete)
        }
    }

    /** Maps each element to a [Many] and flattens sequentially, subscribing to one inner stream at a time. */
    fun <R : Any> concatMap(transform: (T) -> Many<R>): Many<R> =
        fromStep(Step.ConcatMap(step, transform))

    @LowPriorityInOverloadResolution
    fun <R : Any> concatMap(transform: suspend (T) -> Many<R>): Many<R> =
        generate { emit, onRequest ->
            var cancelled = false
            source(
                { value ->
                    if (cancelled) return@source Signal.Downstream.Cancel
                    transform(value).source(
                        { inner ->
                            val downstream = emit(Signal.Upstream.Next(inner))
                            if (downstream == Signal.Downstream.Cancel) cancelled = true
                            downstream
                        },
                        { /* do nothing */ },
                        { issue -> emit(Signal.Upstream.Error(issue)); cancelled = true },
                        onRequest,
                    )
                    if (cancelled) Signal.Downstream.Cancel else Signal.Downstream.Request
                },
                { if (!cancelled) emit(Signal.Upstream.Complete) },
                { issue -> if (!cancelled) emit(Signal.Upstream.Error(issue)) },
                onRequest,
            )
        }

    /**
     * Maps each item to a [Many], subscribing to up to [maxConcurrency] inner streams concurrently,
     * but emitting results in upstream order.  Completed-but-out-of-order results are held until
     * all earlier inner streams have drained.
     *
     * With `maxConcurrency = 1` this is identical to [concatMap].
     */
    fun <R : Any> flatMapSequential(
        maxConcurrency: Int = 256,
        transform: (T) -> Many<R>,
    ): Many<R> {
        require(maxConcurrency > 0) { "maxConcurrency must be positive, got $maxConcurrency" }
        if (maxConcurrency == 1) return concatMap(transform)
        return Many.generate { emit, onRequest ->
            val semaphore = Semaphore(maxConcurrency)
            val orderChannel = Channel<Channel<Signal.Upstream<R>>>(maxConcurrency)
            coroutineScope {
                val producerJob = launch {
                    var outerError: Any = Unset
                    source(
                        { value ->
                            val innerChannel = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
                            orderChannel.send(innerChannel)
                            this@launch.launch(start = CoroutineStart.UNDISPATCHED) {
                                semaphore.withPermit {
                                    transform(value).source(
                                        { inner -> innerChannel.send(Signal.Upstream.Next(inner)); Signal.Downstream.Request },
                                        { innerChannel.close() },
                                        { issue -> innerChannel.close(issue) },
                                        onRequest,
                                    )
                                }
                            }
                            Signal.Downstream.Request
                        },
                        { /* do nothing */ },
                        { issue -> outerError = issue },
                        onRequest,
                    )
                    val sentinel = Channel<Signal.Upstream<R>>(0)
                    if (outerError.isError()) sentinel.close(outerError.asError()) else sentinel.close()
                    orderChannel.send(sentinel)
                    orderChannel.close()
                }
                var cancelled = false
                for (innerChannel in orderChannel) {
                    if (cancelled) { innerChannel.cancel(); continue }
                    val result = Either.catching {
                        for (signal in innerChannel) {
                            when (signal) {
                                is Signal.Upstream.Next -> if (emit(signal) == Signal.Downstream.Cancel) {
                                    cancelled = true; producerJob.cancel(); break
                                }
                                else -> break
                            }
                        }
                    }
                    if (result is Failure && !cancelled) {
                        cancelled = true
                        producerJob.cancel()
                        emit(Signal.Upstream.Error(result.value))
                    }
                }
                if (!cancelled) emit(Signal.Upstream.Complete)
            }
        }
    }

    /**
     * Maps each item to a [Many], cancels the previous inner subscription when a new item arrives,
     * and subscribes to the latest inner [Many].  Only the most recent inner stream is active at any time.
     */
    fun <R : Any> switchMap(transform: (T) -> Many<R>): Many<R> =
        generate { emit, onRequest ->
            val channel = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
            coroutineScope {
                val producerJob = launch {
                    var activeJob = launch {}
                    source(
                        { value ->
                            activeJob.cancelAndJoin()
                            activeJob = this@launch.launch(start = CoroutineStart.UNDISPATCHED) {
                                transform(value).source(
                                    { inner -> channel.send(Signal.Upstream.Next(inner)); Signal.Downstream.Request },
                                    { },
                                    { issue -> channel.send(Signal.Upstream.Error(issue)) },
                                    onRequest,
                                )
                            }
                            Signal.Downstream.Request
                        },
                        {
                            activeJob.join()
                            channel.send(Signal.Upstream.Complete)
                        },
                        { issue ->
                            activeJob.cancelAndJoin()
                            channel.send(Signal.Upstream.Error(issue))
                        },
                        onRequest,
                    )
                    channel.close()
                }
                for (signal in channel) {
                    when (signal) {
                        is Signal.Upstream.Next     -> if (emit(signal) == Signal.Downstream.Cancel) { producerJob.cancel(); break }
                        is Signal.Upstream.Complete -> { emit(Signal.Upstream.Complete); break }
                        is Signal.Upstream.Error    -> { producerJob.cancel(); emit(Signal.Upstream.Error(signal.cause)); break }
                    }
                }
            }
        }

    /**
     * Emits items from this [Many] until [other] emits any signal (Next, Complete, or Error),
     * at which point the subscription to this source is cancelled and the stream completes.
     */
    fun takeUntilOther(other: Publisher<*>): Many<T> =
        generate { emit, onRequest ->
            val channel = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
            coroutineScope {
                val controlJob = launch {
                    Many.from(other).source(
                        { channel.send(Signal.Upstream.Complete); Signal.Downstream.Cancel },
                        { channel.send(Signal.Upstream.Complete) },
                        { channel.send(Signal.Upstream.Complete) },
                        onRequest,
                    )
                }
                val producerJob = launch {
                    source(
                        { value -> channel.send(Signal.Upstream.Next(value)); Signal.Downstream.Request },
                        { channel.send(Signal.Upstream.Complete) },
                        { issue -> channel.send(Signal.Upstream.Error(issue)) },
                        onRequest,
                    )
                    channel.close()
                }
                for (signal in channel) {
                    when (signal) {
                        is Signal.Upstream.Next     -> if (emit(signal) == Signal.Downstream.Cancel) { producerJob.cancel(); controlJob.cancel(); break }
                        is Signal.Upstream.Complete -> { producerJob.cancel(); controlJob.cancel(); emit(Signal.Upstream.Complete); break }
                        is Signal.Upstream.Error    -> { producerJob.cancel(); controlJob.cancel(); emit(signal); break }
                    }
                }
            }
        }

    fun mergeWith(other: Many<T>): Many<T> = merge(this, other)

    /**
     * Collects items into fixed-size lists of [size] and emits each list downstream.
     * A partial list is emitted on upstream completion.
     */
    fun buffer(size: Int): Many<List<T>> {
        require(size > 0) { "buffer size must be positive, got $size" }
        return generate { emit, onRequest ->
            val bucket = mutableListOf<T>()
            source(
                { value ->
                    bucket.add(value)
                    if (bucket.size == size) {
                        val downstream = emit(Signal.Upstream.Next(bucket.toList()))
                        bucket.clear()
                        downstream
                    } else Signal.Downstream.Request
                },
                {
                    if (bucket.isNotEmpty() && emit(Signal.Upstream.Next(bucket.toList())) == Signal.Downstream.Cancel) {
                        return@source
                    }
                    emit(Signal.Upstream.Complete)
                },
                { issue -> emit(Signal.Upstream.Error(issue)) },
                onRequest,
            )
        }
    }

    /**
     * Collects items into overlapping or sliding windows of [size], advancing by [skip] items each time.
     * - `skip == size`: non-overlapping (equivalent to [buffer]).
     * - `skip < size`: overlapping windows.
     * - `skip > size`: gaps between windows (some items are skipped).
     */
    fun buffer(size: Int, skip: Int): Many<List<T>> {
        require(size > 0) { "buffer size must be positive, got $size" }
        require(skip > 0) { "buffer skip must be positive, got $skip" }
        return Many.generate { emit, onRequest ->
            val buffers = ArrayDeque<MutableList<T>>()
            var index = 0L
            source(
                { value ->
                    if (index % skip == 0L) buffers.addLast(mutableListOf())
                    buffers.forEach { it.add(value) }
                    val full = buffers.firstOrNull { it.size == size }?.toList()
                    index++
                    if (full != null) {
                        val downstream = emit(Signal.Upstream.Next(full))
                        buffers.removeFirst()
                        downstream
                    } else Signal.Downstream.Request
                },
                {
                    // Emit partial trailing buffers, consistent with buffer(size).
                    for (partial in buffers) {
                        if (partial.isNotEmpty() &&
                            emit(Signal.Upstream.Next(partial.toList())) == Signal.Downstream.Cancel) {
                            return@source
                        }
                    }
                    emit(Signal.Upstream.Complete)
                },
                { issue -> emit(Signal.Upstream.Error(issue)) },
                onRequest,
            )
        }
    }

    /**
     * Collects items into a list and emits it when either [size] items have accumulated or [timeout]
     * elapses since the first item entered the current bucket — whichever comes first.
     * An incomplete bucket is emitted on upstream Complete.
     */
    fun bufferTimeout(size: Int, timeout: Duration): Many<List<T>> {
        require(size > 0) { "buffer size must be positive, got $size" }
        require(timeout.isPositive()) { "timeout must be positive, got $timeout" }
        return Many.generate { emit, onRequest ->
            // Merged event channel: SourceSignal = upstream item/terminal, TimerFlush = timeout sentinel.
            val events = Channel<BufferEvent<T>>(Channel.BUFFERED)
            coroutineScope {
                val producerJob = launch {
                    source(
                        { value ->
                            events.send(BufferEvent.SourceSignal(Signal.Upstream.Next(value)))
                            Signal.Downstream.Request
                        },
                        {
                            events.send(BufferEvent.SourceSignal(Signal.Upstream.Complete))
                        },
                        { issue ->
                            events.send(BufferEvent.SourceSignal(Signal.Upstream.Error(issue)))
                        },
                        onRequest,
                    )
                    events.close()
                }
                val bucket = mutableListOf<T>()
                var timer: TimerState = TimerState.Idle

                fun resetTimer() {
                    timer.cancel()
                    timer = TimerState.Running(launch { delay(timeout); events.send(BufferEvent.TimerFlush) })
                }

                suspend fun flushBucket(): Boolean {
                    if (bucket.isEmpty()) return true
                    val downstream = emit(Signal.Upstream.Next(bucket.toList()))
                    bucket.clear()
                    timer.cancel()
                    timer = TimerState.Idle
                    return downstream != Signal.Downstream.Cancel
                }

                var terminated = false
                for (event in events) {
                    when (event) {
                        is BufferEvent.TimerFlush -> {
                            if (!flushBucket()) { producerJob.cancel(); terminated = true; break }
                        }
                        is BufferEvent.SourceSignal -> when (val signal = event.signal) {
                            is Signal.Upstream.Next -> {
                                if (bucket.isEmpty()) resetTimer()
                                bucket.add(signal.value)
                                if (bucket.size >= size) {
                                    if (!flushBucket()) { producerJob.cancel(); terminated = true; break }
                                }
                            }
                            is Signal.Upstream.Complete -> {
                                timer.cancel()
                                flushBucket()
                                break
                            }
                            is Signal.Upstream.Error -> {
                                timer.cancel()
                                emit(Signal.Upstream.Error(signal.cause))
                                terminated = true
                                break
                            }
                        }
                    }
                }
                timer.cancel()
                if (!terminated) emit(Signal.Upstream.Complete)
            }
        }
    }

    /**
     * Groups items by [keySelector] and routes each group to [groupHandler].
     *
     * One sub-stream is created per distinct key.  Each sub-stream is passed to [groupHandler] which
     * transforms it into a [Many] of results; all results are merged into the output stream.
     *
     * The operator owns all group subscriptions — callers cannot accidentally leave a group
     * unsubscribed.  Cancelling the outer stream cancels all active groups.
     */
    fun <K : Any, R : Any> groupBy(
        keySelector: (T) -> K,
        groupHandler: (key: K, group: Many<T>) -> Many<R>,
    ): Many<R> = generate { emit, onRequest ->
        val groupChannels = mutableMapOf<K, Channel<Signal.Upstream<T>>>()
        val output = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
        val remaining     = AtomicInteger(0)
        coroutineScope {
            val producerJob = launch {
                source(
                    { value ->
                        val key = keySelector(value)
                        val groupInbox  = groupChannels.getOrPut(key) {
                            val newGroupInbox = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
                            remaining.incrementAndGet()
                            this@launch.launch(start = CoroutineStart.UNDISPATCHED) {
                                val groupMany = generate { groupEmit, _ ->
                                    for (upstream in newGroupInbox) {
                                        if (groupEmit(upstream) == Signal.Downstream.Cancel) {
                                            newGroupInbox.cancel(); break
                                        }
                                    }
                                }
                                groupHandler(key, groupMany).source(
                                    { inner -> output.send(Signal.Upstream.Next(inner)); Signal.Downstream.Request },
                                    { },
                                    { issue -> output.send(Signal.Upstream.Error(issue)) },
                                    onRequest,
                                )
                                if (remaining.decrementAndGet() == 0) output.close()
                            }
                            newGroupInbox
                        }
                        groupInbox.send(Signal.Upstream.Next(value))
                        Signal.Downstream.Request
                    },
                    {
                        for ((_, groupInbox) in groupChannels) runCatching { groupInbox.send(Signal.Upstream.Complete) }
                        if (remaining.get() == 0) output.close()
                    },
                    { issue ->
                        for ((_, groupInbox) in groupChannels) runCatching { groupInbox.send(Signal.Upstream.Error(issue)) }
                        if (remaining.get() == 0) output.close()
                    },
                    onRequest,
                )
            }
            var terminated = false
            for (signal in output) {
                when (signal) {
                    is Signal.Upstream.Next     -> if (emit(signal) == Signal.Downstream.Cancel) {
                        producerJob.cancel(); terminated = true; break
                    }
                    is Signal.Upstream.Complete -> break
                    is Signal.Upstream.Error    -> { producerJob.cancel(); emit(signal); terminated = true; break }
                }
            }
            if (!terminated) emit(Signal.Upstream.Complete)
        }
    }

    /**
     * Drops upstream items that arrive when the internal buffer is full.
     *
     * Use this only when data loss is acceptable — e.g. high-frequency sensor readings where
     * only the latest values matter.
     */
    fun onBackpressureDrop(): Many<T> =
        generate { emit, onRequest ->
            val channel = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
            coroutineScope {
                val producerJob = launch {
                    source(
                        { value ->
                            if (!channel.trySend(Signal.Upstream.Next(value)).isSuccess)
                                log.operator.dropping("onBackpressureDrop", value)
                            Signal.Downstream.Request
                        },
                        {
                            channel.send(Signal.Upstream.Complete)  // must not lose Complete
                        },
                        { issue ->
                            channel.send(Signal.Upstream.Error(issue))  // must not lose Error
                        },
                        onRequest,
                    )
                    channel.close()
                }
                for (signal in channel) {
                    when (signal) {
                        is Signal.Upstream.Next     -> if (emit(signal) == Signal.Downstream.Cancel) { producerJob.cancel(); break }
                        is Signal.Upstream.Complete -> { emit(Signal.Upstream.Complete); break }
                        is Signal.Upstream.Error    -> { producerJob.cancel(); emit(signal); break }
                    }
                }
            }
        }

    /**
     * On error, emits the single value returned by [fallback] and then completes.
     * On normal completion, [fallback] is not invoked.
     */
    fun recoverWith(fallback: (Exception) -> T): Many<T> =
        Many.generate { emit, onRequest ->
            val result = collect { emit(Signal.Upstream.Next(it)) }
            when (result) {
                is Success  -> emit(Signal.Upstream.Complete)
                is Failure -> {
                    if (emit(Signal.Upstream.Next(fallback(result.value))) != Signal.Downstream.Cancel)
                        emit(Signal.Upstream.Complete)
                }
            }
        }

    fun concatWith(other: Many<T>): Many<T> = concat(this, other)

    fun <T : Any, B : Any, R : Any> Many<T>.zipWith(other: Many<B>, transform: (T, B) -> R): Many<R> =
        zip(this, other, transform)

    fun flatMapNone(transform: (T) -> None<T>): None<T> =
        concatMap { transform(it).toMany() }.discard()

    @LowPriorityInOverloadResolution
    fun flatMapNone(transform: suspend (T) -> None<T>): None<T> =
        concatMap(transform = suspend { value: T -> transform(value).toMany() }).discard()


    companion object {

        fun <T : Any> items(vararg items: T): Many<T> = Many(
            step = Step.Items(items),
            fusion = ArrayFusion(items),
        )

        fun <T : Any> from(iterable: Iterable<T>): Many<T> = Many(
            step = Step.FromIterable(iterable),
            fusion = IterableFusion(iterable),
        )

        fun <T : Any> from(flow: Flow<T>): Many<T> = Many(step = Step.FromFlow(flow))

        fun <T : Any> from(publisher: Publisher<T>): Many<T> = Many(step = Step.FromPublisher(publisher))

        fun range(start: Int, count: Int): Many<Int> {
            require(count >= 0) { "count must be non-negative, got $count" }
            return Many(
                step = Step.Range(start, count),
                fusion = RangeFusion(start, count),
            )
        }

        fun <T : Any> empty(): Many<T> = Many(step = Step.Empty)

        fun <T : Any> error(cause: Exception): Many<T> = Many(Step.Error(cause))

        fun <T : Any> never(): Many<T> = Many(Step.Never)

        fun <T : Any> defer(factory: () -> Many<T>): Many<T> = Many(Step.Defer(factory))

        fun <T : Any> defer(factory: suspend () -> Many<T>): Many<T> = fused {
            onNext,
            onComplete,
            onError,
            onRequest -> factory().source(onNext, onComplete, onError, onRequest)
        }

        fun <T : Any> pipelineFrom(): Many<T> = Many(
            step = Step.PipelineSource(),
            fusion = SourceFusion(),
        )

        fun interval(period: Duration): Many<Long> {
            require(period.isPositive()) { "interval period must be positive, got $period" }
            return fused { onNext, _, _, _ ->
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
                onRequest:  Demand,
            ) -> Unit,
        ): Many<T> = Many(Step.Suspend(block), fusion)

        internal fun <T : Any> generate(
            block: suspend (
                emit:      suspend (Signal.Upstream<T>) -> Signal.Downstream,
                onRequest: Demand,
            ) -> Unit,
        ): Many<T> = fused { onNext, onComplete, onError, onRequest ->
            block({ signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> onNext(signal.value)
                    is Signal.Upstream.Complete -> { onComplete(); Signal.Downstream.Cancel }
                    is Signal.Upstream.Error    -> { onError(signal.cause); Signal.Downstream.Cancel }
                }
            }, onRequest)
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
            onRequest:  Demand,
        ) -> Unit {
            val upstream = Many(upstreamStep)
            return { onNext, onComplete, onError, onRequest ->
                val semaphore  = Semaphore(concurrency)
                val queue      = ConcurrentLinkedQueue<B>()
                val wip        = AtomicInteger(0)
                val cancelled  = AtomicBoolean(false)
                val outerError = AtomicReference<Any>(Unset)
                suspend fun drain() {
                    do {
                        while (true) {
                            if (cancelled.get()) { queue.clear(); break }
                            val item = queue.poll() ?: break
                            val downstream = onNext(item)
                            if (downstream == Signal.Downstream.Cancel) {
                                cancelled.set(true); queue.clear(); break
                            }
                        }
                    } while (wip.decrementAndGet() != 0)
                }
                coroutineScope {
                    upstream.source(
                        { value ->
                            if (cancelled.get()) return@source Signal.Downstream.Cancel
                            val inner = transform(value)
                            val innerFusion = inner.fusion
                            if (innerFusion is Fusion.Available) {
                                // Fast path: fused (synchronous) inner — poll directly, no launch or queue.
                                // Single-threaded execution here satisfies RS §1.3 trivially.
                                val poll = innerFusion.create(EmptyCoroutineContext)
                                if (poll != null) {
                                    tailrec suspend fun pollDrain() {
                                        if (cancelled.get()) return
                                        val item = poll.poll() ?: return
                                        if (onNext(item) == Signal.Downstream.Cancel) cancelled.set(true)
                                        else pollDrain()
                                    }
                                    pollDrain()
                                    return@source if (cancelled.get()) Signal.Downstream.Cancel else Signal.Downstream.Request
                                }
                            }
                            // Async path: launch inner concurrently, serialise output via drain queue.
                            this@coroutineScope.launch(start = CoroutineStart.UNDISPATCHED) {
                                semaphore.acquire()
                                try {
                                    inner.source(
                                        { item ->
                                            queue.add(item)
                                            if (wip.getAndIncrement() == 0) drain()
                                            if (cancelled.get()) Signal.Downstream.Cancel
                                            else Signal.Downstream.Request
                                        },
                                        { },
                                        { issue -> outerError.compareAndSet(Unset, issue) },
                                        onRequest,
                                    )
                                } finally {
                                    semaphore.release()
                                }
                            }
                            Signal.Downstream.Request
                        },
                        { },
                        { issue -> outerError.compareAndSet(Unset, issue) },
                        onRequest,
                    )
                }
                val error = outerError.get()
                when {
                    cancelled.get() -> { /* no operation */ }
                    error.isError() -> onError(error.asError())
                    else            -> onComplete()
                }
            }
        }

        internal suspend fun <R : Any, T : Any> bracket(
            resource: R,
            release:  (R, Either<Throwable, Unit>) -> None<*>,
            emit:     suspend (Signal.Upstream<T>) -> Signal.Downstream,
            use:      () -> Many<T>,
        ) {
            val stream = try {
                use()
            } catch (e: Exception) {
                release(resource, Either.failure(e)).toMany().collect { Signal.Downstream.Request }
                emit(Signal.Upstream.Error(e))
                return
            }
            var error: Exception? = null
            var cancelled = false
            val result = stream.collect { value ->
                emit(Signal.Upstream.Next(value)).also { if (it == Signal.Downstream.Cancel) cancelled = true }
            }
            if (result is Failure) error = result.value
            val releaseSignal = if (error != null) Either.failure(error) else Either.success(Unit)
            release(resource, releaseSignal).toMany().collect { Signal.Downstream.Request }
            when {
                error != null -> emit(Signal.Upstream.Error(error))
                cancelled     -> { /* do nothing */ }
                else          -> emit(Signal.Upstream.Complete)
            }
        }

        fun <R : Any, T : Any> resource(
            acquire: () -> One<R>,
            release: (R, Either<Throwable, Unit>) -> None<*>,
            use:     (R) -> Many<T>,
        ): Many<T> = acquire().flatMapMany { resource ->
            generate { emit, onRequest -> bracket(resource, release, emit) { use(resource) } }
        }


        /**
         * Emits a combined value whenever either [a] or [b] emits, using the most recent value from the
         * other source.  Does not emit until both sources have emitted at least one item.
         */
        fun <A : Any, B : Any, R : Any> combineLatest(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
            generate { emit, onRequest ->
                // Channel carries tagged values (FromA/FromB) plus errors.
                // All producer signals pass through the channel — single collector, serial emit — RS 1.3.
                val channel = Channel<Signal.Upstream<Tagged<A, B>>>(Channel.BUFFERED)
                coroutineScope {
                    val jobA = launch {
                        a.source(
                            { value -> channel.send(Signal.Upstream.Next(FromA(value))); Signal.Downstream.Request },
                            { },
                            { issue -> channel.send(Signal.Upstream.Error(issue)) },
                            onRequest,
                        )
                    }
                    val jobB = launch {
                        b.source(
                            { value -> channel.send(Signal.Upstream.Next(FromB(value))); Signal.Downstream.Request },
                            { },
                            { issue -> channel.send(Signal.Upstream.Error(issue)) },
                            onRequest,
                        )
                    }
                    val closerJob = launch {
                        jobA.join()
                        jobB.join()
                        channel.close()
                    }
                    var latestA: Either<Unset, A> = Unset.left()
                    var latestB: Either<Unset, B> = Unset.left()
                    var terminated = false
                    for (signal in channel) {
                        when (signal) {
                            is Signal.Upstream.Error -> { emit(signal); terminated = true; break }
                            is Signal.Upstream.Complete -> break
                            is Signal.Upstream.Next -> when (val tagged = signal.value) {
                                is FromA -> {
                                    latestA = tagged.value.right()
                                    val capturedB = latestB
                                    if (capturedB is Success &&
                                        emit(Signal.Upstream.Next(transform(tagged.value, capturedB.value)))
                                            == Signal.Downstream.Cancel) {
                                        terminated = true; break
                                    }
                                }
                                is FromB -> {
                                    latestB = tagged.value.right()
                                    val capturedA = latestA
                                    if (capturedA is Success &&
                                        emit(Signal.Upstream.Next(transform(capturedA.value, tagged.value)))
                                            == Signal.Downstream.Cancel) {
                                        terminated = true; break
                                    }
                                }
                            }
                        }
                    }
                    // Cancel all producers so any blocked channel.send() unblocks immediately.
                    jobA.cancel()
                    jobB.cancel()
                    closerJob.cancel()
                    // RS §1.7: no signal after a terminal.
                    if (!terminated) emit(Signal.Upstream.Complete)
                }
            }


        /**
         * Pairs items from [a] and [b] by position, applying [transform] to each pair.
         * Completes when the shorter source is exhausted.
         */
        fun <A : Any, B : Any, R : Any> zip(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
            Many.generate { emit, onRequest ->
                val channelA = Channel<Signal.Upstream<A>>(Channel.BUFFERED)
                val channelB = Channel<Signal.Upstream<B>>(Channel.BUFFERED)
                coroutineScope {
                    val jobA = launch {
                        a.source(
                            { value -> channelA.send(Signal.Upstream.Next(value)); Signal.Downstream.Request },
                            { channelA.send(Signal.Upstream.Complete) },
                            { issue -> channelA.send(Signal.Upstream.Error(issue)) },
                            onRequest,
                        )
                    }
                    val jobB = launch {
                        b.source(
                            { value -> channelB.send(Signal.Upstream.Next(value)); Signal.Downstream.Request },
                            { channelB.send(Signal.Upstream.Complete) },
                            { issue -> channelB.send(Signal.Upstream.Error(issue)) },
                            onRequest,
                        )
                    }
                    var zipError: Any = Unset
                    while (when (val signalA = channelA.receive()) {
                            is Signal.Upstream.Complete -> { jobB.cancel(); false }
                            is Signal.Upstream.Error    -> { jobB.cancel(); zipError = signalA.cause; false }
                            is Signal.Upstream.Next     -> when (val signalB = channelB.receive()) {
                                is Signal.Upstream.Complete -> { jobA.cancel(); false }
                                is Signal.Upstream.Error    -> { jobA.cancel(); zipError = signalB.cause; false }
                                is Signal.Upstream.Next     -> emit(Signal.Upstream.Next(transform(signalA.value, signalB.value))) != Signal.Downstream.Cancel
                            }
                        }) {}
                    jobA.cancel()
                    jobB.cancel()
                    if (zipError.isError()) emit(Signal.Upstream.Error(zipError.asError()))
                    else emit(Signal.Upstream.Complete)
                }
            }


        /**
         * Merges all [sources] into a single [Many], interleaving items as they arrive.
         * Completes when all sources have completed.  Errors from any source are forwarded immediately.
         */
        fun <T : Any> merge(vararg sources: Many<T>): Many<T> = generate { emit, onRequest ->
            if (sources.isEmpty()) { emit(Signal.Upstream.Complete); return@generate }
            // Channel carries only Next and Error — never Complete.
            // Completion is tracked via an atomic counter so sources never block on a drained channel.
            val channel = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
            val remaining = AtomicInteger(sources.size)
            coroutineScope {
                val jobs = sources.map { src ->
                    launch {
                        src.source(
                            { value -> channel.send(Signal.Upstream.Next(value)); Signal.Downstream.Request },
                            { if (remaining.decrementAndGet() == 0) channel.close() },
                            { issue -> channel.send(Signal.Upstream.Error(issue)) },
                            onRequest,
                        )
                    }
                }
                var terminated = false
                for (signal in channel) {
                    when (signal) {
                        is Signal.Upstream.Next  -> if (emit(signal) == Signal.Downstream.Cancel) {
                            jobs.forEach { it.cancel() }
                            terminated = true
                            break
                        }
                        is Signal.Upstream.Error -> {
                            jobs.forEach { it.cancel() }
                            emit(signal)
                            terminated = true
                            break
                        }
                        is Signal.Upstream.Complete -> break  // unreachable: channel only carries Next/Error
                    }
                }
                // RS §1.7: no signal after a terminal.
                if (!terminated) emit(Signal.Upstream.Complete)
            }
        }

        /**
         * Subscribes to [sources] one at a time in order, emitting all items from each before moving
         * to the next.  Errors from any source terminate the sequence immediately.
         */
        fun <T : Any> concat(vararg sources: Many<T>): Many<T> = generate { emit, onRequest ->
            for (src in sources) {
                var cancelled = false
                val result = src.collect { value ->
                    emit(Signal.Upstream.Next(value)).also { if (it == Signal.Downstream.Cancel) cancelled = true }
                }
                if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
                if (cancelled) return@generate
            }
            emit(Signal.Upstream.Complete)
        }
    }
}
