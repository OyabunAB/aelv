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
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import org.reactivestreams.Publisher

private val log = Logging.of<Many<*>>()

fun <T : Any, R : Any> Many<T>.map(transform: (T) -> R): Many<R> {
    val currentFusion = fusion
    return Many.fromStep(Step.Map(step, transform), if (currentFusion is Fusion.Available) MapFusion(currentFusion, transform) else Fusion.None)
}

/** Suspend variant of [map] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Many<T>.map(transform: suspend (T) -> R): Many<R> =
    Many.fused { onNext, onComplete, onError ->
        source(
            { value -> onNext(transform(value)) },
            onComplete,
            onError,
        )
    }

/**
 * Applies [transform] to each item and emits the result only when it is non-null.
 * Null results are silently dropped and demand is replenished from upstream.
 */
fun <T : Any, R : Any> Many<T>.mapNotNull(transform: (T) -> R?): Many<R> =
    Many.fused { onNext, onComplete, onError ->
        source(
            { value -> val result = transform(value); if (result != null) onNext(result) else Signal.Downstream.Request },
            onComplete,
            onError,
        )
    }

/** Suspend variant of [mapNotNull] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Many<T>.mapNotNull(transform: suspend (T) -> R?): Many<R> =
    Many.fused { onNext, onComplete, onError ->
        source(
            { value -> val result = transform(value); if (result != null) onNext(result) else Signal.Downstream.Request },
            onComplete,
            onError,
        )
    }

fun <T : Any> Many<T>.filter(predicate: (T) -> Boolean): Many<T> {
    val currentFusion = fusion
    return Many.fromStep(Step.Filter(step, predicate), if (currentFusion is Fusion.Available) FilterFusion(currentFusion, predicate) else Fusion.None)
}

/** Suspend variant of [filter] — [predicate] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> Many<T>.filter(predicate: suspend (T) -> Boolean): Many<T> =
    Many.fused { onNext, onComplete, onError ->
        source(
            { value -> if (predicate(value)) onNext(value) else Signal.Downstream.Request },
            onComplete,
            onError,
        )
    }

/** Emits at most [n] items then completes.  Requires `n >= 0`. */
fun <T : Any> Many<T>.take(n: Long): Many<T> {
    require(n >= 0) { "take count must be non-negative, got $n" }
    val currentFusion = fusion
    return Many.fromStep(Step.Take(step, n), if (currentFusion is Fusion.Available) TakeFusion(currentFusion, n) else Fusion.None)
}

fun <T : Any> Many<T>.takeWhile(predicate: (T) -> Boolean): Many<T> =
    Many.fused { onNext, onComplete, onError ->
        source(
            { value ->
                if (predicate(value)) onNext(value)
                else { onComplete(); Signal.Downstream.Cancel }
            },
            onComplete,
            onError,
        )
    }

/** Drops the first [n] items then emits the rest.  Requires `n >= 0`. */
fun <T : Any> Many<T>.skip(n: Long): Many<T> {
    require(n >= 0) { "skip count must be non-negative, got $n" }
    return Many.fromStep(Step.Skip(step, n))
}

fun <T : Any> Many<T>.skipWhile(predicate: (T) -> Boolean): Many<T> =
    Many.fused { onNext, onComplete, onError ->
        var skipping = true
        source(
            { value ->
                if (skipping && predicate(value)) Signal.Downstream.Request
                else { skipping = false; onNext(value) }
            },
            onComplete,
            onError,
        )
    }

fun <T : Any> Many<T>.distinct(): Many<T> =
    Many.fused { onNext, onComplete, onError ->
        val seen = HashSet<T>()
        source({ value -> if (seen.add(value)) onNext(value) else Signal.Downstream.Request }, onComplete, onError)
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
fun <T : Any, S : Any> Many<T>.scan(initial: S, accumulate: (S, T) -> S): Many<S> =
    Many.fused { onNext, onComplete, onError ->
        var state = initial
        source(
            { value ->
                state = accumulate(state, value)
                onNext(state)
            },
            onComplete,
            onError,
        )
    }

/** Suppresses consecutive duplicate items; non-adjacent duplicates are still emitted. */
fun <T : Any> Many<T>.distinctUntilChanged(): Many<T> =
    Many.fused { onNext, onComplete, onError ->
        var last: Any = Unset
        source(
            { value -> if (value != last) { last = value; onNext(value) } else Signal.Downstream.Request },
            onComplete,
            onError,
        )
    }

fun <T : Any, K : Any> Many<T>.distinctUntilChangedBy(key: (T) -> K): Many<T> =
    Many.fused { onNext, onComplete, onError ->
        var lastKey: Any = Unset
        source(
            { value ->
                val itemKey = key(value)
                if (itemKey != lastKey) { lastKey = itemKey; onNext(value) } else Signal.Downstream.Request
            },
            onComplete,
            onError,
        )
    }


/**
 * Maps each item to a [Many] and merges the results concurrently.
 *
 * [concurrency] limits the number of simultaneously active inner subscriptions.
 * Defaults to 256, matching Reactor's default prefetch for unbounded operators.
 * Use `concurrency = 1` for sequential processing — equivalent to [concatMap].
 */
fun <T : Any, R : Any> Many<T>.flatMap(
    concurrency: Int = 256,
    transform: (T) -> Many<R>,
): Many<R> = if (concurrency == 1) Many.fromStep(Step.ConcatMap(step, transform))
             else Many.fromStep(Step.FlatMap(step, concurrency, transform))

/** Suspend variant of [flatMap] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Many<T>.flatMap(
    concurrency: Int = 256,
    transform: suspend (T) -> Many<R>,
): Many<R> = Many.generate { emit ->
    val semaphore = Semaphore(concurrency)
    val mutex = kotlinx.coroutines.sync.Mutex()
    var cancelled = false
    var outerError: Any = Unset
    coroutineScope {
        source(
            { value ->
                if (cancelled) return@source Signal.Downstream.Cancel
                semaphore.withPermit {
                    transform(value).source(
                        { inner ->
                            mutex.withLock {
                                if (cancelled) Signal.Downstream.Cancel
                                else {
                                    val downstream = emit(Signal.Upstream.Next(inner))
                                    if (downstream == Signal.Downstream.Cancel) cancelled = true
                                    downstream
                                }
                            }
                        },
                        {},
                        { issue -> if (outerError === Unset) outerError = issue },
                    )
                }
                Signal.Downstream.Request
            },
            {},
            { issue -> if (outerError === Unset) outerError = issue },
        )
    }
    when {
        cancelled       -> {}
        outerError.isError() -> emit(Signal.Upstream.Error(outerError.asError()))
        else            -> emit(Signal.Upstream.Complete)
    }
}

/** Maps each element to a [Many] and flattens sequentially, subscribing to one inner stream at a time. */
fun <T : Any, R : Any> Many<T>.concatMap(transform: (T) -> Many<R>): Many<R> =
    Many.fromStep(Step.ConcatMap(step, transform))

/** Suspend variant of [concatMap] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Many<T>.concatMap(transform: suspend (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
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
                    {},
                    { issue -> emit(Signal.Upstream.Error(issue)); cancelled = true },
                )
                if (cancelled) Signal.Downstream.Cancel else Signal.Downstream.Request
            },
            { if (!cancelled) emit(Signal.Upstream.Complete) },
            { issue -> if (!cancelled) emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Maps each item to a [Many], subscribing to up to [maxConcurrency] inner streams concurrently,
 * but emitting results in upstream order.  Completed-but-out-of-order results are held until
 * all earlier inner streams have drained.
 *
 * With `maxConcurrency = 1` this is identical to [concatMap].
 */
fun <T : Any, R : Any> Many<T>.flatMapSequential(
    maxConcurrency: Int = 256,
    transform: (T) -> Many<R>,
): Many<R> = if (maxConcurrency == 1) concatMap(transform) else Many.generate { emit ->
    require(maxConcurrency > 0) { "maxConcurrency must be positive, got $maxConcurrency" }
    val semaphore = Semaphore(maxConcurrency)
    val orderChannel = Channel<Channel<Signal.Upstream<R>>>(maxConcurrency)
    coroutineScope {
        val producerJob = launch {
            var outerError: Any = Unset
            source(
                { value ->
                    val innerChannel = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
                    orderChannel.send(innerChannel)
                    launch {
                        semaphore.withPermit {
                            transform(value).source(
                                { inner -> innerChannel.send(Signal.Upstream.Next(inner)); Signal.Downstream.Request },
                                { innerChannel.close() },
                                { issue -> innerChannel.close(issue) },
                            )
                        }
                    }
                    Signal.Downstream.Request
                },
                { },
                { issue -> outerError = issue },
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

/**
 * Maps each item to a [Many], cancels the previous inner subscription when a new item arrives,
 * and subscribes to the latest inner [Many].  Only the most recent inner stream is active at any time.
 */
fun <T : Any, R : Any> Many<T>.switchMap(transform: (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
        val channel = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
        coroutineScope {
            val producerJob = launch {
                var activeJob = launch {}
                source(
                    { value ->
                        activeJob.cancelAndJoin()
                        activeJob = launch {
                            transform(value).source(
                                { inner -> channel.send(Signal.Upstream.Next(inner)); Signal.Downstream.Request },
                                { },
                                { issue -> channel.send(Signal.Upstream.Error(issue)) },
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
fun <T : Any> Many<T>.takeUntilOther(other: Publisher<*>): Many<T> =
    Many.generate { emit ->
        val channel = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
        coroutineScope {
            // Control publisher — first signal stops the main source.
            val controlJob = launch {
                Many.from(other).source(
                    { channel.send(Signal.Upstream.Complete); Signal.Downstream.Cancel },
                    { channel.send(Signal.Upstream.Complete) },
                    { channel.send(Signal.Upstream.Complete) },
                )
            }
            val producerJob = launch {
                source(
                    { value -> channel.send(Signal.Upstream.Next(value)); Signal.Downstream.Request },
                    { channel.send(Signal.Upstream.Complete) },
                    { issue -> channel.send(Signal.Upstream.Error(issue)) },
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

fun <T : Any> Many<T>.mergeWith(other: Many<T>): Many<T> = merge(this, other)

/**
 * Collects items into fixed-size lists of [size] and emits each list downstream.
 * A partial list is emitted on upstream completion.
 */
fun <T : Any> Many<T>.buffer(size: Int): Many<List<T>> {
    require(size > 0) { "buffer size must be positive, got $size" }
    return Many.generate { emit ->
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
                if (bucket.isNotEmpty()) {
                    if (emit(Signal.Upstream.Next(bucket.toList())) == Signal.Downstream.Cancel)
                        return@source
                }
                emit(Signal.Upstream.Complete)
            },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }
}

/**
 * Collects items into overlapping or sliding windows of [size], advancing by [skip] items each time.
 * - `skip == size`: non-overlapping (equivalent to [buffer]).
 * - `skip < size`: overlapping windows.
 * - `skip > size`: gaps between windows (some items are skipped).
 */
fun <T : Any> Many<T>.buffer(size: Int, skip: Int): Many<List<T>> {
    require(size > 0) { "buffer size must be positive, got $size" }
    require(skip > 0) { "buffer skip must be positive, got $skip" }
    return Many.generate { emit ->
        val buffers = ArrayDeque<MutableList<T>>()
        var index = 0L
        source(
            { value ->
                if (index % skip == 0L) buffers.addLast(mutableListOf())
                buffers.forEach { it.add(value) }
                val full = buffers.firstOrNull { it.size == size }
                index++
                if (full != null) {
                    val downstream = emit(Signal.Upstream.Next(full.toList()))
                    buffers.removeFirst()
                    downstream
                } else Signal.Downstream.Request
            },
            {
                // Emit partial trailing buffers, consistent with buffer(size).
                for (partial in buffers) {
                    if (partial.isNotEmpty()) {
                        if (emit(Signal.Upstream.Next(partial.toList())) == Signal.Downstream.Cancel)
                            return@source
                    }
                }
                emit(Signal.Upstream.Complete)
            },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }
}

/**
 * Collects items into a list and emits it when either [size] items have accumulated or [timeout]
 * elapses since the first item entered the current bucket — whichever comes first.
 * An incomplete bucket is emitted on upstream Complete.
 */
fun <T : Any> Many<T>.bufferTimeout(size: Int, timeout: Duration): Many<List<T>> {
    require(size > 0) { "buffer size must be positive, got $size" }
    require(timeout.isPositive()) { "timeout must be positive, got $timeout" }
    return Many.generate { emit ->
        // Merged event channel: Left = source signal, Right = timer tick sentinel.
        val events = Channel<Either<Signal.Upstream<T>, Unit>>(Channel.BUFFERED)
        coroutineScope {
            val producerJob = launch {
                source(
                    { value ->
                        events.send(Either.Left(Signal.Upstream.Next(value)))
                        Signal.Downstream.Request
                    },
                    {
                        events.send(Either.Left(Signal.Upstream.Complete))
                    },
                    { issue ->
                        events.send(Either.Left(Signal.Upstream.Error(issue)))
                    },
                )
                events.close()
            }
            val bucket = mutableListOf<T>()
            var timerJob: Job = Job().also { it.complete() }

            fun resetTimer() {
                timerJob.cancel()
                timerJob = launch { delay(timeout); events.trySend(Either.Right(Unit)) }
            }

            suspend fun flushBucket(): Boolean {
                if (bucket.isEmpty()) return true
                val downstream = emit(Signal.Upstream.Next(bucket.toList()))
                bucket.clear()
                timerJob.cancel()
                timerJob = Job().also { it.complete() }
                return downstream != Signal.Downstream.Cancel
            }

            var terminated = false
            for (event in events) {
                when (event) {
                    is Success -> {
                        if (!flushBucket()) { producerJob.cancel(); terminated = true; break }
                    }
                    is Failure -> when (val signal = event.value) {
                        is Signal.Upstream.Next -> {
                            if (bucket.isEmpty()) resetTimer()
                            bucket.add(signal.value)
                            if (bucket.size >= size) {
                                if (!flushBucket()) { producerJob.cancel(); terminated = true; break }
                            }
                        }
                        is Signal.Upstream.Complete -> {
                            timerJob.cancel()
                            flushBucket()
                            break
                        }
                        is Signal.Upstream.Error -> {
                            timerJob.cancel()
                            emit(Signal.Upstream.Error(signal.cause))
                            terminated = true
                            break
                        }
                    }
                }
            }
            timerJob.cancel()
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
fun <T : Any, K : Any, R : Any> Many<T>.groupBy(
    keySelector: (T) -> K,
    groupHandler: (key: K, group: Many<T>) -> Many<R>,
): Many<R> = Many.generate { emit ->
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
                        launch {
                            val groupMany = Many.generate<T> { groupEmit ->
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
fun <T : Any> Many<T>.onBackpressureDrop(): Many<T> =
    Many.generate { emit ->
        val channel = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
        coroutineScope {
            val producerJob = launch {
                source(
                    { value ->
                        // Non-blocking offer: drop if the channel buffer is full.
                        channel.trySend(Signal.Upstream.Next(value))
                        Signal.Downstream.Request
                    },
                    {
                        channel.send(Signal.Upstream.Complete)  // must not lose Complete
                    },
                    { issue ->
                        channel.send(Signal.Upstream.Error(issue))  // must not lose Error
                    },
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
fun <T : Any> Many<T>.recoverWith(fallback: (Exception) -> T): Many<T> =
    Many.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        when (result) {
            is Success  -> emit(Signal.Upstream.Complete)
            is Failure -> {
                if (emit(Signal.Upstream.Next(fallback(result.value))) != Signal.Downstream.Cancel)
                    emit(Signal.Upstream.Complete)
            }
        }
    }

fun <T : Any> Many<T>.concatWith(other: Many<T>): Many<T> = concat(this, other)

fun <T : Any> Many<T>.flatMapNone(transform: (T) -> None<*>): None<T> =
    concatMap { value -> transform(value).then { Many.items(value) } }.discard()

/** Suspend variant of [flatMapNone] on [Many]. */
@LowPriorityInOverloadResolution
fun <T : Any> Many<T>.flatMapNone(transform: suspend (T) -> None<*>): None<T> =
    concatMap(transform = suspend { value: T -> transform(value).then { Many.items(value) } }).discard()

/**
 * Maps each item to a [One] and flattens, subscribing concurrently up to the default concurrency.
 *
 * Sugar over [flatMap] that keeps the call-site type unambiguous when [transform] returns a [One]
 * rather than a [Many].  Each inner [One] is lifted to a [Many] before merging, so ordering is
 * not guaranteed when concurrency > 1.
 */
fun <T : Any, R : Any> Many<T>.flatMapOne(transform: (T) -> One<R>): Many<R> =
    flatMap { value: T -> Many.from(transform(value)) }

/** Suspend variant of [flatMapOne] on [Many]. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Many<T>.flatMapOne(transform: suspend (T) -> One<R>): Many<R> {
    val asManyTransform: suspend (T) -> Many<R> = { value -> Many.from(transform(value)) }
    return flatMap(transform = asManyTransform)
}
