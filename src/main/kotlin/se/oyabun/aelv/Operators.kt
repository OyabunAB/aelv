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
import kotlinx.coroutines.CancellationException
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
import kotlinx.coroutines.yield
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
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

/** Maps each element to a [One] and flattens, subscribing concurrently up to [concurrency] at a time. */
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
 * Delays subscription to this [Many] until the [trigger] publisher emits an item or completes.
 * The trigger's first signal starts the subscription; the trigger itself is then cancelled.
 * If the trigger errors, the error is forwarded and this source is never subscribed.
 */
fun <T : Any> Many<T>.delaySubscription(trigger: Publisher<*>): Many<T> =
    Many.generate { emit ->
        var triggerFailed = false
        Many.from(trigger).source(
            { Signal.Downstream.Cancel },
            { },
            { issue -> triggerFailed = true; emit(Signal.Upstream.Error(issue)) },
        )
        if (triggerFailed) return@generate
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Merges all [sources] into a single [Many], interleaving items as they arrive.
 * Completes when all sources have completed.  Errors from any source are forwarded immediately.
 */
fun <T : Any> merge(vararg sources: Many<T>): Many<T> =
    Many.generate { emit ->
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
fun <T : Any> concat(vararg sources: Many<T>): Many<T> =
    Many.generate { emit ->
        for (src in sources) {
            var cancelled = false
            var errored = false
            src.source(
                { value ->
                    if (emit(Signal.Upstream.Next(value)) == Signal.Downstream.Cancel) {
                        cancelled = true; Signal.Downstream.Cancel
                    } else Signal.Downstream.Request
                },
                { },
                { issue -> emit(Signal.Upstream.Error(issue)); errored = true },
            )
            if (cancelled || errored) return@generate
        }
        emit(Signal.Upstream.Complete)
    }

/**
 * Pairs items from [a] and [b] by position, applying [transform] to each pair.
 * Completes when the shorter source is exhausted.
 */
fun <A : Any, B : Any, R : Any> zip(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
    Many.generate { emit ->
        val channelA = Channel<Signal.Upstream<A>>(Channel.BUFFERED)
        val channelB = Channel<Signal.Upstream<B>>(Channel.BUFFERED)
        coroutineScope {
            val jobA = launch {
                a.source(
                    { value -> channelA.send(Signal.Upstream.Next(value)); Signal.Downstream.Request },
                    { channelA.send(Signal.Upstream.Complete) },
                    { issue -> channelA.send(Signal.Upstream.Error(issue)) },
                )
            }
            val jobB = launch {
                b.source(
                    { value -> channelB.send(Signal.Upstream.Next(value)); Signal.Downstream.Request },
                    { channelB.send(Signal.Upstream.Complete) },
                    { issue -> channelB.send(Signal.Upstream.Error(issue)) },
                )
            }
            var zipError: Any = Unset
            while (when (val signalA = channelA.receive()) {
                is Signal.Upstream.Complete -> false
                is Signal.Upstream.Error    -> { zipError = signalA.cause; false }
                is Signal.Upstream.Next     -> when (val signalB = channelB.receive()) {
                    is Signal.Upstream.Complete -> false
                    is Signal.Upstream.Error    -> { zipError = signalB.cause; false }
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
 * Emits a combined value whenever either [a] or [b] emits, using the most recent value from the
 * other source.  Does not emit until both sources have emitted at least one item.
 */
fun <A : Any, B : Any, R : Any> combineLatest(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
    Many.generate { emit ->
        // Channel carries tagged values (Left = from a, Right = from b) plus errors.
        // All producer signals pass through the channel — single collector, serial emit — RS 1.3.
        val channel = Channel<Signal.Upstream<Either<A, B>>>(Channel.BUFFERED)
        coroutineScope {
            val jobA = launch {
                a.source(
                    { value -> channel.send(Signal.Upstream.Next(Either.Left(value))); Signal.Downstream.Request },
                    { },
                    { issue -> channel.send(Signal.Upstream.Error(issue)) },
                )
            }
            val jobB = launch {
                b.source(
                    { value -> channel.send(Signal.Upstream.Next(Either.Right(value))); Signal.Downstream.Request },
                    { },
                    { issue -> channel.send(Signal.Upstream.Error(issue)) },
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
                        is Failure  -> {
                            latestA = tagged.value.right()
                            val capturedB = latestB
                            if (capturedB is Success) {
                                if (emit(Signal.Upstream.Next(transform(tagged.value, capturedB.value))) == Signal.Downstream.Cancel) {
                                    terminated = true; break
                                }
                            }
                        }
                        is Success -> {
                            latestB = tagged.value.right()
                            val capturedA = latestA
                            if (capturedA is Success) {
                                if (emit(Signal.Upstream.Next(transform(capturedA.value, tagged.value))) == Signal.Downstream.Cancel) {
                                    terminated = true; break
                                }
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
 * Drops upstream items that arrive when the downstream has no pending demand.
 *
 * Use this only when data loss is acceptable — issue.g. high-frequency sensor readings where
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

fun <T : Any> Many<T>.doOnNext(action: (T) -> Unit): Many<T> =
    Many.generate { emit ->
        source(
            { value -> action(value); emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doOnNext] — [action] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> Many<T>.doOnNext(action: suspend (T) -> Unit): Many<T> =
    Many.generate { emit ->
        source(
            { value -> action(value); emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

fun <T : Any> Many<T>.doOnComplete(action: () -> Unit): Many<T> =
    Many.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { action(); emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doOnComplete] — [action] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> Many<T>.doOnComplete(action: suspend () -> Unit): Many<T> =
    Many.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { action(); emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

fun <T : Any> Many<T>.doOnError(action: (Exception) -> Unit): Many<T> =
    Many.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> action(issue); emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doOnError] — [action] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> Many<T>.doOnError(action: suspend (Exception) -> Unit): Many<T> =
    Many.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> action(issue); emit(Signal.Upstream.Error(issue)) },
        )
    }

fun <T : Any> Many<T>.doOnSubscribe(action: () -> Unit): Many<T> =
    Many.generate { emit ->
        action()
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Invokes [action] when the stream terminates for any reason: normal completion, error, or
 * downstream cancellation.  The [Signal.Terminal] argument identifies which terminal was received.
 */
fun <T : Any> Many<T>.doFinally(action: (Signal.Terminal) -> Unit): Many<T> =
    Many.generate { emit ->
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
fun <T : Any> Many<T>.doFinally(action: suspend (Signal.Terminal) -> Unit): Many<T> =
    Many.generate { emit ->
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

/**
 * On error, switches to the [Many] returned by [fallback], continuing from there.
 * On normal completion, [fallback] is not invoked.
 */
fun <T : Any> Many<T>.recover(fallback: (Exception) -> Many<T>): Many<T> =
    Many.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        if (result is Failure) fallback(result.value).source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
        else emit(Signal.Upstream.Complete)
    }

/** Suspend variant of [recover] — [fallback] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> Many<T>.recover(fallback: suspend (Exception) -> Many<T>): Many<T> =
    Many.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        if (result is Failure) fallback(result.value).source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
        else emit(Signal.Upstream.Complete)
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

/** Re-subscribes to the source on error, up to [times] times.  Defaults to unbounded retries. */
fun <T : Any> Many<T>.retry(times: Long = Long.MAX_VALUE): Many<T> =
    retry(Policy.retry().maxAttempts(times))

/**
 * Re-subscribes to the source on error according to [policy].
 * The policy controls the error filter, maximum attempt count, and backoff strategy.
 */
fun <T : Any> Many<T>.retry(policy: Policy.Retry): Many<T> =
    Many.generate { emit ->
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

/**
 * Switches the dispatcher on which downstream [emit] calls execute.
 * The source continues running on whatever dispatcher it was already using;
 * only the hand-off to the subscriber is moved to [context].
 */
fun <T : Any> Many<T>.publishOn(context: CoroutineContext): Many<T> =
    Many.generate { emit ->
        source(
            { value -> withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Next(value)) } },
            { withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Complete) } },
            { issue -> withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Error(issue)) } },
        )
    }

/**
 * Switches the dispatcher on which the source lambda executes.
 * Item production runs on [context]; the emit calls themselves remain on the caller's dispatcher.
 */
fun <T : Any> Many<T>.subscribeOn(context: CoroutineContext): Many<T> =
    Many.generate { emit ->
        withContext(currentCoroutineContext() + context) {
            source(
                { value -> emit(Signal.Upstream.Next(value)) },
                { emit(Signal.Upstream.Complete) },
                { issue -> emit(Signal.Upstream.Error(issue)) },
            )
        }
    }

/**
 * Pairs the values of [a] and [b], applying [transform] once both have emitted.
 * If either source completes without emitting, the result completes empty.
 */
fun <A : Any, B : Any, R : Any> zip(a: One<A>, b: One<B>, transform: (A, B) -> R): One<R> =
    One.generate { emit ->
        var valueA: Either<Unset, A> = Unset.left()
        val resultA = a.collect { v -> valueA = v.right(); Signal.Downstream.Cancel }
        if (resultA is Failure) { emit(Signal.Upstream.Error(resultA.value)); return@generate }
        val finalA = valueA
        var valueB: Either<Unset, B> = Unset.left()
        val resultB = b.collect { v -> valueB = v.right(); Signal.Downstream.Cancel }
        if (resultB is Failure) { emit(Signal.Upstream.Error(resultB.value)); return@generate }
        val finalB = valueB
        when (finalA) {
            is Failure  -> emit(Signal.Upstream.Complete)
            is Success -> when (finalB) {
                is Failure  -> emit(Signal.Upstream.Complete)
                is Success -> {
                    if (emit(Signal.Upstream.Next(transform(finalA.value, finalB.value))) != Signal.Downstream.Cancel)
                        emit(Signal.Upstream.Complete)
                }
            }
        }
    }

fun <A : Any, B : Any, R : Any> One<A>.zipWith(other: One<B>, transform: (A, B) -> R): One<R> =
    zip(this, other, transform)

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

/** Suspend variant of [flatMapMany] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> One<T>.flatMapMany(transform: suspend (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
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
fun <T : Any, R : Any> One<T>.flatMapNone(transform: (T) -> None<R>): None<R> =
    None.generate {
        source(
            { value ->
                val innerResult = transform(value).await()
                if (innerResult is Failure) throw innerResult.value
                Signal.Downstream.Request
            },
            { },
            ::rethrow,
        )
    }

/** Suspend variant of [flatMapNone] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> One<T>.flatMapNone(transform: suspend (T) -> None<R>): None<R> =
    None.generate {
        source(
            { value ->
                val innerResult = transform(value).await()
                if (innerResult is Failure) throw innerResult.value
                Signal.Downstream.Request
            },
            { },
            ::rethrow,
        )
    }

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
            { value -> action(value); emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doOnNext] — [action] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> One<T>.doOnNext(action: suspend (T) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> action(value); emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

fun <T : Any> One<T>.doOnError(action: (Exception) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> action(issue); emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [doOnError] — [action] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> One<T>.doOnError(action: suspend (Exception) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> action(issue); emit(Signal.Upstream.Error(issue)) },
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

/**
 * Sequences this [None] with a [One] producer: awaits completion of the [None], then subscribes
 * to the [One] returned by [producer].
 *
 * If this [None] errors, [producer] is never called and the error is forwarded.  This is the
 * primary way to chain a fire-and-forget step before a value-producing step without nesting.
 */
fun <T : Any, R : Any> None<T>.then(producer: () -> One<R>): One<R> =
    One.generate { emit ->
        val result = await()
        if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
        producer().source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [then] returning [One]. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> None<T>.then(producer: suspend () -> One<R>): One<R> =
    One.generate { emit ->
        val result = await()
        if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
        producer().source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Sequences this [None] with a [Maybe] producer.  The [Maybe] is only subscribed if this [None]
 * completes without error; an error in the [None] is forwarded and [producer] is skipped.
 */
fun <T : Any, R : Any> None<T>.then(producer: () -> Maybe<R>): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        val result = await()
        if (result is Failure) { onError(result.value); return@Maybe }
        producer().source(onNext, onComplete, onError)
    }

/** Suspend variant of [then] returning [Maybe]. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> None<T>.then(producer: suspend () -> Maybe<R>): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        val result = await()
        if (result is Failure) { onError(result.value); return@Maybe }
        producer().source(onNext, onComplete, onError)
    }

/**
 * Sequences this [None] with a [Many] producer.  The [Many] is only subscribed if this [None]
 * completes without error; an error in the [None] terminates the stream without subscribing to
 * [producer].
 */
fun <T : Any, R : Any> None<T>.then(producer: () -> Many<R>): Many<R> =
    Many.generate { emit ->
        val result = await()
        if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
        producer().source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [then] returning [Many]. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> None<T>.then(producer: suspend () -> Many<R>): Many<R> =
    Many.generate { emit ->
        val result = await()
        if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
        producer().source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Sequences two [None]s: awaits this one, then awaits the [None] returned by [producer].
 * Any error from either step is rethrown, short-circuiting the second step if the first fails.
 */
fun <T : Any, R : Any> None<T>.then(producer: () -> None<R>): None<R> =
    None.generate {
        val result = await()
        if (result is Failure) throw result.value
        producer().await().let { if (it is Failure) throw it.value }
    }

/** Suspend variant of [then] returning [None]. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> None<T>.then(producer: suspend () -> None<R>): None<R> =
    None.generate {
        val result = await()
        if (result is Failure) throw result.value
        producer().await().let { if (it is Failure) throw it.value }
    }

/**
 * For each upstream item, applies [transform] and awaits the resulting [None] before requesting
 * the next item.  Items are processed sequentially — the next item is not consumed until the
 * [None] from the current item has completed.
 *
 * The output type is [None] because no values are emitted; the operator is used purely for
 * side-effecting work (e.g. writes, deletes) that must complete before moving on.
 */
fun <T : Any> Many<T>.flatMapNone(transform: (T) -> None<Any>): None<T> =
    None.generate {
        source(
            { value ->
                val result = transform(value).await()
                if (result is Failure) throw result.value
                Signal.Downstream.Request
            },
            { },
            ::rethrow,
        )
    }

/** Suspend variant of [flatMapNone] on [Many]. */
@LowPriorityInOverloadResolution
fun <T : Any> Many<T>.flatMapNone(transform: suspend (T) -> None<Any>): None<T> =
    None.generate {
        source(
            { value ->
                val result = transform(value).await()
                if (result is Failure) throw result.value
                Signal.Downstream.Request
            },
            { },
            ::rethrow,
        )
    }

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

fun <T : Any, R : Any> Maybe<T>.map(transform: (T) -> R): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> onNext(transform(value)) },
            onComplete,
            onError,
        )
    }

/** Suspend variant of [map] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Maybe<T>.map(transform: suspend (T) -> R): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> onNext(transform(value)) },
            onComplete,
            onError,
        )
    }

/** Keeps the value if [predicate] returns true, otherwise produces an empty [Maybe]. */
fun <T : Any> Maybe<T>.filter(predicate: (T) -> Boolean): Maybe<T> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> if (predicate(value)) onNext(value) else { onComplete(); Signal.Downstream.Cancel } },
            onComplete,
            onError,
        )
    }

/** Suspend variant of [filter]. */
@LowPriorityInOverloadResolution
fun <T : Any> Maybe<T>.filter(predicate: suspend (T) -> Boolean): Maybe<T> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> if (predicate(value)) onNext(value) else { onComplete(); Signal.Downstream.Cancel } },
            onComplete,
            onError,
        )
    }

fun <T : Any, R : Any> Maybe<T>.flatMap(transform: (T) -> Maybe<R>): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> transform(value).source(onNext, onComplete, onError); Signal.Downstream.Cancel },
            onComplete,
            onError,
        )
    }

/** Suspend variant of [flatMap] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Maybe<T>.flatMap(transform: suspend (T) -> Maybe<R>): Maybe<R> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> transform(value).source(onNext, onComplete, onError); Signal.Downstream.Cancel },
            onComplete,
            onError,
        )
    }

/**
 * Maps the present value to a [Many] and subscribes to it; if this [Maybe] is empty the result
 * completes empty without invoking [transform].
 *
 * The absent case propagates as an empty [Many] rather than an error, so callers cannot
 * distinguish between "Maybe was empty" and "inner Many was empty" at the output level — both
 * yield a [Many] that completes with zero items.
 */
fun <T : Any, R : Any> Maybe<T>.flatMapMany(transform: (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
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

/** Suspend variant of [flatMapMany] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Maybe<T>.flatMapMany(transform: suspend (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
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
 * Maps the present value to a [None] and awaits completion; if this [Maybe] is empty, completes
 * immediately without invoking [transform].
 *
 * Useful for fire-and-forget side effects that should be skipped when no value is present.
 */
fun <T : Any, R : Any> Maybe<T>.flatMapNone(transform: (T) -> None<R>): None<R> =
    None.generate {
        source(
            { value ->
                val result = transform(value).await()
                if (result is Failure) throw result.value
                Signal.Downstream.Cancel
            },
            { },
            ::rethrow,
        )
    }

/** Suspend variant of [flatMapNone] — [transform] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any, R : Any> Maybe<T>.flatMapNone(transform: suspend (T) -> None<R>): None<R> =
    None.generate {
        source(
            { value ->
                val result = transform(value).await()
                if (result is Failure) throw result.value
                Signal.Downstream.Cancel
            },
            { },
            ::rethrow,
        )
    }

/**
 * Provides a fallback value when this [Maybe] is empty, producing a [One].
 *
 * If this [Maybe] emits a value, that value is forwarded. If it completes empty,
 * [fallback] is invoked and its result is emitted.
 */
fun <T : Any> Maybe<T>.or(fallback: () -> T): One<T> =
    One.generate { emit ->
        var emitted = false
        source(
            { value -> emitted = true; emit(Signal.Upstream.Next(value)) },
            {
                if (!emitted) {
                    emit(Signal.Upstream.Next(fallback()))
                }
                emit(Signal.Upstream.Complete)
            },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [or] — [fallback] may call suspend functions. */
@LowPriorityInOverloadResolution
fun <T : Any> Maybe<T>.or(fallback: suspend () -> T): One<T> =
    One.generate { emit ->
        var emitted = false
        source(
            { value -> emitted = true; emit(Signal.Upstream.Next(value)) },
            {
                if (!emitted) {
                    emit(Signal.Upstream.Next(fallback()))
                }
                emit(Signal.Upstream.Complete)
            },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Switches to [fallback] stream when this [Maybe] is empty.
 *
 * If this [Maybe] emits a value, that value is forwarded and [fallback] is never subscribed.
 * If it completes empty, [fallback] is subscribed and its items are forwarded.
 */
fun <T : Any> Maybe<T>.orMany(fallback: () -> Many<T>): Many<T> =
    Many.generate { emit ->
        var emitted = false
        source(
            { value -> emitted = true; emit(Signal.Upstream.Next(value)) },
            {
                if (!emitted) {
                    fallback().source(
                        { inner -> emit(Signal.Upstream.Next(inner)) },
                        { emit(Signal.Upstream.Complete) },
                        { issue -> emit(Signal.Upstream.Error(issue)) },
                    )
                } else {
                    emit(Signal.Upstream.Complete)
                }
            },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Suspend variant of [orMany]. */
@LowPriorityInOverloadResolution
fun <T : Any> Maybe<T>.orMany(fallback: suspend () -> Many<T>): Many<T> =
    Many.generate { emit ->
        var emitted = false
        source(
            { value -> emitted = true; emit(Signal.Upstream.Next(value)) },
            {
                if (!emitted) {
                    fallback().source(
                        { inner -> emit(Signal.Upstream.Next(inner)) },
                        { emit(Signal.Upstream.Complete) },
                        { issue -> emit(Signal.Upstream.Error(issue)) },
                    )
                } else {
                    emit(Signal.Upstream.Complete)
                }
            },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/** Converts to a [Many] that emits zero or one items. */
fun <T : Any> Maybe<T>.toMany(): Many<T> =
    Many.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
        )
    }

/**
 * Converts to a [One], throwing [NoSuchElementException] if this [Maybe] is empty.
 *
 * Use [or] when the empty case is expected and a fallback is available.
 */
fun <T : Any> Maybe<T>.toOne(): One<T> =
    One.defer {
        var result: T? = null
        collect { value -> result = value; Signal.Downstream.Cancel }
        result ?: throw NoSuchElementException()
    }

fun <T : Any> Maybe<T>.doOnNext(action: (T) -> Unit): Maybe<T> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> action(value); onNext(value) },
            onComplete,
            onError,
        )
    }

/** Suspend variant of [doOnNext]. */
@LowPriorityInOverloadResolution
fun <T : Any> Maybe<T>.doOnNext(action: suspend (T) -> Unit): Maybe<T> =
    Maybe { onNext, onComplete, onError ->
        source(
            { value -> action(value); onNext(value) },
            onComplete,
            onError,
        )
    }

fun <T : Any> Maybe<T>.doOnComplete(action: () -> Unit): Maybe<T> =
    Maybe { onNext, onComplete, onError ->
        source(onNext, { action(); onComplete() }, onError)
    }

fun <T : Any> Maybe<T>.doOnError(action: (Exception) -> Unit): Maybe<T> =
    Maybe { onNext, onComplete, onError ->
        source(onNext, onComplete, { issue -> action(issue); onError(issue) })
    }

fun <T : Any> Maybe<T>.recover(fallback: (Exception) -> Maybe<T>): Maybe<T> =
    Maybe { onNext, onComplete, onError ->
        source(
            onNext,
            onComplete,
            { issue -> fallback(issue).source(onNext, onComplete, onError) },
        )
    }

/** Suspend variant of [recover]. */
@LowPriorityInOverloadResolution
fun <T : Any> Maybe<T>.recover(fallback: suspend (Exception) -> Maybe<T>): Maybe<T> =
    Maybe { onNext, onComplete, onError ->
        source(
            onNext,
            onComplete,
            { issue -> fallback(issue).source(onNext, onComplete, onError) },
        )
    }

suspend fun <T : Any> Maybe<T>.await(): Either<Exception, T?> = Either.catching {
    var result: T? = null
    source(
        { value -> result = value; Signal.Downstream.Cancel },
        { },
        ::rethrow,
    )
    result
}
