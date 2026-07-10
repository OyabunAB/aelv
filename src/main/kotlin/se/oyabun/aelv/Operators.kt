package se.oyabun.aelv

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
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import org.reactivestreams.Publisher

private val log = Logging.of<Many<*>>()

/** Transforms each item by applying [transform] to it. */
fun <T : Any, R : Any> Many<T>.map(transform: (T) -> R): Many<R> {
    val f = fusion
    return Many.fused(if (f is Fusion.Available) MapFusion(f, transform) else Fusion.None) { onNext, onComplete, onError ->
        source({ value -> onNext(transform(value)) }, onComplete, onError)
    }
}

/**
 * Applies [transform] to each item and emits the result only when it is non-null.
 * Null results are silently dropped and demand is replenished from upstream.
 */
fun <T : Any, R : Any> Many<T>.mapNotNull(transform: (T) -> R?): Many<R> =
    Many.build { onNext, onComplete, onError ->
        source(
            { value -> val r = transform(value); if (r != null) onNext(r) else Signal.Downstream.Request },
            onComplete,
            onError,
        )
    }

/** Emits only items for which [predicate] returns true. */
fun <T : Any> Many<T>.filter(predicate: (T) -> Boolean): Many<T> {
    val f = fusion
    return Many.fused(if (f is Fusion.Available) FilterFusion(f, predicate) else Fusion.None) { onNext, onComplete, onError ->
        source(
            { value -> if (predicate(value)) onNext(value) else Signal.Downstream.Request },
            onComplete,
            onError,
        )
    }
}

/** Emits at most [n] items then completes.  Requires `n >= 0`. */
fun <T : Any> Many<T>.take(n: Long): Many<T> {
    require(n >= 0) { "take count must be non-negative, got $n" }
    val f = fusion
    val slowPath: suspend (
        onNext: suspend (T) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError: suspend (AelvException) -> Unit,
    ) -> Unit = block@{ onNext, onComplete, onError ->
        if (n == 0L) { onComplete(); return@block }
        var remaining = n
        source(
            { value ->
                when {
                    remaining == 0L -> Signal.Downstream.Cancel
                    else -> {
                        val downstream = onNext(value)
                        remaining--
                        if (remaining == 0L) {
                            if (downstream != Signal.Downstream.Cancel) onComplete()
                            Signal.Downstream.Cancel
                        } else downstream
                    }
                }
            },
            onComplete,
            onError,
        )
    }
    return Many.fused(if (f is Fusion.Available) TakeFusion(f, n) else Fusion.None, slowPath)
}

/** Emits items while [predicate] returns true; completes on the first non-matching item. */
fun <T : Any> Many<T>.takeWhile(predicate: (T) -> Boolean): Many<T> =
    Many.build { onNext, onComplete, onError ->
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
    return Many.build { onNext, onComplete, onError ->
        var skipped = 0L
        source(
            { value -> if (skipped < n) { skipped++; Signal.Downstream.Request } else onNext(value) },
            onComplete,
            onError,
        )
    }
}

/** Drops items while [predicate] returns true; emits all items once the predicate first returns false. */
fun <T : Any> Many<T>.skipWhile(predicate: (T) -> Boolean): Many<T> =
    Many.build { onNext, onComplete, onError ->
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

/** Emits only items that have not been seen before, using [equals] for comparison. */
fun <T : Any> Many<T>.distinct(): Many<T> =
    Many.build { onNext, onComplete, onError ->
        val seen = HashSet<T>()
        source({ value -> if (seen.add(value)) onNext(value) else Signal.Downstream.Request }, onComplete, onError)
    }

/** Suppresses consecutive duplicate items; non-adjacent duplicates are still emitted. */
fun <T : Any> Many<T>.distinctUntilChanged(): Many<T> =
    Many.build { onNext, onComplete, onError ->
        var last: Any = Unset
        source(
            { value -> if (value != last) { last = value; onNext(value) } else Signal.Downstream.Request },
            onComplete,
            onError,
        )
    }

/** Suppresses consecutive items whose [key] projection produces the same value. */
fun <T : Any, K : Any> Many<T>.distinctUntilChangedBy(key: (T) -> K): Many<T> =
    Many.build { onNext, onComplete, onError ->
        var lastKey: Any = Unset
        source(
            { value ->
                val k = key(value)
                if (k != lastKey) { lastKey = k; onNext(value) } else Signal.Downstream.Request
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
): Many<R> = if (concurrency == 1) concatMap(transform) else Many.generate { emit ->
    val semaphore = Semaphore(concurrency)
    val mutex     = Mutex()
    var cancelled = false
    var outerError: AelvException? = null
    coroutineScope {
        source(
            { value ->
                if (cancelled) return@source Signal.Downstream.Cancel
                semaphore.acquire()
                try {
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
                        { e -> mutex.withLock { if (outerError == null) outerError = e } },
                    )
                } finally {
                    semaphore.release()
                }
                Signal.Downstream.Request
            },
            {},
            { e -> outerError = e },
        )
    }
    when {
        cancelled          -> {}
        outerError != null -> emit(Signal.Upstream.Error(outerError!!))
        else               -> emit(Signal.Upstream.Complete)
    }
}

/** Maps each item to a [Many] and subscribes sequentially, preserving upstream order. */
fun <T : Any, R : Any> Many<T>.concatMap(transform: (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
        var cancelled = false
        var outerError: AelvException? = null
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
                    { e -> outerError = e },
                )
                if (cancelled) Signal.Downstream.Cancel else Signal.Downstream.Request
            },
            {
                if (outerError != null) emit(Signal.Upstream.Error(outerError!!))
                else emit(Signal.Upstream.Complete)
            },
            { e -> emit(Signal.Upstream.Error(e)) },
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
            var outerError: Signal.Upstream.Error? = null
            source(
                { value ->
                    semaphore.acquire()
                    val innerChannel = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
                    orderChannel.send(innerChannel)
                    launch {
                        try {
                            transform(value).source(
                                { inner -> innerChannel.send(Signal.Upstream.Next(inner)); Signal.Downstream.Request },
                                { innerChannel.close() },
                                { e -> innerChannel.close(e) },
                            )
                        } finally {
                            semaphore.release()
                            innerChannel.close()
                        }
                    }
                    Signal.Downstream.Request
                },
                { },
                { e -> outerError = Signal.Upstream.Error(e) },
            )
            orderChannel.send(Channel<Signal.Upstream<R>>(0).also { it.close(outerError?.cause) })
            orderChannel.close()
        }
        var cancelled = false
        for (innerChannel in orderChannel) {
            if (cancelled) { innerChannel.cancel(); continue }
            for (signal in innerChannel) {
                when (signal) {
                    is Signal.Upstream.Next -> if (emit(signal) == Signal.Downstream.Cancel) {
                        cancelled = true; producerJob.cancel(); break
                    }
                    else -> break
                }
            }
            val failure = innerChannel.receiveCatching().exceptionOrNull()
            if (failure != null && !cancelled) {
                cancelled = true
                producerJob.cancel()
                emit(Signal.Upstream.Error(if (failure is AelvException) failure else UpstreamErrorException(failure)))
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
                var activeJob: Job? = null
                source(
                    { value ->
                        activeJob?.cancelAndJoin()
                        activeJob = launch {
                            transform(value).source(
                                { inner -> channel.send(Signal.Upstream.Next(inner)); Signal.Downstream.Request },
                                { },
                                { e -> channel.send(Signal.Upstream.Error(e)) },
                            )
                        }
                        Signal.Downstream.Request
                    },
                    {
                        activeJob?.join()
                        channel.send(Signal.Upstream.Complete)
                    },
                    { e ->
                        activeJob?.cancelAndJoin()
                        channel.send(Signal.Upstream.Error(e))
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
                    { e -> channel.send(Signal.Upstream.Error(e)) },
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

/** Merges this [Many] with [other], interleaving items as they arrive. */
fun <T : Any> Many<T>.mergeWith(other: Many<T>): Many<T> = merge(this, other)

/**
 * Delays subscription to this [Many] until the [trigger] publisher emits an item or completes.
 * The trigger's first signal starts the subscription; the trigger itself is then cancelled.
 * If the trigger errors, the error is forwarded and this source is never subscribed.
 */
fun <T : Any> Many<T>.delaySubscription(trigger: Publisher<*>): Many<T> =
    Many.generate { emit ->
        var triggerError: AelvException? = null
        Many.from(trigger).source(
            { Signal.Downstream.Cancel },
            { },
            { e -> triggerError = e },
        )
        if (triggerError != null) {
            emit(Signal.Upstream.Error(triggerError!!))
            return@generate
        }
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
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
                        { e -> channel.send(Signal.Upstream.Error(e)) },
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
                { e -> emit(Signal.Upstream.Error(e)); errored = true },
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
        // Errors are propagated by closing each channel with a cause — RS 1.3.
        val channelA = Channel<A>(Channel.BUFFERED)
        val channelB = Channel<B>(Channel.BUFFERED)
        coroutineScope {
            val jobA = launch {
                a.source(
                    { value -> channelA.send(value); Signal.Downstream.Request },
                    { channelA.close() },
                    { e -> channelA.close(e) },
                )
            }
            val jobB = launch {
                b.source(
                    { value -> channelB.send(value); Signal.Downstream.Request },
                    { channelB.close() },
                    { e -> channelB.close(e) },
                )
            }
            var error: AelvException? = null
            loop@ for (itemA in channelA) {
                val rb = channelB.receiveCatching()
                when {
                    rb.isFailure -> {
                        val cause = rb.exceptionOrNull()
                        error = if (cause is AelvException) cause else cause?.let { UpstreamErrorException(it) }
                        break@loop
                    }
                    rb.isClosed  -> break@loop
                    else         -> {
                        if (emit(Signal.Upstream.Next(transform(itemA, rb.getOrThrow()))) == Signal.Downstream.Cancel) break@loop
                    }
                }
            }
            // Cancel producers before probing channelA, so we never block on it.
            jobA.cancel()
            jobB.cancel()
            if (error == null) {
                // channelA may already be closed with a cause (source A errored after the loop
                // exited via source B completing).  tryReceive() is non-blocking: returns a
                // failure with the close-cause if A errored, or empty/closed if A completed
                // normally.  We do NOT call receiveCatching() here — that would suspend if the
                // channel is still open with no items (jobA cancelled mid-send).
                val ac = channelA.tryReceive()
                if (ac.isFailure) {
                    val cause = ac.exceptionOrNull()
                    if (cause != null) {
                        error = if (cause is AelvException) cause else UpstreamErrorException(cause)
                    }
                }
            }
            if (error != null) emit(Signal.Upstream.Error(error!!))
            else emit(Signal.Upstream.Complete)
        }
    }

/**
 * Emits a combined value whenever either [a] or [b] emits, using the most recent value from the
 * other source.  Does not emit until both sources have emitted at least one item.
 */
@Suppress("UNCHECKED_CAST")
fun <A : Any, B : Any, R : Any> combineLatest(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
    Many.generate { emit ->
        // Channel carries tagged values (Left = from a, Right = from b) plus errors.
        // All producer signals pass through the channel — single collector, serial emit — RS 1.3.
        val channel = Channel<Signal.Upstream<Either<A, B>>>(Channel.BUFFERED)
        coroutineScope {
            val jobA = launch {
                a.source(
                    { value -> channel.send(Signal.Upstream.Next(value.left())); Signal.Downstream.Request },
                    { },
                    { e -> channel.send(Signal.Upstream.Error(e)) },
                )
            }
            val jobB = launch {
                b.source(
                    { value -> channel.send(Signal.Upstream.Next(value.right())); Signal.Downstream.Request },
                    { },
                    { e -> channel.send(Signal.Upstream.Error(e)) },
                )
            }
            val closerJob = launch {
                jobA.join()
                jobB.join()
                channel.close()
            }
            var latestA: Any = Unset
            var latestB: Any = Unset
            var terminated = false
            for (signal in channel) {
                when (signal) {
                    is Signal.Upstream.Error -> { emit(signal); terminated = true; break }
                    is Signal.Upstream.Complete -> break
                    is Signal.Upstream.Next -> when (val tagged = signal.value) {
                        is Either.Left  -> {
                            latestA = tagged.value
                            if (latestB !== Unset) {
                                if (emit(Signal.Upstream.Next(transform(tagged.value, latestB as B))) == Signal.Downstream.Cancel) {
                                    terminated = true; break
                                }
                            }
                        }
                        is Either.Right -> {
                            latestB = tagged.value
                            if (latestA !== Unset) {
                                if (emit(Signal.Upstream.Next(transform(latestA as A, tagged.value))) == Signal.Downstream.Cancel) {
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
            { e -> emit(Signal.Upstream.Error(e)) },
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
            { e -> emit(Signal.Upstream.Error(e)) },
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
                        events.send(Signal.Upstream.Next(value).left())
                        Signal.Downstream.Request
                    },
                    {
                        events.send(Signal.Upstream.Complete.left())
                    },
                    { e ->
                        events.send(Signal.Upstream.Error(e).left())
                    },
                )
                events.close()
            }
            val bucket = mutableListOf<T>()
            var timerJob: Job? = null

            fun resetTimer() {
                timerJob?.cancel()
                timerJob = launch { delay(timeout); events.trySend(Unit.right()) }
            }

            suspend fun flushBucket(): Boolean {
                if (bucket.isEmpty()) return true
                val downstream = emit(Signal.Upstream.Next(bucket.toList()))
                bucket.clear()
                timerJob?.cancel()
                timerJob = null
                return downstream != Signal.Downstream.Cancel
            }

            var terminated = false
            for (event in events) {
                when (event) {
                    is Either.Right -> {
                        // Timer fired — flush current bucket.
                        if (!flushBucket()) { producerJob.cancel(); terminated = true; break }
                    }
                    is Either.Left -> when (val signal = event.value) {
                        is Signal.Upstream.Next -> {
                            if (bucket.isEmpty()) resetTimer()
                            bucket.add(signal.value)
                            if (bucket.size >= size) {
                                if (!flushBucket()) { producerJob.cancel(); terminated = true; break }
                            }
                        }
                        is Signal.Upstream.Complete -> {
                            timerJob?.cancel()
                            flushBucket()
                            break
                        }
                        is Signal.Upstream.Error -> {
                            timerJob?.cancel()
                            emit(Signal.Upstream.Error(signal.cause))
                            terminated = true
                            break
                        }
                    }
                }
            }
            timerJob?.cancel()
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
    val out           = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
    val remaining     = AtomicInteger(0)
    coroutineScope {
        val producerJob = launch {
            source(
                { value ->
                    val key = keySelector(value)
                    val ch  = groupChannels.getOrPut(key) {
                        val newCh = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
                        remaining.incrementAndGet()
                        launch {
                            val groupMany = Many.generate<T> { groupEmit ->
                                for (upstream in newCh) {
                                    if (groupEmit(upstream) == Signal.Downstream.Cancel) {
                                        newCh.cancel(); break
                                    }
                                }
                            }
                            groupHandler(key, groupMany).source(
                                { inner -> out.send(Signal.Upstream.Next(inner)); Signal.Downstream.Request },
                                { },
                                { e -> out.send(Signal.Upstream.Error(e)) },
                            )
                            if (remaining.decrementAndGet() == 0) out.close()
                        }
                        newCh
                    }
                    ch.send(Signal.Upstream.Next(value))
                    Signal.Downstream.Request
                },
                {
                    for ((_, ch) in groupChannels) runCatching { ch.send(Signal.Upstream.Complete) }
                    if (remaining.get() == 0) out.close()
                },
                { e ->
                    for ((_, ch) in groupChannels) runCatching { ch.send(Signal.Upstream.Error(e)) }
                    if (remaining.get() == 0) out.close()
                },
            )
        }
        var terminated = false
        for (signal in out) {
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
                    { e ->
                        channel.send(Signal.Upstream.Error(e))  // must not lose Error
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

/** Invokes [action] for each item without modifying the stream. */
fun <T : Any> Many<T>.doOnNext(action: (T) -> Unit): Many<T> =
    Many.generate { emit ->
        source(
            { value -> action(value); emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
        )
    }

/** Invokes [action] when the stream completes normally, without modifying the stream. */
fun <T : Any> Many<T>.doOnComplete(action: () -> Unit): Many<T> =
    Many.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { action(); emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
        )
    }

/** Invokes [action] when the stream signals an error, without modifying the stream. */
fun <T : Any> Many<T>.doOnError(action: (AelvException) -> Unit): Many<T> =
    Many.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { e -> action(e); emit(Signal.Upstream.Error(e)) },
        )
    }

/** Invokes [action] when a subscriber subscribes to this stream, before any items are emitted. */
fun <T : Any> Many<T>.doOnSubscribe(action: () -> Unit): Many<T> =
    Many.generate { emit ->
        action()
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
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
            { e -> action(Signal.Upstream.Error(e)); emit(Signal.Upstream.Error(e)) },
        )
    }

/**
 * On error, switches to the [Many] returned by [fallback], continuing from there.
 * On normal completion, [fallback] is not invoked.
 */
fun <T : Any> Many<T>.recover(fallback: (AelvException) -> Many<T>): Many<T> =
    Many.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        if (result is Either.Right) fallback(result.value).source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
        )
        else emit(Signal.Upstream.Complete)
    }

/**
 * On error, emits the single value returned by [fallback] and then completes.
 * On normal completion, [fallback] is not invoked.
 */
fun <T : Any> Many<T>.recoverWith(fallback: (AelvException) -> T): Many<T> =
    Many.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        when (result) {
            is Either.Left  -> emit(Signal.Upstream.Complete)
            is Either.Right -> {
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
                result is Either.Left                           -> break
                !policy.filter((result as Either.Right).value)  -> { emit(Signal.Upstream.Error(result.value)); return@generate }
                attempts >= policy.maxAttempts                  -> { log.operator.retryExhausted("retry", result.value); emit(Signal.Upstream.Error(result.value)); return@generate }
                else -> {
                    log.operator.retrying("retry", attempts, result.value)
                    val d = policy.backoff.delayFor(attempts)
                    if (d.isPositive()) delay(d)
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
            { e -> withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Error(e)) } },
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
                { e -> emit(Signal.Upstream.Error(e)) },
            )
        }
    }

/**
 * Pairs the values of [a] and [b], applying [transform] once both have emitted.
 * If either source completes without emitting, the result completes empty.
 */
fun <A : Any, B : Any, R : Any> zip(a: One<A>, b: One<B>, transform: (A, B) -> R): One<R> =
    One.generate { emit ->
        var valueA: Any = Unset
        val resultA = a.collect { v -> valueA = v; Signal.Downstream.Cancel }
        if (resultA.isRight()) { emit(Signal.Upstream.Error(resultA.rightOrNull()!!)); return@generate }
        if (valueA === Unset) { emit(Signal.Upstream.Complete); return@generate }
        var valueB: Any = Unset
        val resultB = b.collect { v -> valueB = v; Signal.Downstream.Cancel }
        if (resultB.isRight()) { emit(Signal.Upstream.Error(resultB.rightOrNull()!!)); return@generate }
        if (valueB === Unset) { emit(Signal.Upstream.Complete); return@generate }
        @Suppress("UNCHECKED_CAST")
        val r = transform(valueA as A, valueB as B)
        if (emit(Signal.Upstream.Next(r)) != Signal.Downstream.Cancel)
            emit(Signal.Upstream.Complete)
    }

/** Pairs the value of this [One] with [other], applying [transform] to produce the result. */
fun <A : Any, B : Any, R : Any> One<A>.zipWith(other: One<B>, transform: (A, B) -> R): One<R> =
    zip(this, other, transform)

/** Transforms the value of this [One] by applying [transform] to it. */
fun <T : Any, R : Any> One<T>.map(transform: (T) -> R): One<R> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(transform(value))) },
            { emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
        )
    }

/** Passes the value of this [One] to [transform] and subscribes to the resulting [One]. */
fun <T : Any, R : Any> One<T>.flatMap(transform: (T) -> One<R>): One<R> =
    One.generate { emit ->
        source(
            { value ->
                transform(value).source(
                    { inner -> emit(Signal.Upstream.Next(inner)) },
                    { emit(Signal.Upstream.Complete) },
                    { e -> emit(Signal.Upstream.Error(e)) },
                )
                Signal.Downstream.Cancel
            },
            { emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
        )
    }

/** Passes the value of this [One] to [transform] and subscribes to the resulting [Many]. */
fun <T : Any, R : Any> One<T>.flatMapMany(transform: (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
        source(
            { value ->
                transform(value).source(
                    { inner -> emit(Signal.Upstream.Next(inner)) },
                    { emit(Signal.Upstream.Complete) },
                    { e -> emit(Signal.Upstream.Error(e)) },
                )
                Signal.Downstream.Cancel
            },
            { emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
        )
    }

/** Passes the value of this [One] to [transform] and awaits the resulting [None]. */
fun <T : Any> One<T>.flatMapNone(transform: (T) -> None<T>): None<T> =
    None.generate {
        var error: AelvException? = null
        source(
            { value ->
                val innerResult = transform(value).await()
                if (innerResult is Either.Right) { error = innerResult.value; Signal.Downstream.Cancel }
                else Signal.Downstream.Request
            },
            { },
            { e -> error = e },
        )
        if (error != null) throw error!!
    }

/** On error, emits the value returned by [fallback] and completes normally. */
fun <T : Any> One<T>.recover(fallback: (AelvException) -> T): One<T> =
    One.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        if (result is Either.Right) {
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
                result is Either.Left                           -> break
                !policy.filter((result as Either.Right).value)  -> { emit(Signal.Upstream.Error(result.value)); return@generate }
                attempts >= policy.maxAttempts                  -> { log.operator.retryExhausted("retry", result.value); emit(Signal.Upstream.Error(result.value)); return@generate }
                else -> {
                    log.operator.retrying("retry", attempts, result.value)
                    val d = policy.backoff.delayFor(attempts)
                    if (d.isPositive()) delay(d)
                    attempts++
                }
            }
        }
        emit(Signal.Upstream.Complete)
    }

/** Invokes [action] for the emitted value without modifying the stream. */
fun <T : Any> One<T>.doOnNext(action: (T) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> action(value); emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { e -> emit(Signal.Upstream.Error(e)) },
        )
    }

/** Invokes [action] when the stream signals an error, without modifying the stream. */
fun <T : Any> One<T>.doOnError(action: (AelvException) -> Unit): One<T> =
    One.generate { emit ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { e -> action(e); emit(Signal.Upstream.Error(e)) },
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
            { e -> action(Signal.Upstream.Error(e)); emit(Signal.Upstream.Error(e)) },
        )
    }

/**
 * [One] variant of [publishOn].
 */
fun <T : Any> One<T>.publishOn(context: CoroutineContext): One<T> =
    One.generate { emit ->
        source(
            { value -> withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Next(value)) } },
            { withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Complete) } },
            { e -> withContext(currentCoroutineContext() + context) { emit(Signal.Upstream.Error(e)) } },
        )
    }

/**
 * [One] variant of [subscribeOn].
 */
fun <T : Any> One<T>.subscribeOn(context: CoroutineContext): One<T> =
    One.generate { emit ->
        withContext(currentCoroutineContext() + context) {
            source(
                { value -> emit(Signal.Upstream.Next(value)) },
                { emit(Signal.Upstream.Complete) },
                { e -> emit(Signal.Upstream.Error(e)) },
            )
        }
    }

/**
 * Suspends until this [One] emits its value or signals an error.
 *
 * Returns [Either.Left] containing the value on success, or [Either.Right] containing the
 * [AelvException] if the source errored or completed without emitting.
 */
suspend fun <T : Any> One<T>.get(): Either<T, AelvException> {
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
 * Returns a [One] that executes the upstream source at most once and replays the result to every
 * subscriber.  The first subscriber triggers execution; subsequent subscribers receive the cached
 * result immediately without re-executing the source.
 *
 * Thread-safe: a [Mutex] ensures only one subscriber runs the source even under concurrent
 * subscriptions.
 */
fun <T : Any> One<T>.cache(): One<T> {
    val mutex  = Mutex()
    var cached: Either<T, AelvException>? = null
    return One.generate { emit ->
        val result: Either<T, AelvException> = mutex.withLock {
            cached ?: run {
                val r = get()
                cached = r
                r
            }
        }
        when (result) {
            is Either.Left  -> {
                if (emit(Signal.Upstream.Next(result.value)) != Signal.Downstream.Cancel)
                    emit(Signal.Upstream.Complete)
            }
            is Either.Right -> emit(Signal.Upstream.Error(result.value))
        }
    }
}
