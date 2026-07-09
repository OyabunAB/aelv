package se.oyabun.aelv

import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.atomic.AtomicInteger

fun <T : Any, R : Any> Many<T>.map(transform: (T) -> R): Many<R> =
    Many.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> emit(Signal.Upstream.Next(transform(signal.value)))
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any> Many<T>.filter(predicate: (T) -> Boolean): Many<T> =
    Many.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> if (predicate(signal.value)) emit(signal) else Signal.Downstream.Request(1)
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any> Many<T>.take(n: Long): Many<T> {
    require(n >= 0) { "take count must be non-negative, got $n" }
    return Many.generate { emit ->
        if (n == 0L) { emit(Signal.Upstream.Complete); return@generate }
        var remaining = n
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> when {
                    remaining == 0L -> Signal.Downstream.Cancel  // Complete already sent; just stop
                    else -> {
                        val downstream = emit(signal)
                        remaining--
                        if (remaining == 0L) {
                            if (downstream != Signal.Downstream.Cancel) emit(Signal.Upstream.Complete)
                            Signal.Downstream.Cancel
                        } else downstream
                    }
                }
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }
}

fun <T : Any> Many<T>.takeWhile(predicate: (T) -> Boolean): Many<T> =
    Many.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> if (predicate(signal.value)) {
                    emit(signal)
                } else {
                    emit(Signal.Upstream.Complete)
                    Signal.Downstream.Cancel
                }
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any> Many<T>.skip(n: Long): Many<T> {
    require(n >= 0) { "skip count must be non-negative, got $n" }
    return Many.generate { emit ->
        var skipped = 0L
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> if (skipped < n) { skipped++; Signal.Downstream.Request(1) } else emit(signal)
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }
}

fun <T : Any> Many<T>.skipWhile(predicate: (T) -> Boolean): Many<T> =
    Many.generate { emit ->
        var skipping = true
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> if (skipping && predicate(signal.value)) {
                    Signal.Downstream.Request(1)
                } else {
                    skipping = false
                    emit(signal)
                }
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any> Many<T>.distinct(): Many<T> =
    Many.generate { emit ->
        val seen = HashSet<T>()
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> if (seen.add(signal.value)) emit(signal) else Signal.Downstream.Request(1)
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any> Many<T>.distinctUntilChanged(): Many<T> =
    Many.generate { emit ->
        var last: Any = Unset
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> if (signal.value != last) {
                    last = signal.value
                    emit(signal)
                } else Signal.Downstream.Request(1)
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any, K : Any> Many<T>.distinctUntilChangedBy(key: (T) -> K): Many<T> =
    Many.generate { emit ->
        var lastKey: Any = Unset
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> {
                    val k = key(signal.value)
                    if (k != lastKey) { lastKey = k; emit(signal) } else Signal.Downstream.Request(1)
                }
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any, R : Any> Many<T>.flatMap(
    concurrency: Int = Int.MAX_VALUE,
    transform: (T) -> Many<R>,
): Many<R> = Many.generate { emit ->
    val semaphore = if (concurrency < Int.MAX_VALUE) Semaphore(concurrency) else null
    // Channel serialises all inner emissions through a single collector — RS 1.3.
    val channel = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
    coroutineScope {
        val producerJob = launch {
            val innerJobs = mutableListOf<Job>()
            var outerError: Signal.Upstream.Error? = null
            source { signal ->
                when (signal) {
                    is Signal.Upstream.Next -> {
                        semaphore?.acquire()
                        innerJobs.add(launch {
                            try {
                                transform(signal.value).source { inner ->
                                    when (inner) {
                                        is Signal.Upstream.Next     -> { channel.send(inner); Signal.Downstream.Request(1) }
                                        is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                                        is Signal.Upstream.Error    -> { channel.send(inner); Signal.Downstream.Cancel }
                                    }
                                }
                            } finally {
                                semaphore?.release()
                            }
                        })
                        Signal.Downstream.Request(1)
                    }
                    is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                    is Signal.Upstream.Error    -> { outerError = signal; Signal.Downstream.Cancel }
                }
            }
            innerJobs.forEach { it.join() }
            channel.send(outerError ?: Signal.Upstream.Complete)
            channel.close()
        }
        for (signal in channel) {
            when (signal) {
                is Signal.Upstream.Next -> {
                    if (emit(signal) == Signal.Downstream.Cancel) {
                        producerJob.cancel()
                        break
                    }
                }
                is Signal.Upstream.Complete -> { emit(Signal.Upstream.Complete); break }
                is Signal.Upstream.Error    -> { producerJob.cancel(); emit(Signal.Upstream.Error(signal.cause)); break }
            }
        }
    }
}

fun <T : Any, R : Any> Many<T>.concatMap(transform: (T) -> Many<R>): Many<R> =
    flatMap(concurrency = 1, transform = transform)

fun <T : Any, R : Any> Many<T>.switchMap(transform: (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
        val channel = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
        coroutineScope {
            val producerJob = launch {
                var activeJob: Job? = null
                source { signal ->
                    when (signal) {
                        is Signal.Upstream.Next -> {
                            activeJob?.cancelAndJoin()
                            activeJob = launch {
                                transform(signal.value).source { inner ->
                                    when (inner) {
                                        // Inner Complete is not forwarded — only the outer Complete terminates the stream.
                                        is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                                        else                        -> { channel.send(inner); Signal.Downstream.Request(1) }
                                    }
                                }
                            }
                            Signal.Downstream.Request(1)
                        }
                        is Signal.Upstream.Complete -> {
                            activeJob?.join()
                            channel.send(Signal.Upstream.Complete)
                            Signal.Downstream.Cancel
                        }
                        is Signal.Upstream.Error -> {
                            activeJob?.cancelAndJoin()
                            channel.send(Signal.Upstream.Error(signal.cause))
                            Signal.Downstream.Cancel
                        }
                    }
                }
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

fun <T : Any> Many<T>.mergeWith(other: Many<T>): Many<T> = merge(this, other)

fun <T : Any> merge(vararg sources: Many<T>): Many<T> =
    Many.generate { emit ->
        if (sources.isEmpty()) { emit(Signal.Upstream.Complete); return@generate }
        // Channel carries only Next and Error — never Complete.
        // Completion is tracked via an atomic counter so sources never block on a drained channel.
        val channel = Channel<Signal.Upstream<T>>(Channel.BUFFERED)
        val remaining = AtomicInteger(sources.size)
        coroutineScope {
            val jobs = sources.map { source ->
                launch {
                    source.source { signal ->
                        when (signal) {
                            is Signal.Upstream.Next     -> { channel.send(signal); Signal.Downstream.Request(1) }
                            is Signal.Upstream.Complete -> {
                                if (remaining.decrementAndGet() == 0) channel.close()
                                Signal.Downstream.Cancel
                            }
                            is Signal.Upstream.Error    -> { channel.send(signal); Signal.Downstream.Cancel }
                        }
                    }
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

fun <T : Any> concat(vararg sources: Many<T>): Many<T> =
    Many.generate { emit ->
        for (source in sources) {
            var cancelled = false
            var errored = false
            source.source { signal ->
                when (signal) {
                    is Signal.Upstream.Next -> {
                        if (emit(signal) == Signal.Downstream.Cancel) { cancelled = true; Signal.Downstream.Cancel }
                        else Signal.Downstream.Request(1)
                    }
                    is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                    is Signal.Upstream.Error    -> { emit(Signal.Upstream.Error(signal.cause)); errored = true; Signal.Downstream.Cancel }
                }
            }
            if (cancelled || errored) return@generate
        }
        emit(Signal.Upstream.Complete)
    }

fun <A : Any, B : Any, R : Any> zip(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
    Many.generate { emit ->
        // Errors are propagated by closing each channel with a cause — RS 1.3.
        val channelA = Channel<A>(Channel.BUFFERED)
        val channelB = Channel<B>(Channel.BUFFERED)
        coroutineScope {
            val jobA = launch {
                a.source { signal ->
                    when (signal) {
                        is Signal.Upstream.Next     -> { channelA.send(signal.value); Signal.Downstream.Request(1) }
                        is Signal.Upstream.Complete -> { channelA.close(); Signal.Downstream.Cancel }
                        is Signal.Upstream.Error    -> { channelA.close(signal.cause); Signal.Downstream.Cancel }
                    }
                }
            }
            val jobB = launch {
                b.source { signal ->
                    when (signal) {
                        is Signal.Upstream.Next     -> { channelB.send(signal.value); Signal.Downstream.Request(1) }
                        is Signal.Upstream.Complete -> { channelB.close(); Signal.Downstream.Cancel }
                        is Signal.Upstream.Error    -> { channelB.close(signal.cause); Signal.Downstream.Cancel }
                    }
                }
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

@Suppress("UNCHECKED_CAST")
fun <A : Any, B : Any, R : Any> combineLatest(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
    Many.generate { emit ->
        // Channel carries tagged values (Left = from a, Right = from b) plus errors.
        // All producer signals pass through the channel — single collector, serial emit — RS 1.3.
        val channel = Channel<Signal.Upstream<Either<A, B>>>(Channel.BUFFERED)
        coroutineScope {
            val jobA = launch {
                a.source { signal ->
                    when (signal) {
                        is Signal.Upstream.Next     -> { channel.send(Signal.Upstream.Next(signal.value.left())); Signal.Downstream.Request(1) }
                        is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                        is Signal.Upstream.Error    -> { channel.send(Signal.Upstream.Error(signal.cause)); Signal.Downstream.Cancel }
                    }
                }
            }
            val jobB = launch {
                b.source { signal ->
                    when (signal) {
                        is Signal.Upstream.Next     -> { channel.send(Signal.Upstream.Next(signal.value.right())); Signal.Downstream.Request(1) }
                        is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                        is Signal.Upstream.Error    -> { channel.send(Signal.Upstream.Error(signal.cause)); Signal.Downstream.Cancel }
                    }
                }
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

fun <T : Any> Many<T>.buffer(size: Int): Many<List<T>> {
    require(size > 0) { "buffer size must be positive, got $size" }
    return Many.generate { emit ->
        val bucket = mutableListOf<T>()
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> {
                    bucket.add(signal.value)
                    if (bucket.size == size) {
                        val downstream = emit(Signal.Upstream.Next(bucket.toList()))
                        bucket.clear()
                        downstream
                    } else Signal.Downstream.Request(1)
                }
                is Signal.Upstream.Complete -> {
                    if (bucket.isNotEmpty()) {
                        if (emit(Signal.Upstream.Next(bucket.toList())) == Signal.Downstream.Cancel)
                            return@source Signal.Downstream.Cancel
                    }
                    emit(Signal.Upstream.Complete)
                }
                is Signal.Upstream.Error -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }
}

fun <T : Any> Many<T>.buffer(size: Int, skip: Int): Many<List<T>> {
    require(size > 0) { "buffer size must be positive, got $size" }
    require(skip > 0) { "buffer skip must be positive, got $skip" }
    return Many.generate { emit ->
        val buffers = ArrayDeque<MutableList<T>>()
        var index = 0L
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> {
                    if (index % skip == 0L) buffers.addLast(mutableListOf())
                    buffers.forEach { it.add(signal.value) }
                    val full = buffers.firstOrNull { it.size == size }
                    index++
                    if (full != null) {
                        val downstream = emit(Signal.Upstream.Next(full.toList()))
                        buffers.removeFirst()
                        downstream
                    } else Signal.Downstream.Request(1)
                }
                is Signal.Upstream.Complete -> {
                    // Emit partial trailing buffers, consistent with buffer(size).
                    for (partial in buffers) {
                        if (partial.isNotEmpty()) {
                            if (emit(Signal.Upstream.Next(partial.toList())) == Signal.Downstream.Cancel)
                                return@source Signal.Downstream.Cancel
                        }
                    }
                    emit(Signal.Upstream.Complete)
                }
                is Signal.Upstream.Error -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }
}

fun <T : Any, K : Any, R : Any> Many<T>.groupBy(
    keySelector: (T) -> K,
    groupHandler: (key: K, group: Many<T>) -> Many<R>,
): Many<R> = Many.generate { emit ->
    // One RENDEZVOUS channel per group key.  Each group's channel is drained by a dedicated
    // coroutine that runs groupHandler and forwards results to the shared output channel.
    // The source sends directly — back-pressure propagates naturally.  Because the operator
    // owns all group subscriptions, the caller cannot accidentally leave a group unsubscribed.
    val groupChannels = mutableMapOf<K, Channel<Signal.Upstream<T>>>()
    val out = Channel<Signal.Upstream<R>>(Channel.BUFFERED)
    val remaining = AtomicInteger(0)
    coroutineScope {
        val producerJob = launch {
            source { signal ->
                when (signal) {
                    is Signal.Upstream.Next -> {
                        val key = keySelector(signal.value)
                        val ch = groupChannels.getOrPut(key) {
                            val newCh = Channel<Signal.Upstream<T>>(Channel.RENDEZVOUS)
                            remaining.incrementAndGet()
                            launch {
                                val groupMany = Many.generate<T> { groupEmit ->
                                    for (upstream in newCh) {
                                        if (groupEmit(upstream) == Signal.Downstream.Cancel) {
                                            newCh.cancel(); break
                                        }
                                    }
                                }
                                groupHandler(key, groupMany).source { inner ->
                                    when (inner) {
                                        is Signal.Upstream.Next     -> { out.send(inner); Signal.Downstream.Request(1) }
                                        is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                                        is Signal.Upstream.Error    -> { out.send(inner); Signal.Downstream.Cancel }
                                    }
                                }
                                if (remaining.decrementAndGet() == 0) out.close()
                            }
                            newCh
                        }
                        ch.send(signal)
                        Signal.Downstream.Request(1)
                    }
                    is Signal.Upstream.Complete -> {
                        for ((_, ch) in groupChannels) runCatching { ch.send(Signal.Upstream.Complete) }
                        if (remaining.get() == 0) out.close()
                        Signal.Downstream.Cancel
                    }
                    is Signal.Upstream.Error -> {
                        // Broadcast error to all group pipelines — they handle it via groupHandler
                        // (recover, doOnError, etc.) and propagate to out if unhandled.
                        // Do NOT send directly to out: that bypasses the group handlers.
                        for ((_, ch) in groupChannels) runCatching { ch.send(signal) }
                        if (remaining.get() == 0) out.close()
                        Signal.Downstream.Cancel
                    }
                }
            }
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

fun <T : Any> Many<T>.doOnNext(action: (T) -> Unit): Many<T> =
    Many.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> { action(signal.value); emit(signal) }
                else                    -> emit(signal)
            }
        }
    }

fun <T : Any> Many<T>.doOnComplete(action: () -> Unit): Many<T> =
    Many.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Complete -> { action(); emit(Signal.Upstream.Complete) }
                else                        -> emit(signal)
            }
        }
    }

fun <T : Any> Many<T>.doOnError(action: (AelvException) -> Unit): Many<T> =
    Many.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Error -> { action(signal.cause); emit(signal) }
                else                     -> emit(signal)
            }
        }
    }

fun <T : Any> Many<T>.doOnSubscribe(action: () -> Unit): Many<T> =
    Many.generate { emit -> action(); source { emit(it) } }

fun <T : Any> Many<T>.doFinally(action: (Signal.Terminal) -> Unit): Many<T> =
    Many.generate { emit ->
        source { signal ->
            val downstream = emit(signal)
            when (signal) {
                is Signal.Upstream.Complete -> action(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> action(Signal.Upstream.Error(signal.cause))
                is Signal.Upstream.Next     -> if (downstream == Signal.Downstream.Cancel) action(Signal.Downstream.Cancel)
            }
            downstream
        }
    }

fun <T : Any> Many<T>.recover(fallback: (AelvException) -> Many<T>): Many<T> =
    Many.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        if (result is Either.Right) fallback(result.value).source { emit(it) }
        else emit(Signal.Upstream.Complete)
    }

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

fun <T : Any> Many<T>.retry(times: Long = Long.MAX_VALUE): Many<T> =
    retry(Policy.retry().maxAttempts(times))

fun <T : Any> Many<T>.retry(policy: Policy.Retry): Many<T> =
    Many.generate { emit ->
        var attempts = 0L
        while (true) {
            val result = collect { emit(Signal.Upstream.Next(it)) }
            when {
                result is Either.Left                           -> break
                !policy.filter((result as Either.Right).value)  -> { emit(Signal.Upstream.Error(result.value)); return@generate }
                attempts >= policy.maxAttempts                  -> { emit(Signal.Upstream.Error(result.value)); return@generate }
                else -> {
                    val d = policy.backoff.delayFor(attempts)
                    if (d.isPositive()) delay(d)
                    attempts++
                }
            }
        }
        emit(Signal.Upstream.Complete)
    }

fun <T : Any, R : Any> One<T>.map(transform: (T) -> R): One<R> =
    One.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> emit(Signal.Upstream.Next(transform(signal.value)))
                is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any, R : Any> One<T>.flatMap(transform: (T) -> One<R>): One<R> =
    One.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> { transform(signal.value).source { emit(it) }; Signal.Downstream.Cancel }
                is Signal.Upstream.Complete -> { emit(Signal.Upstream.Complete); Signal.Downstream.Cancel }
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any, R : Any> One<T>.flatMapMany(transform: (T) -> Many<R>): Many<R> =
    Many.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next     -> { transform(signal.value).source { emit(it) }; Signal.Downstream.Cancel }
                is Signal.Upstream.Complete -> { emit(Signal.Upstream.Complete); Signal.Downstream.Cancel }
                is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(signal.cause))
            }
        }
    }

fun <T : Any> One<T>.flatMapNone(transform: (T) -> None<T>): None<T> =
    None.generate {
        var error: AelvException? = null
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> {
                    val innerResult = transform(signal.value).await()
                    if (innerResult is Either.Right) { error = innerResult.value; Signal.Downstream.Cancel }
                    else Signal.Downstream.Request(1)
                }
                is Signal.Upstream.Complete -> Signal.Downstream.Cancel
                is Signal.Upstream.Error    -> { error = signal.cause; Signal.Downstream.Cancel }
            }
        }
        if (error != null) throw error!!
    }

fun <T : Any> One<T>.recover(fallback: (AelvException) -> T): One<T> =
    One.generate { emit ->
        val result = collect { emit(Signal.Upstream.Next(it)) }
        if (result is Either.Right) {
            if (emit(Signal.Upstream.Next(fallback(result.value))) == Signal.Downstream.Cancel) return@generate
        }
        emit(Signal.Upstream.Complete)
    }

fun <T : Any> One<T>.retry(times: Long = Long.MAX_VALUE): One<T> =
    retry(Policy.retry().maxAttempts(times))

fun <T : Any> One<T>.retry(policy: Policy.Retry): One<T> =
    One.generate { emit ->
        var attempts = 0L
        while (true) {
            val result = collect { emit(Signal.Upstream.Next(it)) }
            when {
                result is Either.Left                           -> break
                !policy.filter((result as Either.Right).value)  -> { emit(Signal.Upstream.Error(result.value)); return@generate }
                attempts >= policy.maxAttempts                  -> { emit(Signal.Upstream.Error(result.value)); return@generate }
                else -> {
                    val d = policy.backoff.delayFor(attempts)
                    if (d.isPositive()) delay(d)
                    attempts++
                }
            }
        }
        emit(Signal.Upstream.Complete)
    }

fun <T : Any> One<T>.doOnNext(action: (T) -> Unit): One<T> =
    One.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Next -> { action(signal.value); emit(signal) }
                else                    -> emit(signal)
            }
        }
    }

fun <T : Any> One<T>.doOnError(action: (AelvException) -> Unit): One<T> =
    One.generate { emit ->
        source { signal ->
            when (signal) {
                is Signal.Upstream.Error -> { action(signal.cause); emit(signal) }
                else                     -> emit(signal)
            }
        }
    }

fun <T : Any> One<T>.doFinally(action: (Signal.Terminal) -> Unit): One<T> =
    One.generate { emit ->
        source { signal ->
            val downstream = emit(signal)
            when (signal) {
                is Signal.Upstream.Complete -> action(Signal.Upstream.Complete)
                is Signal.Upstream.Error    -> action(Signal.Upstream.Error(signal.cause))
                is Signal.Upstream.Next     -> if (downstream == Signal.Downstream.Cancel) action(Signal.Downstream.Cancel)
            }
            downstream
        }
    }

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
