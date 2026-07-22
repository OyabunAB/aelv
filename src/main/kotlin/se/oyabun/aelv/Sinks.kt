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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.yield

/**
 * Policy applied when a [Sink] cannot deliver an emitted item because its internal
 * buffer is at capacity.
 *
 * - [Unbounded] — no limit; items accumulate indefinitely (default for [UnicastSink]).
 * - [DropNewest] — silently discard the arriving item; existing buffered items are preserved.
 * - [DropOldest] — evict the oldest buffered item to make room for the arriving one.
 * - [Error] — throw [IllegalStateException] (the pre-1.0.2 behaviour of multi-subscriber sinks).
 *
 * When an item is dropped due to any policy other than [Error], the [Sink.doOnOverflow]
 * callback fires with the dropped item so the producer can release resources
 */
sealed interface OverflowStrategy {
    data object Unbounded  : OverflowStrategy
    data object DropNewest : OverflowStrategy
    data object DropOldest : OverflowStrategy
    data object Error      : OverflowStrategy
}

/** Invoked when a subscriber connects to a [Sink]. */
typealias OnSubscribe                   = () -> Unit

/** Invoked when a subscriber disconnects from a [Sink] (cancel or stream termination). */
typealias OnCancel                      = () -> Unit

/** Invoked when a [Sink] completes or errors. */
typealias OnTerminate                   = () -> Unit

/** Invoked with the dropped item when a [Sink]'s [OverflowStrategy] discards an emission. */
typealias OnOverflow<T>                 = (T) -> Unit

/** Slow-path buffer for promoted subscribers. Grows freely up to [max], then signals overflow. */
private class BoundedQueue<T : Any>(val max: Int) {
    private val queue = ConcurrentLinkedQueue<T>()
    private val count = AtomicInteger(0)

    /** Returns false when the cap is reached. */
    fun add(item: T): Boolean {
        if (count.get() >= max) return false
        queue.add(item)
        count.incrementAndGet()
        return true
    }

    fun poll(): T? = queue.poll()?.also { count.decrementAndGet() }
    fun pollFirst(): T? = poll()
    fun isEmpty(): Boolean = queue.isEmpty()
    fun peek(): T? = queue.peek()
}

/**
 * Per-subscriber state attached to the ring buffer.
 *
 * [cursor] is written only by the subscriber coroutine and read by the emitter for
 * overflow detection. [@Volatile] provides the cross-thread visibility guarantee
 * without the AtomicLong CAS overhead — a single writer makes CAS unnecessary.
 */
private class SubHandle<T : Any>(writeStart: Long) {
    @Volatile var cursor:    Long             = writeStart
    val             wakeup:  Channel<Unit>    = Channel(Channel.CONFLATED)
    @Volatile var waiting:   Boolean          = false
    @Volatile var promoted:  Boolean          = false
    @Volatile var slowQueue: BoundedQueue<T>? = null
}

/**
 * Common interface for all aelv sink types.
 *
 * Returns [Self] from mutating operations to enable fluent chaining:
 * ```kotlin
 * Sinks.broadcast<Int>().emit(1, 2, 3).complete()
 * ```
 */
interface SinkOf<T : Any, Self : SinkOf<T, Self>> {
    fun emit(value: T): Self
    fun emit(vararg values: T): Self
    fun complete(): Self
    fun error(cause: Exception): Self
}

/**
 * A hot multicast push source backed by a shared ring buffer.
 *
 * ## Producer-side lifecycle hooks
 *
 * Register callbacks to observe subscriber lifecycle events from the producer side.
 * All hooks are optional and fire synchronously on the thread that triggers the event.
 *
 * - [onSubscribe] — a subscriber connected.
 * - [onCancel] — a subscriber disconnected (cancelled or stream terminated for that subscriber).
 * - [doOnOverflow] — an emitted item was dropped due to the configured [OverflowStrategy].
 * - [doOnTerminate] — the sink itself completed or errored.
 *
 * **Note**: `onRequest(n: Long)` — demand signalling from subscriber to producer — is not
 * yet available on queue-backed sinks. It requires explicit demand tracking in the generation
 * model and will be added in a future release.
 *
 * ## Thread safety
 *
 * [emit] is NOT safe for concurrent callers — the ring buffer is single-writer. Serialise
 * external callers if needed. [asMany] and subscriber lifecycle callbacks may be called
 * concurrently.
 */
sealed class Sink<T : Any>(
    private val historySize:    Int,
    private val bufferSize:     Int,
    private val maxSlowBuffer:  Int,
    private val overflowStrategy: OverflowStrategy = OverflowStrategy.Error,
) {
    private val log          = Logging.of<Sink<*>>()
    private val name         = this::class.simpleName ?: "Sink"
    private val buffer       = arrayOfNulls<Any>(bufferSize)
    @Volatile private var writePos = 0L

    private val terminal    = AtomicReference<Any>(Unset)
    private val histLock    = ReentrantLock()
    private val history     = ArrayDeque<T>()
    private val subscribers = CopyOnWriteArrayList<SubHandle<T>>()

    private data class Hooks<T : Any>(
        val onSubscribe:   OnSubscribe  = {},
        val onCancel:      OnCancel      = {},
        val doOnOverflow:  OnOverflow<T> = {},
        val doOnTerminate: OnTerminate   = {},
    )

    private val hooks = AtomicReference(Hooks<T>())

    /** Registers a callback invoked when a subscriber connects. */
    fun onSubscribe(callback: () -> Unit): Sink<T> =
        apply { hooks.updateAndGet { it.copy(onSubscribe = callback) } }

    /** Registers a callback invoked when a subscriber disconnects (cancel or stream end). */
    fun onCancel(callback: () -> Unit): Sink<T> =
        apply { hooks.updateAndGet { it.copy(onCancel = callback) } }

    /**
     * Registers a callback invoked with each item dropped due to [OverflowStrategy].
     * Not called when [OverflowStrategy.Error] is configured — that throws instead.
     */
    fun doOnOverflow(callback: (T) -> Unit): Sink<T> =
        apply { hooks.updateAndGet { it.copy(doOnOverflow = callback) } }

    /** Registers a callback invoked when this sink completes or errors. */
    fun doOnTerminate(callback: () -> Unit): Sink<T> =
        apply { hooks.updateAndGet { it.copy(doOnTerminate = callback) } }

    protected fun doEmit(value: T) {
        if (terminal.isSet()) return

        if (historySize > 0) histLock.withLock {
            history.addLast(value)
            if (historySize != Int.MAX_VALUE && history.size > historySize) history.removeFirst()
        }

        val pos = writePos
        for (sub in subscribers) {
            if (!sub.promoted && pos - sub.cursor >= bufferSize) promote(sub, pos)
        }

        buffer[pos.toInt() and (bufferSize - 1)] = value
        writePos = pos + 1

        for (sub in subscribers) {
            if (sub.promoted && !sub.slowQueue!!.add(value)) {
                handleSlowOverflow(sub, value)
            }
            if (sub.waiting) sub.wakeup.trySend(Unit)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun handleSlowOverflow(sub: SubHandle<T>, value: T) {
        when (overflowStrategy) {
            OverflowStrategy.Error     ->
                throw IllegalStateException("Sink subscriber buffer overflow — slow subscriber or maxSlowBuffer too small")
            OverflowStrategy.DropNewest -> {
                hooks.get().doOnOverflow(value)
            }
            OverflowStrategy.DropOldest -> {
                val dropped = sub.slowQueue!!.pollFirst()
                if (dropped != null) hooks.get().doOnOverflow(dropped)
                sub.slowQueue!!.add(value)
            }
            OverflowStrategy.Unbounded ->
                throw IllegalStateException("Sink subscriber buffer overflow — unreachable for Unbounded")
        }
    }

    private fun promote(sub: SubHandle<T>, currentPos: Long) {
        if (sub.promoted) return
        val q = BoundedQueue<T>(maxSlowBuffer)
        val cursor = sub.cursor
        for (i in cursor until currentPos) {
            @Suppress("UNCHECKED_CAST")
            q.add(buffer[(i.toInt() and (bufferSize - 1))] as T)
        }
        sub.slowQueue = q
        sub.promoted  = true
    }

    protected fun doTerminate(signal: Signal.Upstream<T>) {
        if (!terminal.compareAndSet(Unset, signal)) return
        when (signal) {
            is Signal.Upstream.Complete -> log.sink.completed(name)
            is Signal.Upstream.Error    -> log.sink.error(name, signal.cause)
            is Signal.Upstream.Next     -> Unit
        }
        hooks.get().doOnTerminate()
        for (sub in subscribers) if (sub.waiting) sub.wakeup.trySend(Unit)
    }

    fun tryEmit(value: T): Boolean {
        if (terminal.isSet()) return false
        if (historySize > 0) histLock.withLock {
            history.addLast(value)
            if (historySize != Int.MAX_VALUE && history.size > historySize) history.removeFirst()
        }
        val pos = writePos
        for (sub in subscribers) {
            if (!sub.promoted && pos - sub.cursor >= bufferSize) promote(sub, pos)
        }
        buffer[pos.toInt() and (bufferSize - 1)] = value
        writePos = pos + 1
        for (sub in subscribers) {
            if (sub.promoted && !sub.slowQueue!!.add(value)) return false
        }
        for (sub in subscribers) if (sub.waiting) sub.wakeup.trySend(Unit)
        return true
    }

    private fun register(): Pair<SubHandle<T>, List<T>> {
        val handle   = SubHandle<T>(writeStart = writePos)
        val snapshot = if (historySize > 0) histLock.withLock {
            subscribers.add(handle)
            history.toList()
        } else {
            subscribers.add(handle)
            emptyList()
        }
        log.sink.subscriberAttached(name)
        hooks.get().onSubscribe()
        return handle to snapshot
    }

    @Suppress("UNCHECKED_CAST")
    fun asMany(): Many<T> = Many.generate { generatorEmit ->
        val (handle, snapshot) = register()
        try {
            for (item in snapshot) {
                if (generatorEmit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@generate
            }
            while (true) {
                if (handle.promoted) {
                    var item = handle.slowQueue!!.poll()
                    while (item != null) {
                        if (generatorEmit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@generate
                        item = handle.slowQueue!!.poll()
                    }
                    if (terminal.isSet()) { generatorEmit(terminal.get() as Signal.Upstream<T>); return@generate }
                } else {
                    val endPos  = writePos
                    var drained = false
                    while (handle.cursor < endPos) {
                        val item = buffer[(handle.cursor.toInt() and (bufferSize - 1))] as T
                        handle.cursor++
                        if (generatorEmit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@generate
                        drained = true
                    }
                    if (!drained) {
                        if (terminal.isSet()) { generatorEmit(terminal.get() as Signal.Upstream<T>); return@generate }
                        var spins = 0
                        while (handle.cursor >= writePos && !terminal.isSet() && spins++ < 100) yield()
                        if (handle.cursor < writePos || terminal.isSet()) continue
                        handle.waiting = true
                        if (handle.cursor >= writePos) handle.wakeup.receive()
                        handle.waiting = false
                    }
                }
            }
        } finally {
            subscribers.remove(handle)
            log.sink.subscriberDetached(name)
            hooks.get().onCancel()
        }
    }

    fun asOne(): One<T> = asMany().first()
}

/** Emits only to subscribers present at the time of emission; no history. */
class BroadcastSink<T : Any>(
    bufferSize:    Int              = Aelv.bufferSize,
    maxSlowBuffer: Int              = Aelv.maxSlowBuffer,
    overflow:      OverflowStrategy = OverflowStrategy.Error,
) : Sink<T>(historySize = 0, bufferSize = bufferSize, maxSlowBuffer = maxSlowBuffer, overflowStrategy = overflow),
    SinkOf<T, BroadcastSink<T>> {

    override fun emit(value: T): BroadcastSink<T>          = apply { doEmit(value) }
    override fun emit(vararg values: T): BroadcastSink<T>  = apply { values.forEach { doEmit(it) } }
    override fun complete(): BroadcastSink<T>               = apply { doTerminate(Signal.Upstream.Complete) }
    override fun error(cause: Exception): BroadcastSink<T> = apply { doTerminate(Signal.Upstream.Error(cause)) }
}

/** Buffers the full emission history; late subscribers receive all past items then live items. */
class ReplaySink<T : Any>(
    bufferSize:    Int              = Aelv.bufferSize,
    maxSlowBuffer: Int              = Aelv.maxSlowBuffer,
    overflow:      OverflowStrategy = OverflowStrategy.Error,
) : Sink<T>(historySize = Int.MAX_VALUE, bufferSize = bufferSize, maxSlowBuffer = maxSlowBuffer, overflowStrategy = overflow),
    SinkOf<T, ReplaySink<T>> {

    override fun emit(value: T): ReplaySink<T>          = apply { doEmit(value) }
    override fun emit(vararg values: T): ReplaySink<T>  = apply { values.forEach { doEmit(it) } }
    override fun complete(): ReplaySink<T>               = apply { doTerminate(Signal.Upstream.Complete) }
    override fun error(cause: Exception): ReplaySink<T> = apply { doTerminate(Signal.Upstream.Error(cause)) }
}

/** Buffers the last [count] items; late subscribers receive recent history then live items. */
class ReplayLastSink<T : Any>(
    val count:     Int,
    bufferSize:    Int              = Aelv.bufferSize,
    maxSlowBuffer: Int              = Aelv.maxSlowBuffer,
    overflow:      OverflowStrategy = OverflowStrategy.Error,
) : Sink<T>(historySize = count, bufferSize = bufferSize, maxSlowBuffer = maxSlowBuffer, overflowStrategy = overflow),
    SinkOf<T, ReplayLastSink<T>> {

    init { require(count > 0) { "ReplayLastSink requires count > 0, got $count" } }

    override fun emit(value: T): ReplayLastSink<T>          = apply { doEmit(value) }
    override fun emit(vararg values: T): ReplayLastSink<T>  = apply { values.forEach { doEmit(it) } }
    override fun complete(): ReplayLastSink<T>               = apply { doTerminate(Signal.Upstream.Complete) }
    override fun error(cause: Exception): ReplayLastSink<T> = apply { doTerminate(Signal.Upstream.Error(cause)) }
}

object Sinks {
    fun <T : Any> broadcast(
        bufferSize:    Int              = Aelv.bufferSize,
        maxSlowBuffer: Int              = Aelv.maxSlowBuffer,
        overflow:      OverflowStrategy = OverflowStrategy.Error,
    ): BroadcastSink<T> = BroadcastSink(bufferSize, maxSlowBuffer, overflow)

    fun <T : Any> replay(
        bufferSize:    Int              = Aelv.bufferSize,
        maxSlowBuffer: Int              = Aelv.maxSlowBuffer,
        overflow:      OverflowStrategy = OverflowStrategy.Error,
    ): ReplaySink<T> = ReplaySink(bufferSize, maxSlowBuffer, overflow)

    fun <T : Any> replayLast(
        n:             Int,
        bufferSize:    Int              = Aelv.bufferSize,
        maxSlowBuffer: Int              = Aelv.maxSlowBuffer,
        overflow:      OverflowStrategy = OverflowStrategy.Error,
    ): ReplayLastSink<T> = ReplayLastSink(n, bufferSize, maxSlowBuffer, overflow)

    fun <T : Any> unicast(): UnicastSink<T> = UnicastSink()
}

/**
 * A unicast push source — exactly one subscriber is permitted for the lifetime of this sink.
 *
 * ## Producer-side lifecycle hooks
 *
 * - [onSubscribe] — subscriber connected.
 * - [onCancel] — subscriber disconnected (cancel or stream termination).
 * - [doOnOverflow] — item dropped due to [overflow] strategy; use to release ref-counted resources.
 * - [doOnTerminate] — the sink completed or errored.
 *
 * ## Overflow
 *
 * Pass an [OverflowStrategy] other than [OverflowStrategy.Unbounded] to bound the internal queue.
 * [OverflowStrategy.Unbounded] (default) uses an unbounded [ConcurrentLinkedQueue].
 * Any other strategy uses a bounded queue of size [maxBuffered].
 *
 * **Note**: `onRequest` is not yet available. It requires explicit demand tracking
 * in the generation model (tracked in issue #78).
 */
class UnicastSink<T : Any>(
    private val overflow:    OverflowStrategy = OverflowStrategy.Unbounded,
    private val maxBuffered: Int              = 256,
) : SinkOf<T, UnicastSink<T>> {

    private val queue      = ConcurrentLinkedQueue<T>()
    private val signal     = Channel<Unit>(Channel.CONFLATED)
    private val terminal   = AtomicReference<Signal.Upstream<T>>(null)
    private val subscribed = AtomicBoolean(false)

    private data class Hooks<T : Any>(
        val onSubscribe:   OnSubscribe  = {},
        val onCancel:      OnCancel      = {},
        val doOnOverflow:  OnOverflow<T> = {},
        val doOnTerminate: OnTerminate   = {},
    )

    private val hooks = AtomicReference(Hooks<T>())

    /** Registers a callback invoked when the subscriber connects. */
    fun onSubscribe(callback: () -> Unit): UnicastSink<T> =
        apply { hooks.updateAndGet { it.copy(onSubscribe = callback) } }

    /** Registers a callback invoked when the subscriber disconnects. */
    fun onCancel(callback: () -> Unit): UnicastSink<T> =
        apply { hooks.updateAndGet { it.copy(onCancel = callback) } }

    /**
     * Registers a callback invoked with each item dropped due to [overflow].
     * Not called when [OverflowStrategy.Error] is configured — that throws instead.
     */
    fun doOnOverflow(callback: (T) -> Unit): UnicastSink<T> =
        apply { hooks.updateAndGet { it.copy(doOnOverflow = callback) } }

    /** Registers a callback invoked when this sink completes or errors. */
    fun doOnTerminate(callback: () -> Unit): UnicastSink<T> =
        apply { hooks.updateAndGet { it.copy(doOnTerminate = callback) } }

    override fun emit(value: T): UnicastSink<T> {
        enqueue(value)
        signal.trySend(Unit)
        return this
    }

    override fun emit(vararg values: T): UnicastSink<T> {
        values.forEach { enqueue(it) }
        signal.trySend(Unit)
        return this
    }

    private fun enqueue(value: T) {
        if (overflow == OverflowStrategy.Unbounded) {
            queue.add(value)
            return
        }
        if (queue.size < maxBuffered) {
            queue.add(value)
            return
        }
        when (overflow) {
            OverflowStrategy.Error      ->
                throw IllegalStateException("UnicastSink buffer overflow (maxBuffered=$maxBuffered)")
            OverflowStrategy.DropNewest -> hooks.get().doOnOverflow(value)
            OverflowStrategy.DropOldest -> {
                val dropped = queue.poll()
                if (dropped != null) hooks.get().doOnOverflow(dropped)
                queue.add(value)
            }
            OverflowStrategy.Unbounded  -> queue.add(value)
        }
    }

    override fun complete(): UnicastSink<T> {
        terminal.compareAndSet(null, Signal.Upstream.Complete)
        hooks.get().doOnTerminate()
        signal.trySend(Unit)
        return this
    }

    override fun error(cause: Exception): UnicastSink<T> {
        terminal.compareAndSet(null, Signal.Upstream.Error(cause))
        hooks.get().doOnTerminate()
        signal.trySend(Unit)
        return this
    }

    fun asMany(): Many<T> = Many.generate { downstream ->
        if (!subscribed.compareAndSet(false, true)) {
            downstream(Signal.Upstream.Error(IllegalStateException("UnicastSink already has a subscriber")))
            return@generate
        }
        hooks.get().onSubscribe()
        try {
            while (true) {
                var item = queue.poll()
                while (item != null) {
                    if (downstream(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@generate
                    item = queue.poll()
                }
                when (val term = terminal.get()) {
                    null                        -> signal.receive()
                    is Signal.Upstream.Complete -> { downstream(Signal.Upstream.Complete); return@generate }
                    is Signal.Upstream.Error    -> { downstream(term); return@generate }
                    else                        -> return@generate
                }
            }
        } finally {
            hooks.get().onCancel()
        }
    }

    fun asOne(): One<T> = One.generate { downstream ->
        asMany().take(1).source(
            { value -> downstream(Signal.Upstream.Next(value)) },
            { downstream(Signal.Upstream.Complete) },
            { cause -> downstream(Signal.Upstream.Error(cause)) },
        )
    }
}
