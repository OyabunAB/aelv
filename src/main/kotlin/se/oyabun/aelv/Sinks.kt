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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.yield
import java.util.concurrent.atomic.AtomicBoolean

/** Slow-path buffer for promoted subscribers. Grows freely up to [max], then rejects. */
private class BoundedQueue<T : Any>(val max: Int) {
    private val queue = ConcurrentLinkedQueue<T>()
    private val count = AtomicInteger(0)

    /** Returns false if the cap is reached — caller must throw. */
    fun add(item: T): Boolean {
        if (count.get() >= max) return false
        queue.add(item)
        count.incrementAndGet()
        return true
    }

    fun poll(): T? = queue.poll()?.also { count.decrementAndGet() }
    fun isEmpty(): Boolean = queue.isEmpty()
}

/**
 * Per-subscriber state attached to the ring buffer.
 *
 * [cursor] is written only by the subscriber coroutine and read by the emitter for
 * overflow detection. [@Volatile] provides the cross-thread visibility guarantee
 * without the AtomicLong CAS overhead — a single writer makes CAS unnecessary.
 *
 * [promoted] and [slowQueue] are written under the Sink lock by [promote], and read
 * by the subscriber coroutine (without the lock). [@Volatile] ensures visibility.
 */
private class SubHandle<T : Any>(writeStart: Long) {
    @Volatile var cursor:    Long             = writeStart
    val             wakeup:  Channel<Unit>    = Channel(Channel.CONFLATED)
    @Volatile var waiting:   Boolean          = false   // true only while blocked in wakeup.receive()
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
 * Fast subscribers share the ring buffer via per-subscriber cursors — emission cost
 * is O(1) write + N lightweight wakeup taps regardless of subscriber count.
 * A subscriber that falls behind the ring buffer window is automatically promoted
 * to a dedicated [BoundedQueue], keeping it isolated from fast subscribers.
 *
 * Obtain instances via [Sinks.broadcast], [Sinks.replay], [Sinks.replayLast].
 *
 * **Thread safety**: [emit] is NOT safe for concurrent callers — the ring buffer
 * is single-writer. Concurrent calls to [emit] will corrupt the buffer. Serialise
 * external callers if needed. [asMany] and [asOne] may be called concurrently.
 */
sealed class Sink<T : Any>(
    private val historySize:    Int,
    private val bufferSize:     Int,
    private val maxSlowBuffer:  Int,
) {
    private val log          = Logging.of<Sink<*>>()
    private val name         = this::class.simpleName ?: "Sink"
    private val buffer       = arrayOfNulls<Any>(bufferSize)
    @Volatile private var writePos = 0L               // written by emitter, read by subscribers

    private val terminal    = AtomicReference<Any>(Unset)
    private val histLock    = ReentrantLock()          // guards history and subscriber registration
    private val history     = ArrayDeque<T>()          // raw T, guarded by histLock
    private val subscribers = CopyOnWriteArrayList<SubHandle<T>>()

    protected fun doEmit(value: T) {
        if (terminal.isSet()) return

        if (historySize > 0) {
            histLock.withLock {
                history.addLast(value)
                if (historySize != Int.MAX_VALUE && history.size > historySize) history.removeFirst()
            }
        }

        val pos = writePos

        for (sub in subscribers) {
            if (!sub.promoted && pos - sub.cursor >= bufferSize) promote(sub, pos)
        }

        buffer[pos.toInt() and (bufferSize - 1)] = value  // write before making visible
        writePos = pos + 1                                  // volatile write — subscribers see buffer[pos]

        for (sub in subscribers) {
            if (sub.promoted && !sub.slowQueue!!.add(value))
                throw IllegalStateException("Sink subscriber buffer overflow — slow subscriber or maxSlowBuffer too small")
            if (sub.waiting) sub.wakeup.trySend(Unit)
        }
    }

    /** Promote [sub] to its dedicated slow-path queue; backfill its unread ring slots. */
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
        for (sub in subscribers) if (sub.waiting) sub.wakeup.trySend(Unit)
    }

    /** Returns false if terminated or any slow subscriber's overflow queue is full. */
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
        // For replay sinks: writeStart is captured inside histLock so the history snapshot
        // and the ring-buffer cursor are consistent with each other. A one-item duplicate
        // can still occur if doEmit releases histLock (after adding to history) but has not
        // yet incremented writePos when register runs — moving writePos inside the lock would
        // close this window but serialises every emit with every subscribe.
        val handle = SubHandle<T>(writeStart = writePos)
        val snapshot = if (historySize > 0) histLock.withLock {
            subscribers.add(handle)
            history.toList()
        } else {
            subscribers.add(handle)
            emptyList()
        }
        log.sink.subscriberAttached(name)
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
                    val endPos = writePos              // snapshot: one volatile read per drain batch
                    var drained = false
                    while (handle.cursor < endPos) {
                        val item = buffer[(handle.cursor.toInt() and (bufferSize - 1))] as T
                        handle.cursor++
                        if (generatorEmit(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@generate
                        drained = true
                    }
                    if (terminal.isSet()) { generatorEmit(terminal.get() as Signal.Upstream<T>); return@generate }
                    if (!drained) {
                        var spins = 0
                        while (handle.cursor >= writePos && !terminal.isSet() && spins++ < 100) {
                            yield()
                        }
                        if (handle.cursor < writePos || terminal.isSet()) continue
                        handle.waiting = true
                        if (handle.cursor >= writePos) {
                            handle.wakeup.receive()
                        }
                        handle.waiting = false
                    }
                }
            }
        } finally {
            subscribers.remove(handle)
            log.sink.subscriberDetached(name)
        }
    }

    fun asOne(): One<T> = asMany().first()
}

/** Emits only to subscribers present at the time of emission; no history. */
class BroadcastSink<T : Any>(bufferSize: Int = Aelv.bufferSize, maxSlowBuffer: Int = Aelv.maxSlowBuffer) :
    Sink<T>(historySize = 0, bufferSize = bufferSize, maxSlowBuffer = maxSlowBuffer),
    SinkOf<T, BroadcastSink<T>> {

    override fun emit(value: T): BroadcastSink<T>          = apply { doEmit(value) }
    override fun emit(vararg values: T): BroadcastSink<T>  = apply { values.forEach { doEmit(it) } }
    override fun complete(): BroadcastSink<T>               = apply { doTerminate(Signal.Upstream.Complete) }
    override fun error(cause: Exception): BroadcastSink<T> = apply { doTerminate(Signal.Upstream.Error(cause)) }
}

/** Buffers the full emission history; late subscribers receive all past items then live items. */
class ReplaySink<T : Any>(bufferSize: Int = Aelv.bufferSize, maxSlowBuffer: Int = Aelv.maxSlowBuffer) :
    Sink<T>(historySize = Int.MAX_VALUE, bufferSize = bufferSize, maxSlowBuffer = maxSlowBuffer),
    SinkOf<T, ReplaySink<T>> {

    override fun emit(value: T): ReplaySink<T>          = apply { doEmit(value) }
    override fun emit(vararg values: T): ReplaySink<T>  = apply { values.forEach { doEmit(it) } }
    override fun complete(): ReplaySink<T>               = apply { doTerminate(Signal.Upstream.Complete) }
    override fun error(cause: Exception): ReplaySink<T> = apply { doTerminate(Signal.Upstream.Error(cause)) }
}

/** Buffers the last [n] items; late subscribers receive recent history then live items. */
class ReplayLastSink<T : Any>(val count: Int, bufferSize: Int = Aelv.bufferSize, maxSlowBuffer: Int = Aelv.maxSlowBuffer) :
    Sink<T>(historySize = count, bufferSize = bufferSize, maxSlowBuffer = maxSlowBuffer),
    SinkOf<T, ReplayLastSink<T>> {

    init { require(count > 0) { "ReplayLastSink requires count > 0, got $count" } }

    override fun emit(value: T): ReplayLastSink<T>          = apply { doEmit(value) }
    override fun emit(vararg values: T): ReplayLastSink<T>  = apply { values.forEach { doEmit(it) } }
    override fun complete(): ReplayLastSink<T>               = apply { doTerminate(Signal.Upstream.Complete) }
    override fun error(cause: Exception): ReplayLastSink<T> = apply { doTerminate(Signal.Upstream.Error(cause)) }
}

object Sinks {
    fun <T : Any> broadcast(bufferSize: Int = Aelv.bufferSize, maxSlowBuffer: Int = Aelv.maxSlowBuffer): BroadcastSink<T> =
        BroadcastSink(bufferSize, maxSlowBuffer)

    fun <T : Any> replay(bufferSize: Int = Aelv.bufferSize, maxSlowBuffer: Int = Aelv.maxSlowBuffer): ReplaySink<T> =
        ReplaySink(bufferSize, maxSlowBuffer)

    fun <T : Any> replayLast(n: Int, bufferSize: Int = Aelv.bufferSize, maxSlowBuffer: Int = Aelv.maxSlowBuffer): ReplayLastSink<T> =
        ReplayLastSink(n, bufferSize, maxSlowBuffer)

    fun <T : Any> unicast(): UnicastSink<T> = UnicastSink()
}

/**
 * A unicast push source — exactly one subscriber is permitted for the lifetime of this sink.
 */
class UnicastSink<T : Any> : SinkOf<T, UnicastSink<T>> {

    private val queue      = ConcurrentLinkedQueue<T>()
    private val signal     = Channel<Unit>(Channel.CONFLATED)
    private val terminal   = AtomicReference<Signal.Upstream<T>>(null)
    private val subscribed = AtomicBoolean(false)

    override fun emit(value: T): UnicastSink<T> {
        queue.add(value)
        signal.trySend(Unit)
        return this
    }

    override fun emit(vararg values: T): UnicastSink<T> {
        values.forEach { queue.add(it) }
        signal.trySend(Unit)
        return this
    }

    override fun complete(): UnicastSink<T> {
        terminal.compareAndSet(null, Signal.Upstream.Complete)
        signal.trySend(Unit)
        return this
    }

    override fun error(cause: Exception): UnicastSink<T> {
        terminal.compareAndSet(null, Signal.Upstream.Error(cause))
        signal.trySend(Unit)
        return this
    }

    fun asMany(): Many<T> = Many.generate { downstream ->
        if (!subscribed.compareAndSet(false, true)) {
            downstream(Signal.Upstream.Error(IllegalStateException("UnicastSink already has a subscriber")))
            return@generate
        }
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
    }

    fun asOne(): One<T> = One.generate { downstream ->
        asMany().take(1).source(
            { value -> downstream(Signal.Upstream.Next(value)) },
            { downstream(Signal.Upstream.Complete) },
            { cause -> downstream(Signal.Upstream.Error(cause)) },
        )
    }
}
