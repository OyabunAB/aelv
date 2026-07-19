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

import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private const val DEFAULT_BUFFER = 256

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
 * A hot multicast push source.
 *
 * Obtain instances via [Sinks.broadcast], [Sinks.replay], [Sinks.replayLast].
 */
sealed class Sink<T : Any>(
    private val historySize: Int,
    private val bufferSize: Int,
) {
    private val lock        = ReentrantLock()
    private val terminal    = AtomicReference<Any>(Unset)
    private val history     = ArrayDeque<Signal.Upstream.Next<T>>()
    private val subscribers = CopyOnWriteArrayList<Channel<Signal.Upstream<T>>>()

    protected fun doEmit(value: T) {
        if (terminal.get().notUnset()) return
        val signal = Signal.Upstream.Next(value)
        if (historySize > 0) {
            lock.withLock {
                history.addLast(signal)
                if (historySize != Int.MAX_VALUE && history.size > historySize) history.removeFirst()
            }
        }
        for (inbox in subscribers) {
            val result = inbox.trySend(signal)
            if (result.isFailure && !result.isClosed)
                throw IllegalStateException("Sink subscriber buffer overflow — slow subscriber or bufferSize too small")
        }
    }

    protected fun doTerminate(signal: Signal.Upstream<T>) {
        if (!terminal.compareAndSet(Unset, signal)) return
        for (inbox in subscribers) {
            inbox.trySend(signal)
            inbox.close()
        }
    }

    /** Returns false if the sink is terminated. */
    fun tryEmit(value: T): Boolean {
        if (terminal.get().notUnset()) return false
        doEmit(value)
        return true
    }

    private fun registerWithHistory(inbox: Channel<Signal.Upstream<T>>): Pair<AutoCloseable, List<Signal.Upstream.Next<T>>> =
        lock.withLock {
            subscribers.add(inbox)
            val snapshot = if (historySize > 0) history.toList() else emptyList()
            AutoCloseable { lock.withLock { subscribers.remove(inbox) } } to snapshot
        }

    fun asMany(): Many<T> = Many.generate { emit ->
        val inbox = Channel<Signal.Upstream<T>>(bufferSize)
        val (handle, replay) = registerWithHistory(inbox)
        handle.using {
            for (item in replay) {
                if (emit(item) == Signal.Downstream.Cancel) return@using
            }
            val terminalState = terminal.get()
            if (terminalState.notUnset()) {
                when (terminalState) {
                    is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                    is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(terminalState.cause))
                    else                        -> {}
                }
                return@using
            }
            for (signal in inbox) {
                if (emit(signal) == Signal.Downstream.Cancel) break
            }
        }
    }

    fun asOne(): One<T> = One.generate { emit ->
        val inbox = Channel<Signal.Upstream<T>>(bufferSize)
        val (handle, history) = registerWithHistory(inbox)
        handle.using {
            for (item in history) {
                emit(item)
                emit(Signal.Upstream.Complete)
                return@using
            }
            val terminalState = terminal.get()
            if (terminalState.notUnset()) {
                when (terminalState) {
                    is Signal.Upstream.Complete -> emit(Signal.Upstream.Complete)
                    is Signal.Upstream.Error    -> emit(Signal.Upstream.Error(terminalState.cause))
                    else                        -> {}
                }
                return@using
            }
            for (signal in inbox) {
                when {
                    signal is Signal.Upstream.Next -> { emit(signal); emit(Signal.Upstream.Complete); return@using }
                    else -> { emit(signal); return@using }
                }
            }
        }
    }
}

/** Emits only to subscribers present at the time of emission; no history. */
class BroadcastSink<T : Any>(bufferSize: Int = DEFAULT_BUFFER) :
    Sink<T>(historySize = 0, bufferSize = bufferSize),
    SinkOf<T, BroadcastSink<T>> {

    override fun emit(value: T): BroadcastSink<T>          = apply { doEmit(value) }
    override fun emit(vararg values: T): BroadcastSink<T>  = apply { values.forEach { doEmit(it) } }
    override fun complete(): BroadcastSink<T>               = apply { doTerminate(Signal.Upstream.Complete) }
    override fun error(cause: Exception): BroadcastSink<T> = apply { doTerminate(Signal.Upstream.Error(cause)) }
}

/** Buffers the full emission history; late subscribers receive all past items then live items. */
class ReplaySink<T : Any>(bufferSize: Int = DEFAULT_BUFFER) :
    Sink<T>(historySize = Int.MAX_VALUE, bufferSize = bufferSize),
    SinkOf<T, ReplaySink<T>> {

    override fun emit(value: T): ReplaySink<T>          = apply { doEmit(value) }
    override fun emit(vararg values: T): ReplaySink<T>  = apply { values.forEach { doEmit(it) } }
    override fun complete(): ReplaySink<T>               = apply { doTerminate(Signal.Upstream.Complete) }
    override fun error(cause: Exception): ReplaySink<T> = apply { doTerminate(Signal.Upstream.Error(cause)) }
}

/** Buffers the last [n] items; late subscribers receive recent history then live items. */
class ReplayLastSink<T : Any>(val count: Int, bufferSize: Int = DEFAULT_BUFFER) :
    Sink<T>(historySize = count, bufferSize = bufferSize),
    SinkOf<T, ReplayLastSink<T>> {

    init { require(count > 0) { "ReplayLastSink requires count > 0, got $count" } }

    override fun emit(value: T): ReplayLastSink<T>          = apply { doEmit(value) }
    override fun emit(vararg values: T): ReplayLastSink<T>  = apply { values.forEach { doEmit(it) } }
    override fun complete(): ReplayLastSink<T>               = apply { doTerminate(Signal.Upstream.Complete) }
    override fun error(cause: Exception): ReplayLastSink<T> = apply { doTerminate(Signal.Upstream.Error(cause)) }
}

object Sinks {
    fun <T : Any> broadcast(bufferSize: Int = DEFAULT_BUFFER): BroadcastSink<T> =
        BroadcastSink(bufferSize)

    fun <T : Any> replay(bufferSize: Int = DEFAULT_BUFFER): ReplaySink<T> =
        ReplaySink(bufferSize)

    fun <T : Any> replayLast(n: Int, bufferSize: Int = DEFAULT_BUFFER): ReplayLastSink<T> =
        ReplayLastSink(n, bufferSize)

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
