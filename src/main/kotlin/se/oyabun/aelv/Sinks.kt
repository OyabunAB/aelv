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
 * A hot multicast push source.
 *
 * Push items with [emit] (suspend — backs off when the slowest subscriber's channel is full)
 * or [tryEmit] (non-suspend — returns false if any subscriber channel is full).
 *
 * Signal terminal state with [complete] or [error].
 * Obtain subscribable views via [asMany] or [asOne].
 *
 * Obtain instances via:
 * - [Sinks.broadcast]
 * - [Sinks.replay]
 * - [Sinks.replayLast]
 */
sealed class Sink<T : Any>(
    private val historySize: Int,
    private val bufferSize: Int,
) {
    private val lock        = ReentrantLock()
    private val terminal    = AtomicReference<Any>(Unset)
    private val history     = ArrayDeque<Signal.Upstream.Next<T>>()
    private val subscribers = CopyOnWriteArrayList<Channel<Signal.Upstream<T>>>()

    fun emit(value: T) {
        if (terminal.get().notUnset()) return
        val signal = Signal.Upstream.Next(value)
        lock.withLock {
            if (historySize > 0) {
                history.addLast(signal)
                if (historySize != Int.MAX_VALUE && history.size > historySize) history.removeFirst()
            }
            subscribers.forEach { inbox -> inbox.trySend(signal) }
        }
    }

    /** Returns false if the sink is terminated. */
    fun tryEmit(value: T): Boolean {
        if (terminal.get().notUnset()) return false
        emit(value)
        return true
    }

    fun complete() = terminate(Signal.Upstream.Complete)

    fun error(cause: Exception) = terminate(Signal.Upstream.Error(cause))

    private fun terminate(signal: Signal.Upstream<T>) {
        if (!terminal.compareAndSet(Unset, signal)) return
        for (inbox in subscribers) {
            inbox.trySend(signal)
            inbox.close()
        }
    }

    private fun register(inbox: Channel<Signal.Upstream<T>>): AutoCloseable {
        lock.withLock { subscribers.add(inbox) }
        return AutoCloseable { subscribers.remove(inbox) }
    }

    fun asMany(): Many<T> = Many.generate { emit ->
        val inbox = Channel<Signal.Upstream<T>>(bufferSize)
        register(inbox).using {
            val replay = lock.withLock { history.toList() }
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
        register(inbox).using {
            for (item in lock.withLock { history.toList() }) {
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
class BroadcastSink<T : Any>(bufferSize: Int = DEFAULT_BUFFER) : Sink<T>(
    historySize = 0,
    bufferSize  = bufferSize,
)

/** Buffers the full emission history; late subscribers receive all past items then live items. */
class ReplaySink<T : Any>(bufferSize: Int = DEFAULT_BUFFER) : Sink<T>(
    historySize = Int.MAX_VALUE,
    bufferSize  = bufferSize,
)

/** Buffers the last [n] items; late subscribers receive recent history then live items. */
class ReplayLastSink<T : Any>(val count: Int, bufferSize: Int = DEFAULT_BUFFER) : Sink<T>(
    historySize = count,
    bufferSize  = bufferSize,
) {
    init { require(count > 0) { "ReplayLastSink requires count > 0, got $count" } }
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
 * A unicast push source — each emitted item is delivered to exactly one subscriber.
 *
 * Multiple subscribers compete for items via [asMany]; each item goes to exactly one.
 * [asOne] takes the next available item as a [One].
 *
 * Uses a [ConcurrentLinkedQueue] and a conflated [Channel] as wakeup signal.
 */
class UnicastSink<T : Any> {

    private val queue    = ConcurrentLinkedQueue<T>()
    private val signal   = Channel<Unit>(Channel.CONFLATED)
    private val terminal = AtomicReference<Signal.Upstream<T>>(null)

    /**
     * Queues [value] in an unbounded [java.util.concurrent.ConcurrentLinkedQueue].
     * Not a suspend function — memory grows without bound if no subscriber is consuming.
     */
    fun emit(value: T) {
        queue.add(value)
        signal.trySend(Unit)
    }

    fun complete() {
        terminal.compareAndSet(null, Signal.Upstream.Complete)
        signal.trySend(Unit)
    }

    fun error(cause: Exception) {
        terminal.compareAndSet(null, Signal.Upstream.Error(cause))
        signal.trySend(Unit)
    }

    fun asMany(): Many<T> = Many.generate { downstream ->
        while (true) {
            var item = queue.poll()
            while (item != null) {
                if (downstream(Signal.Upstream.Next(item)) == Signal.Downstream.Cancel) return@generate
                item = queue.poll()
            }
            when (val term = terminal.get()) {
                null                        -> signal.receive()
                is Signal.Upstream.Complete -> return@generate
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
