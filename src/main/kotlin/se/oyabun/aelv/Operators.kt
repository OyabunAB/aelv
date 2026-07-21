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
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicInteger

private sealed interface Tagged<out A : Any, out B : Any>
private data class FromA<out A : Any>(val value: A) : Tagged<A, Nothing>
private data class FromB<out B : Any>(val value: B) : Tagged<Nothing, B>

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
            val result = src.collect { value ->
                emit(Signal.Upstream.Next(value)).also { if (it == Signal.Downstream.Cancel) cancelled = true }
            }
            if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
            if (cancelled) return@generate
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

/**
 * Emits a combined value whenever either [a] or [b] emits, using the most recent value from the
 * other source.  Does not emit until both sources have emitted at least one item.
 */
fun <A : Any, B : Any, R : Any> combineLatest(a: Many<A>, b: Many<B>, transform: (A, B) -> R): Many<R> =
    Many.generate { emit ->
        // Channel carries tagged values (FromA/FromB) plus errors.
        // All producer signals pass through the channel — single collector, serial emit — RS 1.3.
        val channel = Channel<Signal.Upstream<Tagged<A, B>>>(Channel.BUFFERED)
        coroutineScope {
            val jobA = launch {
                a.source(
                    { value -> channel.send(Signal.Upstream.Next(FromA(value))); Signal.Downstream.Request },
                    { },
                    { issue -> channel.send(Signal.Upstream.Error(issue)) },
                )
            }
            val jobB = launch {
                b.source(
                    { value -> channel.send(Signal.Upstream.Next(FromB(value))); Signal.Downstream.Request },
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
                        is FromA -> {
                            latestA = tagged.value.right()
                            val capturedB = latestB
                            if (capturedB is Success) {
                                if (emit(Signal.Upstream.Next(transform(tagged.value, capturedB.value))) == Signal.Downstream.Cancel) {
                                    terminated = true; break
                                }
                            }
                        }
                        is FromB -> {
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

private suspend fun <R : Any, T : Any> bracket(
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
        cancelled     -> { }
        else          -> emit(Signal.Upstream.Complete)
    }
}

fun <R : Any, T : Any> Many.Companion.resource(
    acquire: () -> One<R>,
    release: (R, Either<Throwable, Unit>) -> None<*>,
    use:     (R) -> Many<T>,
): Many<T> =
    acquire().flatMapMany { resource ->
        Many.generate { emit -> bracket(resource, release, emit) { use(resource) } }
    }

fun <R : Any, T : Any> One.Companion.resource(
    acquire: () -> One<R>,
    release: (R, Either<Throwable, Unit>) -> None<*>,
    use:     (R) -> One<T>,
): One<T> =
    acquire().flatMap { resource ->
        Many.generate<T> { emit -> bracket(resource, release, emit) { use(resource).toMany() } }
            .firstMaybe()
            .or { throw NoElementException() }
    }

fun <R : Any, T : Any> Maybe.Companion.resource(
    acquire: () -> One<R>,
    release: (R, Either<Throwable, Unit>) -> None<*>,
    use:     (R) -> Maybe<T>,
): Maybe<T> =
    acquire().flatMapMaybe { resource ->
        Many.generate<T> { emit -> bracket(resource, release, emit) { use(resource).toMany() } }
            .firstMaybe()
    }

fun <R : Any, T : Any> None.Companion.resource(
    acquire: () -> One<R>,
    release: (R, Either<Throwable, Unit>) -> None<*>,
    use:     (R) -> None<T>,
): None<T> =
    acquire().flatMapMany { resource ->
        Many.generate<T> { emit -> bracket(resource, release, emit) { use(resource).toMany() } }
    }.discard()
