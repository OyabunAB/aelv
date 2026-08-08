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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.experimental.ExperimentalTypeInference
import kotlin.internal.LowPriorityInOverloadResolution
import kotlin.time.Duration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/**
 * A cold publisher of exactly one item of type [T].
 *
 * If the source emits zero items the subscriber receives only `onComplete` without `onNext`.
 * If it emits more than one item, all items after the first are silently consumed.
 */
class One<T : Any> private constructor(
    override val step: Step<T>,
    internal val fusion: Fusion<T> = Fusion.None,
) : Publisher<T>, Observable<T, One<T>>() {

    override fun wrap(
        block: suspend (
            onNext:     OnNext<T>,
            onComplete: OnComplete,
            onError:    OnError,
            onRequest:  Demand,
        ) -> Unit,
    ): One<T> = One(Step.Suspend(block))

    override fun toMany(): Many<T> = Many.fromStep(step, fusion)

    override fun toMaybe(): Maybe<T> = Maybe.fromStep(step, fusion)

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber, ::source)
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    fun asMany(): Many<T> = Many.generate { emit, onRequest ->
        source(
            { value -> emit(Signal.Upstream.Next(value)) },
            { emit(Signal.Upstream.Complete) },
            { issue -> emit(Signal.Upstream.Error(issue)) },
            onRequest,
        )
    }

    fun <R : Any> map(transform: (T) -> R): One<R> {
        val currentFusion = fusion
        return fromStep(
            Step.Map(step, transform),
            if (currentFusion is Fusion.Available) MapFusion(currentFusion, transform)
            else Fusion.None
        )
    }

    @LowPriorityInOverloadResolution
    fun <R : Any> map(transform: suspend (T) -> R): One<R> =
        generate { emit, onRequest ->
            source(
                { value -> emit(Signal.Upstream.Next(transform(value))) },
                { emit(Signal.Upstream.Complete) },
                { issue -> emit(Signal.Upstream.Error(issue)) },
                onRequest,
            )
        }

    /**
     * Maps the single value to another [One] and subscribes to it, forwarding the result downstream.
     *
     * If this [One] errors, the error is forwarded without calling [transform].
     */
    fun <R : Any>flatMap(transform: suspend (T) -> One<R>): One<R> =
        generate { emit, onRequest ->
            source(
                { value ->
                    transform(value).source(
                        { inner -> emit(Signal.Upstream.Next(inner)) },
                        { emit(Signal.Upstream.Complete) },
                        { issue -> emit(Signal.Upstream.Error(issue)) },
                        onRequest,
                    )
                    Signal.Downstream.Cancel
                },
                { emit(Signal.Upstream.Complete) },
                { issue -> emit(Signal.Upstream.Error(issue)) },
                onRequest,
            )
        }

    /**
     * Maps the single value to a [Many] and subscribes to it, forwarding all items downstream.
     *
     * The result type widens from [One] to [Many] because the inner stream can emit zero or more items.
     * If the inner [Many] itself errors, the error propagates and no further items are emitted.
     */
    fun <R : Any> flatMapMany(transform: suspend (T) -> Many<R>): Many<R> =
        Many.generate { emit, onRequest ->
            source(
                { value ->
                    val result = transform(value).collect { inner -> emit(Signal.Upstream.Next(inner)) }
                    if (result is Failure) emit(Signal.Upstream.Error(result.value))
                    else emit(Signal.Upstream.Complete)
                    Signal.Downstream.Cancel
                },
                { emit(Signal.Upstream.Complete) },
                { issue -> emit(Signal.Upstream.Error(issue)) },
                onRequest,
            )
        }

    /**
     * Maps the single value to a [Maybe], which may or may not emit a result.
     *
     * Use this when the mapping step can legitimately produce no value — the result is a [Maybe]
     * rather than a [One], reflecting that the downstream may complete empty.
     * If this [One] errors, the error is forwarded without calling [transform].
     */
    fun <R : Any> flatMapMaybe(transform: suspend (T) -> Maybe<R>): Maybe<R> =
        Maybe { onNext, onComplete, onError, onRequest ->
            source(
                { value -> transform(value).source(onNext, onComplete, onError, onRequest); Signal.Downstream.Cancel },
                onComplete,
                onError,
                onRequest,
            )
        }

    /**
     * Maps the single value to a [None] and awaits its completion, discarding the result type.
     *
     * The return type is [None] because the entire chain produces no items — only a completion or
     * error signal.  Any error from the inner [None] is rethrown and terminates the outer stream.
     */
    fun flatMapNone(transform: suspend (T) -> None<*>): None<T> =
        flatMap { value -> transform(value).thenReturn(value) }.discard()

    /**
     * Suspends until this [One] emits its value or signals an error.
     *
     * Returns [Either.Right] containing the value on success, or [Either.Left] containing the
     * [Exception] if the source errored or completed without emitting.
     */
    suspend fun await(): Either<Exception, T> {
        val currentFusion = fusion
        if (currentFusion is Fusion.Available) {
            val poll = currentFusion.create(kotlin.coroutines.EmptyCoroutineContext)
            if (poll != null) return try {
                (poll.poll() ?: throw NoElementException()).right()
            } catch (e: CancellationException) { throw e } catch (e: Exception) { e.left() }
        }
        var result: Either<Unset, T> = Unset.left()
        val outcome = collect { value -> result = value.right(); Signal.Downstream.Cancel }
        val final = result
        return when {
            final  is Success -> final.value.right()
            outcome is Failure -> outcome
            else                   -> NoElementException().left()
        }
    }

    /**
     * Suspends until this [One] emits its value or [timeout] elapses.
     *
     * Returns [Either.Right] with the value on success, or [Either.Left] with a
     * [ExceededTimeoutException] if the timeout elapsed before a value was emitted, or with the upstream
     * [Exception] if the source errored.
     */
    suspend fun await(timeout: Duration): Either<Exception, T> =
        Either.catching(timeout) { await().rightOrThrow() }

    /**
     * Returns a [One] that executes the upstream source at most once and replays the result to every
     * subscriber.  The first subscriber triggers execution; subsequent subscribers receive the cached
     * result immediately without re-executing the source.
     *
     * Thread-safe: a [Mutex] ensures only one subscriber runs the source even under concurrent
     * subscriptions.
     */
    fun cache(): One<T> {
        val mutex  = Mutex()
        var cached: Cached<Outcome<T>> = Unset.left()
        return One.generate { emit, onRequest ->
            val result: Outcome<T> = mutex.withLock {
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

    fun <B : Any, R : Any> zipWith(other: One<B>, transform: (T, B) -> R): One<R> =
        zip(this, other, transform)

    fun concatWith(other: One<T>): Many<T> = Many.concat(toMany(), other.toMany())


    companion object {
        fun <T : Any> single(value: T): One<T> = One(Step.Suspend { onNext, onComplete, _, _ ->
            if (onNext(value) != Signal.Downstream.Cancel) onComplete()
        })

        /**
         * Creates a [One] that executes [closure] on each subscription, emitting its return value.
         * Exceptions thrown by [closure] are caught and routed to [onError].
         * Use [context] to shift execution to a specific [CoroutineContext].
         */
        fun <T : Any> defer(context: CoroutineContext? = null, closure: suspend () -> T): One<T> =
            One(Step.Suspend { onNext, onComplete, _, _ ->
                val value = if (context != null) withContext(currentCoroutineContext() + context) { closure() } else closure()
                if (onNext(value) != Signal.Downstream.Cancel) onComplete()
            })

        internal fun <T : Any> generate(
            block: suspend (
                emit:      suspend (Signal.Upstream<T>) -> Signal.Downstream,
                onRequest: Demand,
            ) -> Unit,
        ): One<T> = One(Step.Suspend { onNext, onComplete, onError, onRequest ->
            block({ signal ->
                when (signal) {
                    is Signal.Upstream.Next     -> onNext(signal.value)
                    is Signal.Upstream.Complete -> { onComplete(); Signal.Downstream.Cancel }
                    is Signal.Upstream.Error    -> { onError(signal.cause); Signal.Downstream.Cancel }
                }
            }, onRequest)
        })

        internal operator fun <T : Any> invoke(
            block: suspend (
                onNext:     OnNext<T>,
                onComplete: OnComplete,
                onError:    OnError,
                onRequest:  Demand,
            ) -> Unit,
        ): One<T> = One(Step.Suspend(block))

        @Suppress("UNCHECKED_CAST")
        fun <T : Any> from(publisher: Publisher<T>): One<T> = One(Step.Suspend { onNext, onComplete, onError, onRequest ->
            when (publisher) {
                is Many<*> -> (publisher as Many<T>).source(
                    { value -> onNext(value); Signal.Downstream.Cancel },
                    onComplete,
                    onError,
                    onRequest,
                )
                is One<*>  -> (publisher as One<T>).source(onNext, onComplete, onError, onRequest)
                else       -> { publisher.asFlow().collectCancelling { value -> onNext(value); false }; onComplete() }
            }
        })

        fun <T : Any> error(cause: Exception): One<T> = One(Step.Error(cause))

        fun <T : Any> never(): One<T> = One(Step.Never)

        fun <T : Any> pipelineFrom(): One<T> = One(Step.PipelineSource())

        internal fun <T : Any> fromStep(step: Step<T>, fusion: Fusion<T> = Fusion.None): One<T> = One(step, fusion)

        fun <T : Any> create(block: (success: (T) -> Unit, failure: (Exception) -> Unit) -> Unit): One<T> =
            One.generate { emit, _ ->
                val result = suspendCancellableCoroutine<Either<Exception, T>> { continuation ->
                    var emitted = false
                    fun emit(value: Either<Exception, T>) {
                        check(!emitted) { "One.create: callback called more than once" }
                        emitted = true
                        continuation.resume(value)
                    }
                    block(
                        { value -> emit(value.right()) },
                        { cause -> emit(cause.left()) },
                    )
                }
                when (result) {
                    is Success -> {
                        if (emit(Signal.Upstream.Next(result.value)) != Signal.Downstream.Cancel)
                            emit(Signal.Upstream.Complete)
                    }
                    is Failure -> emit(Signal.Upstream.Error(result.value))
                }
            }

        fun <R : Any, T : Any> resource(
            acquire: () -> One<R>,
            release: (R, Either<Throwable, Unit>) -> None<*>,
            use:     (R) -> One<T>,
        ): One<T> = acquire().flatMap { resource ->
            Many.generate { emit, _ -> Many.bracket(resource, release, emit) { use(resource).toMany() } }
                .firstMaybe()
                .or { throw NoElementException() }
        }

        /**
         * Pairs the values of [a] and [b], applying [transform] once both have emitted.
         * If either source completes without emitting, the result completes empty.
         */
        fun <A : Any, B : Any, R : Any> zip(a: One<A>, b: One<B>, transform: (A, B) -> R): One<R> =
            generate { emit, onRequest ->
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



    }
}
