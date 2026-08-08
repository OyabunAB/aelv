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
import kotlin.experimental.ExperimentalTypeInference
import kotlin.internal.LowPriorityInOverloadResolution
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.reactive.asFlow


/**
 * A cold publisher of zero or one items of type [T].
 *
 * The contract: [source] calls [onNext] at most once, then calls [onComplete].
 * If no value is available it calls [onComplete] directly without calling [onNext].
 * On error it calls [onError] instead of [onComplete].
 */
class Maybe<T : Any> private constructor(
    override val step: Step<T>,
    internal val fusion: Fusion<T> = Fusion.None,
) : Publisher<T>, Observable<T, Maybe<T>>() {

    override fun wrap(
        block: suspend (
            onNext:     OnNext<T>,
            onComplete: OnComplete,
            onError:    OnError,
            onRequest:  Demand,
        ) -> Unit,
    ): Maybe<T> = Maybe(Step.Suspend(block))

    override fun toMany(): Many<T> = Many.fromStep(step, fusion)

    override fun subscribe(subscriber: Subscriber<in T>) {
        val subscription = StreamSubscription(subscriber, ::source)
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    fun <R : Any> map(transform: (T) -> R): Maybe<R> {
        val currentFusion = fusion
        return fromStep(
            step = Step.Map(step, transform),
            fusion = if (currentFusion is Fusion.Available) MapFusion(currentFusion, transform)
                     else Fusion.None)
    }

    @LowPriorityInOverloadResolution
    fun <R : Any> map(transform: suspend (T) -> R): Maybe<R> =
        Maybe { onNext, onComplete, onError, onRequest ->
            source(
                { value -> onNext(transform(value)) },
                onComplete,
                onError,
                onRequest,
            )
        }

    /** Keeps the value if [predicate] returns true, otherwise produces an empty [Maybe]. */
    fun filter(predicate: (T) -> Boolean): Maybe<T> {
        val currentFusion = fusion
        return fromStep(Step.Filter(step, predicate), if (currentFusion is Fusion.Available) FilterFusion(currentFusion, predicate) else Fusion.None)
    }

    @LowPriorityInOverloadResolution
    fun filter(predicate: suspend (T) -> Boolean): Maybe<T> =
        Maybe { onNext, onComplete, onError, onRequest ->
            var emitComplete = false
            source(
                { value -> if (predicate(value)) onNext(value) else { emitComplete = true; Signal.Downstream.Cancel } },
                onComplete,
                onError,
                onRequest,
            )
            // Call onComplete after source() returns, not from inside the onNext callback — RS §1.3.
            if (emitComplete) onComplete()
        }


    /**
     * Maps the present value to another [Maybe] and subscribes to it; if this [Maybe] is empty
     * the result completes empty without invoking [transform].
     *
     * If this [Maybe] errors, the error is forwarded without calling [transform].
     */
    fun <R : Any> flatMap(transform: suspend (T) -> Maybe<R>): Maybe<R> =
        Maybe { onNext, onComplete, onError, onRequest ->
            source(
                { value -> transform(value).source(onNext, onComplete, onError, onRequest); Signal.Downstream.Cancel },
                onComplete,
                onError,
                onRequest,
            )
        }

    /**
     * Maps the present value to a [One] and subscribes to it; if this [Maybe] is empty
     * the result completes empty without invoking [transform].
     *
     * If this [Maybe] errors, the error is forwarded without calling [transform].
     */
    fun <R : Any> flatMapOne(transform: suspend (T) -> One<R>): Maybe<R> =
        Maybe { onNext, onComplete, onError, onRequest ->
            source(
                { value -> transform(value).source(onNext, onComplete, onError, onRequest); Signal.Downstream.Cancel },
                onComplete,
                onError,
                onRequest,
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
    fun <R : Any> flatMapMany(transform: suspend (T) -> Many<R>): Many<R> =
        Many.generate { emit, onRequest ->
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
     * Maps the present value to a [None] and awaits completion; if this [Maybe] is empty, completes
     * immediately without invoking [transform].
     *
     * Useful for fire-and-forget side effects that should be skipped when no value is present.
     */
    fun flatMapNone(transform: suspend (T) -> None<T>): None<T> =
        toMany().flatMapNone(transform)

    /**
     * Provides a fallback value when this [Maybe] is empty, producing a [One].
     *
     * If this [Maybe] emits a value, that value is forwarded. If it completes empty,
     * [fallback] is invoked and its result is emitted.
     */
    fun or(fallback: suspend () -> T): One<T> =
        One.generate { emit, onRequest ->
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
                onRequest,
            )
        }

    /**
     * Switches to [fallback] stream when this [Maybe] is empty.
     *
     * If this [Maybe] emits a value, that value is forwarded and [fallback] is never subscribed.
     * If it completes empty, [fallback] is subscribed and its items are forwarded.
     */
    fun orMany(fallback: suspend () -> Many<T>): Many<T> =
        Many.generate { emit, onRequest ->
            var emitted = false
            source(
                { value -> emitted = true; emit(Signal.Upstream.Next(value)) },
                {
                    if (!emitted) {
                        fallback().source(
                            { inner -> emit(Signal.Upstream.Next(inner)) },
                            { emit(Signal.Upstream.Complete) },
                            { issue -> emit(Signal.Upstream.Error(issue)) },
                            onRequest,
                        )
                    } else {
                        emit(Signal.Upstream.Complete)
                    }
                },
                { issue -> emit(Signal.Upstream.Error(issue)) },
                onRequest,
            )
        }

    /**
     * Converts to a [One], throwing [NoElementException] if this [Maybe] is empty.
     *
     * Use [or] when the empty case is expected and a fallback is available.
     */
    fun toOne(): One<T> =
        One.defer {
            var result: T? = null
            collect { value -> result = value; Signal.Downstream.Cancel }
            result ?: throw NoElementException()
        }

    suspend fun await(): Either<Exception, T?> {
        val currentFusion = fusion
        if (currentFusion is Fusion.Available) {
            val poll = currentFusion.create(kotlin.coroutines.EmptyCoroutineContext)
            if (poll != null) return try {
                poll.poll().right()
            } catch (e: CancellationException) { throw e } catch (e: Exception) { e.left() }
        }
        return Either.catching {
            var result: T? = null
            source(
                { value -> result = value; Signal.Downstream.Cancel },
                { },
                ::rethrow,
                Demand(),
            )
            result
        }
    }

    fun concatWith(other: Maybe<T>): Many<T> = Many.concat(toMany(), other.toMany())

    companion object {

        fun <T : Any> present(value: T): Maybe<T> = Maybe(Step.Suspend { onNext, onComplete, _, _ ->
            if (onNext(value) != Signal.Downstream.Cancel) onComplete()
        })

        fun <T : Any> empty(): Maybe<T> = Maybe(Step.Empty)

        fun <T : Any> error(cause: Exception): Maybe<T> = Maybe(Step.Error(cause))

        fun <T : Any> never(): Maybe<T> = Maybe(Step.Never)

        /**
         * Creates a [Maybe] that executes [closure] on each subscription.
         * If [closure] returns a non-null value, that value is emitted and the stream completes.
         * If [closure] returns null, the stream completes empty.
         * Exceptions are caught and routed to [onError].
         */
        fun <T : Any> defer(closure: suspend () -> T?): Maybe<T> = Maybe(Step.Suspend { onNext, onComplete, onError, _ ->
            try {
                val value = closure()
                if (value != null) {
                    if (onNext(value) != Signal.Downstream.Cancel) onComplete()
                } else {
                    onComplete()
                }
            } catch (exception: Exception) {
                onError(exception)
            }
        })

        fun <T : Any> from(publisher: Publisher<T>): Maybe<T> = Maybe(Step.Suspend { onNext, onComplete, onError, _ ->
            var emitted = false
            try {
                publisher.asFlow().collectCancelling { value ->
                    if (!emitted) {
                        emitted = true
                        onNext(value) == Signal.Downstream.Cancel
                    } else {
                        true
                    }
                }
                onComplete()
            } catch (exception: Exception) {
                onError(exception)
            }
        })

        internal fun <T : Any> generate(
            block: suspend (
                emit:      suspend (Signal.Upstream<T>) -> Signal.Downstream,
                onRequest: Demand,
            ) -> Unit,
        ): Maybe<T> = Maybe(Step.Suspend { onNext, onComplete, onError, onRequest ->
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
        ): Maybe<T> = Maybe(Step.Suspend(block))

        internal fun <T : Any> fromStep(step: Step<T>, fusion: Fusion<T> = Fusion.None): Maybe<T> =
            Maybe(step, if (fusion is Fusion.Available) TakeFusion(fusion, 1) else fusion)

        fun <R : Any, T : Any> resource(
            acquire: () -> One<R>,
            release: (R, Either<Throwable, Unit>) -> None<*>,
            use:     (R) -> Maybe<T>,
        ): Maybe<T> = acquire().flatMapMaybe { resource ->
            Many.generate { emit, _ -> Many.bracket(resource, release, emit) { use(resource).toMany() } }
                .firstMaybe()
        }

    }
}
