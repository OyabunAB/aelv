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
@file:OptIn(ExperimentalTypeInference::class)
package se.oyabun.aelv

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import kotlin.coroutines.CoroutineContext
import kotlin.experimental.ExperimentalTypeInference
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.withContext

/**
 * A cold publisher of no items of type [T].
 *
 * The type parameter [T] exists only for type-system compatibility and carries no values.
 * Use `None<Unit>` when the type is irrelevant.
 */
class None<T : Any> private constructor(
    override val step: Step<T>,
) : Publisher<Nothing>, Observable<T, None<T>>() {

    override fun wrap(
        block: suspend (
            onNext:     OnNext<T>,
            onComplete: OnComplete,
            onError:    OnError,
            onRequest:  Demand,
        ) -> Unit,
    ): None<T> = None(Step.Suspend(block))

    override fun subscribe(subscriber: Subscriber<in Nothing>) {
        val subscription = CompletionSubscription(subscriber) {
            val result = await()
            if (result is Failure) throw result.value
        }
        subscription.deliverSubscription(subscriber, subscription::cancel, subscription::onSubscribeComplete)
    }

    /**
     * Sequences this [None] with a [One] producer: awaits completion of the [None], then subscribes
     * to the [One] returned by [producer].
     *
     * If this [None] errors, [producer] is never called and the error is forwarded.  This is the
     * primary way to chain a fire-and-forget step before a value-producing step without nesting.
     */
    @OverloadResolutionByLambdaReturnType
    fun <R : Any> andThen(producer: suspend () -> One<R>): One<R> =
        One.generate { emit, onRequest ->
            val result = await()
            if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
            producer().source(
                { value -> emit(Signal.Upstream.Next(value)) },
                { emit(Signal.Upstream.Complete) },
                { issue -> emit(Signal.Upstream.Error(issue)) },
                onRequest,
            )
        }

    /**
     * Sequences this [None] with a [Maybe] producer.  The [Maybe] is only subscribed if this [None]
     * completes without error; an error in the [None] is forwarded and [producer] is skipped.
     */
    @OverloadResolutionByLambdaReturnType
    fun <R : Any> andThen(producer: suspend () -> Maybe<R>): Maybe<R> =
        Maybe { onNext, onComplete, onError, onRequest ->
            val result = await()
            if (result is Failure) { onError(result.value); return@Maybe }
            producer().source(onNext, onComplete, onError, onRequest)
        }

    /**
     * Sequences this [None] with a [Many] producer.  The [Many] is only subscribed if this [None]
     * completes without error; an error in the [None] terminates the stream without subscribing to
     * [producer].
     */
    @OverloadResolutionByLambdaReturnType
    fun <R : Any> andThen(producer: suspend () -> Many<R>): Many<R> =
        Many.generate { emit, onRequest ->
            val result = await()
            if (result is Failure) { emit(Signal.Upstream.Error(result.value)); return@generate }
            producer().source(
                { value -> emit(Signal.Upstream.Next(value)) },
                { emit(Signal.Upstream.Complete) },
                { issue -> emit(Signal.Upstream.Error(issue)) },
                onRequest,
            )
        }

    /**
     * Sequences two [None]s: awaits this one, then awaits the [None] returned by [producer].
     * Any error from either step is rethrown, short-circuiting the second step if the first fails.
     */
    @OverloadResolutionByLambdaReturnType
    fun <R : Any> andThen(producer: suspend () -> None<R>): None<R> =
        None.generate {
            val result = await()
            if (result is Failure) throw result.value
            producer().await().let { if (it is Failure) throw it.value }
        }

    companion object {
        /**
         * Creates a [None] that executes [closure] as a side-effect on each subscription.
         * Exceptions thrown by [closure] are caught and routed to [onError].
         * Use [context] to shift execution to a specific [CoroutineContext].
         */
        fun <T : Any> defer(context: CoroutineContext? = null, closure: suspend () -> Unit): None<T> =
            None(Step.Suspend { _, onComplete, onError, _ ->
                try {
                    if (context != null) withContext(currentCoroutineContext() + context) { closure() } else closure()
                    onComplete()
                } catch (e: Exception) {
                    onError(e)
                }
            })

        internal fun <T : Any> generate(closure: suspend () -> Unit): None<T> =
            None(Step.Suspend { _, onComplete, onError, _ ->
                try { closure(); onComplete() } catch (e: Exception) { onError(e) }
            })

        fun <T : Any> from(publisher: Publisher<T>): None<T> =
            None(Step.Suspend { _, onComplete, onError, _ ->
                try { publisher.asFlow().collect { }; onComplete() } catch (e: Exception) { onError(e) }
            })

        fun <T : Any> complete(): None<T> = None(Step.Empty)
        fun <T : Any> error(cause: Exception): None<T> = None(Step.Error(cause))
        fun <T : Any> never(): None<T> = None(Step.Never)

        fun <T : Any> pipelineFrom(): None<T> = None(Step.PipelineSource())

        internal operator fun <T : Any> invoke(closure: suspend () -> Unit): None<T> =
            generate(closure)

        fun <R : Any, T : Any> resource(
            acquire: () -> One<R>,
            release: (R, Either<Throwable, Unit>) -> None<*>,
            use:     (R) -> None<T>,
        ): None<T> = acquire().flatMapMany { resource ->
            Many.generate<T> { emit, _ -> Many.bracket(resource, release, emit) { use(resource).toMany() } }
        }.discard()
    }

    suspend fun await(): Either<Exception, Unit> = collect { Signal.Downstream.Request }
}
