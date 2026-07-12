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

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import org.reactivestreams.Publisher

internal sealed class Step<out T : Any> {
    class Range(val start: Int, val count: Int)                                                       : Step<Int>()
    class Items<T : Any>(val items: Array<out T>)                                                     : Step<T>()
    class FromIterable<T : Any>(val source: Iterable<T>)                                             : Step<T>()
    object Empty                                                                                       : Step<Nothing>()
    class Error(val cause: Exception)                                                                  : Step<Nothing>()
    object Never                                                                                       : Step<Nothing>()
    class FromFlow<T : Any>(val flow: Flow<T>)                                                        : Step<T>()
    class FromPublisher<T : Any>(val publisher: Publisher<T>)                                               : Step<T>()
    class Defer<T : Any>(val factory: () -> Many<T>)                                                  : Step<T>()
    class PipelineSource<T : Any>                                                                      : Step<T>()
    /** Escape hatch for async operators (interval, buffer, …). Not stack-safe for recursive use. */
    class Suspend<T : Any>(
        val block: suspend (
            onNext:     suspend (T) -> Signal.Downstream,
            onComplete: suspend () -> Unit,
            onError:    suspend (Exception) -> Unit,
        ) -> Unit,
    ) : Step<T>()

    class Map<A : Any, B : Any>(val upstream: Step<A>,      val transform: (A) -> B)                              : Step<B>()
    class Filter<T : Any>(val upstream: Step<T>,             val predicate: (T) -> Boolean)                       : Step<T>()
    class Take<T : Any>(val upstream: Step<T>,               val count: Long)                                     : Step<T>()
    class Skip<T : Any>(val upstream: Step<T>,               val count: Long)                                     : Step<T>()
    class ConcatMap<A : Any, B : Any>(val upstream: Step<A>, val transform: (A) -> Many<B>)                       : Step<B>()
    class FlatMap<A : Any, B : Any>(val upstream: Step<A>,   val concurrency: Int, val transform: (A) -> Many<B>) : Step<B>()
}

internal sealed class Frame<in A : Any> {
    class Collect<A : Any>(val action: suspend (A) -> Signal.Downstream)                         : Frame<A>()
    class Map<A : Any, B : Any>(val transform: (A) -> B,       val next: Frame<B>)              : Frame<A>()
    class Filter<A : Any>(val predicate: (A) -> Boolean,        val next: Frame<A>)              : Frame<A>()
    class Take<A : Any>(var remaining: Long,                     val next: Frame<A>)              : Frame<A>()
    class Skip<A : Any>(var toSkip: Long,                        val next: Frame<A>)              : Frame<A>()
    /** Pushes the inner [Many] to the front of the work-deque so it drains before the outer advances. */
    class ConcatBind<A : Any, B : Any>(val transform: (A) -> Many<B>, val next: Frame<B>)       : Frame<A>()
}

internal sealed class RunSource<out T : Any> {
    class Pending<T : Any>(val step: Step<T>) : RunSource<T>()
    class Range(var current: Long, val end: Long) : RunSource<Int>() {
        fun hasNext() = current < end
        fun next()    = (current++).toInt()
    }
    class Items<T : Any>(val items: Array<out T>, var index: Int = 0) : RunSource<T>() {
        fun hasNext() = index < items.size
        fun next()    = items[index++]
    }
    class Iter<T : Any>(val iter: Iterator<T>) : RunSource<T>()
}

internal class Work<T : Any>(var source: RunSource<T>, val frame: Frame<T>)

@Suppress("UNCHECKED_CAST")
internal suspend fun applyFrame(
    item: Any,
    frame: Frame<Any>,
    todo: ArrayDeque<Work<*>>,
): Signal.Downstream {
    var current = item
    var currentFrame: Frame<Any> = frame

    while (true) when (val node = currentFrame) {
        is Frame.Collect<*>   -> return (node as Frame.Collect<Any>).action(current)

        is Frame.Map<*, *>    -> {
            node as Frame.Map<Any, Any>
            current = node.transform(current)
            currentFrame = node.next
        }

        is Frame.Filter<*>    -> {
            node as Frame.Filter<Any>
            if (!node.predicate(current)) return Signal.Downstream.Request
            currentFrame = node.next
        }

        is Frame.Take<*>      -> {
            node as Frame.Take<Any>
            if (node.remaining <= 0L) return Signal.Downstream.Cancel
            // Emit first, then decrement: remaining=1 means this item is the last one.
            val downstream = applyFrame(current, node.next, todo)
            node.remaining--
            return if (node.remaining == 0L) Signal.Downstream.Cancel else downstream
        }

        is Frame.Skip<*>      -> {
            node as Frame.Skip<Any>
            if (node.toSkip > 0L) { node.toSkip--; return Signal.Downstream.Request }
            currentFrame = node.next
        }

        is Frame.ConcatBind<*, *> -> {
            node as Frame.ConcatBind<Any, Any>
            todo.addFirst(Work(RunSource.Pending(node.transform(current).step), node.next))
            return Signal.Downstream.Request
        }
    }
}

// Returns true = completed naturally, false = cancelled by downstream.
@Suppress("UNCHECKED_CAST")
internal suspend fun <T : Any> interpret(
    step: Step<T>,
    frame: Frame<T>,
): Either<Exception, Boolean> {
    val todo = ArrayDeque<Work<*>>()
    todo.addLast(Work(RunSource.Pending(step), frame))

    return try {
        while (todo.isNotEmpty()) {
            val work = todo.first() as Work<Any>

            when (val runSource = work.source) {
                is RunSource.Pending<*> -> { runSource as RunSource.Pending<Any>; resolve(runSource.step, work, todo) }

                is RunSource.Range -> {
                    if (!runSource.hasNext()) { todo.removeFirst(); continue }
                    if (applyFrame(runSource.next(), work.frame, todo) == Signal.Downstream.Cancel) {
                        todo.clear(); return false.right()
                    }
                }

                is RunSource.Items<*> -> {
                    runSource as RunSource.Items<Any>
                    if (!runSource.hasNext()) { todo.removeFirst(); continue }
                    if (applyFrame(runSource.next(), work.frame, todo) == Signal.Downstream.Cancel) {
                        todo.clear(); return false.right()
                    }
                }

                is RunSource.Iter<*> -> {
                    runSource as RunSource.Iter<Any>
                    if (!runSource.iter.hasNext()) { todo.removeFirst(); continue }
                    if (applyFrame(runSource.iter.next(), work.frame, todo) == Signal.Downstream.Cancel) {
                        todo.clear(); return false.right()
                    }
                }
            }
        }
        true.right()
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        e.left()
    }
}

@Suppress("UNCHECKED_CAST")
private suspend fun resolve(
    rawStep: Step<Any>,
    work: Work<Any>,
    todo: ArrayDeque<Work<*>>,
) {
    var currentStep: Step<Any>   = rawStep
    var currentFrame: Frame<Any> = work.frame

    while (true) when (val step = currentStep) {
        is Step.Map<*, *>       -> { currentFrame = Frame.Map(step.transform, currentFrame) as Frame<Any>;        currentStep = step.upstream }
        is Step.Filter<*>       -> { currentFrame = Frame.Filter(step.predicate, currentFrame) as Frame<Any>;     currentStep = step.upstream }
        is Step.Take<*>         -> { currentFrame = Frame.Take(step.count, currentFrame) as Frame<Any>;           currentStep = step.upstream }
        is Step.Skip<*>         -> { currentFrame = Frame.Skip(step.count, currentFrame) as Frame<Any>;           currentStep = step.upstream }
        is Step.ConcatMap<*, *> -> { currentFrame = Frame.ConcatBind(step.transform, currentFrame) as Frame<Any>; currentStep = step.upstream }

        is Step.Range           -> { todo[0] = Work(RunSource.Range(step.start.toLong(), step.start.toLong() + step.count) as RunSource<Any>, currentFrame); return }
        is Step.Items<*>        -> { todo[0] = Work(RunSource.Items(step.items) as RunSource<Any>, currentFrame); return }
        is Step.FromIterable<*> -> { todo[0] = Work(RunSource.Iter(step.source.iterator()) as RunSource<Any>, currentFrame); return }
        is Step.Empty           -> { todo.removeFirst(); return }
        is Step.Error           -> throw step.cause
        is Step.Never           -> { todo.removeFirst(); awaitCancellation() }

        is Step.Defer<*> -> {
            todo[0] = Work(RunSource.Pending(step.factory().step), currentFrame)
            return
        }
        is Step.PipelineSource<*> -> {
            val source = currentCoroutineContext()[SourceSlot]?.publisher as? Many<Any>
                ?: error("Many.pipelineFrom() executed without a bound source — use applyTo() or then()")
            todo[0] = Work(RunSource.Pending(source.step), currentFrame)
            return
        }

        is Step.Suspend<*>, is Step.FlatMap<*, *>, is Step.FromFlow<*>, is Step.FromPublisher<*> -> {
            todo.removeFirst()
            execSuspend(step, currentFrame, todo)
            return
        }
    }
}

@Suppress("UNCHECKED_CAST")
private suspend fun execSuspend(
    step: Step<Any>,
    frame: Frame<Any>,
    todo: ArrayDeque<Work<*>>,
) {
    val block: suspend (
        onNext: suspend (Any) -> Signal.Downstream,
        onComplete: suspend () -> Unit,
        onError: suspend (Exception) -> Unit,
    ) -> Unit = when (step) {
        is Step.Suspend<*>       -> (step as Step.Suspend<Any>).block
        is Step.FromFlow<*>      -> {
            val flow = (step as Step.FromFlow<Any>).flow
            { onNext, onComplete, _ ->
                try {
                    coroutineScope { flow.collect { if (onNext(it) == Signal.Downstream.Cancel) cancel() } }
                    onComplete()
                } catch (_: CancellationException) {}
            }
        }
        is Step.FromPublisher<*> -> {
            val publisher = (step as Step.FromPublisher<Any>).publisher
            { onNext, onComplete, _ ->
                try {
                    coroutineScope { publisher.asFlow().collect { if (onNext(it) == Signal.Downstream.Cancel) cancel() } }
                    onComplete()
                } catch (_: CancellationException) {}
            }
        }
        is Step.Never            -> { _, _, _ -> awaitCancellation() }
        is Step.FlatMap<*, *>    -> {
            step as Step.FlatMap<Any, Any>
            Many.concurrentFlatMapSuspend(step.upstream, step.concurrency, step.transform)
        }
        else -> error("execSuspend: $step")
    }

    var cancelled = false
    block(
        { item -> applyFrame(item, frame, todo).also { if (it == Signal.Downstream.Cancel) cancelled = true } },
        {},
        { exception -> throw exception },
    )
}
