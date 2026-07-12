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

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext

/**
 * Coroutine context element that carries the bound source publisher for a pipeline execution.
 *
 * Set once per subscription by [Many.applyTo] / [Many.then] (and their [One]/[None] overloads).
 * Read by [Many.pipelineFrom] / [One.pipelineFrom] / [None.pipelineFrom] source lambdas to resolve their
 * upstream at runtime (coroutine path). The fusion path resolves the same connection at
 * composition time via [Fusion.connectSource], so this element is never read inside a poll loop.
 */
internal class SourceSlot(val publisher: Any) : CoroutineContext.Element {
    companion object Key : CoroutineContext.Key<SourceSlot>
    override val key: CoroutineContext.Key<*> = Key
}

/**
 * Composes this [Many] pipeline step with [next], returning a new [Many] that feeds this step's
 * output into [next]'s [Many.pipelineFrom] source slot.
 *
 * Both sides may be pipeline definitions (built on [Many.pipelineFrom]) or concrete sources. The
 * fusion chains of both sides are connected at call time via [Fusion.connectSource]: if the
 * resulting chain is fully resolved, collection will use a single fused poll loop with no
 * per-item coroutine overhead. Unresolved [SourceFusion] leaves fall back gracefully to the
 * coroutine path.
 *
 * The composition is lazy — no execution occurs until a terminal drives the result.
 *
 * Canonical use: chaining named pipeline definitions before binding a source.
 * ```kotlin
 * val full: One<List<ByteArray>> = normalise.then(encode).then(collect)
 * Many.items("a", "b").applyTo(full).await()
 * ```
 */
fun <T : Any, R : Any> Many<T>.then(next: Many<R>): Many<R> {
    val pipelineFusion = next.fusion
    val sourceFusion = this.fusion
    val connectedFusion: Fusion<R> =
        if (pipelineFusion is Fusion.Available && sourceFusion is Fusion.Available) pipelineFusion.connectSource(sourceFusion)
        else Fusion.None
    val left = this
    return Many.fused(connectedFusion) { onNext, onComplete, onError ->
        // Capture the coroutine context that exists *before* we inject the new source slot.
        // This context contains the real upstream source slot (set by applyTo or an outer then)
        // and is what left's own pipelineFrom will need to resolve its source.
        val outerCtx = currentCoroutineContext()
        // Wrap left so that when next's pipelineFrom drives it, left runs in the outer context
        // (where its own source slot is correctly set) rather than the inner context (where
        // the slot has been overwritten to point at left itself — which would be circular).
        val leftDriven = Many.fused<T> { innerOnNext, innerOnComplete, innerOnError ->
            withContext(outerCtx) {
                left.source(innerOnNext, innerOnComplete, innerOnError)
            }
        }
        withContext(outerCtx + SourceSlot(leftDriven)) {
            next.source(onNext, onComplete, onError)
        }
    }
}

/**
 * Composes this [Many] pipeline step with a terminal [One] [next], returning a [One] that feeds
 * this step's output into [next]'s [Many.pipelineFrom] source slot.
 *
 * Typical use: attaching a collecting terminal (issue.g. [toList], [fold]) to a [Many] pipeline.
 * ```kotlin
 * val pipeline: One<List<Int>> = Many.pipelineFrom<Int>().map { it * 2 }.then(collector)
 * ```
 */
fun <T : Any, R : Any> Many<T>.then(next: One<R>): One<R> {
    val left = this
    return One.generate { emit ->
        val outerCtx = currentCoroutineContext()
        val leftDriven = Many.fused<T> { innerOnNext, innerOnComplete, innerOnError ->
            withContext(outerCtx) { left.source(innerOnNext, innerOnComplete, innerOnError) }
        }
        withContext(outerCtx + SourceSlot(leftDriven)) {
            next.source(
                { v -> emit(Signal.Upstream.Next(v)) },
                { emit(Signal.Upstream.Complete) },
                { issue -> emit(Signal.Upstream.Error(issue)) },
            )
        }
    }
}

fun <T : Any, R : Any> Many<T>.then(next: None<R>): None<R> {
    val left = this
    return None.generate {
        val outerCtx = currentCoroutineContext()
        val leftDriven = Many.fused<T> { innerOnNext, innerOnComplete, innerOnError ->
            withContext(outerCtx) { left.source(innerOnNext, innerOnComplete, innerOnError) }
        }
        withContext(outerCtx + SourceSlot(leftDriven)) {
            val result = next.await()
            if (result is Failure) throw result.value
        }
    }
}

fun <T : Any, R : Any> Many<T>.applyTo(pipeline: Many<R>): Many<R> = this.then(pipeline)

fun <T : Any, R : Any> Many<T>.applyTo(pipeline: One<R>): One<R> = this.then(pipeline)

fun <T : Any, R : Any> Many<T>.applyTo(pipeline: None<R>): None<R> = this.then(pipeline)

/**
 * Composes this [One] pipeline step with a terminal [One] [next].
 *
 * Feeds this step's single value into [next]'s [One.pipelineFrom] source slot.
 */
fun <T : Any, R : Any> One<T>.then(next: One<R>): One<R> {
    val left = this
    return One.generate { emit ->
        val outerCtx = currentCoroutineContext()
        val leftDriven = One.generate<T> { innerEmit ->
            withContext(outerCtx) {
                left.source(
                    { v -> innerEmit(Signal.Upstream.Next(v)) },
                    { innerEmit(Signal.Upstream.Complete) },
                    { issue -> innerEmit(Signal.Upstream.Error(issue)) },
                )
            }
        }
        withContext(outerCtx + SourceSlot(leftDriven)) {
            next.source(
                { v -> emit(Signal.Upstream.Next(v)) },
                { emit(Signal.Upstream.Complete) },
                { issue -> emit(Signal.Upstream.Error(issue)) },
            )
        }
    }
}

fun <T : Any, R : Any> One<T>.applyTo(pipeline: One<R>): One<R> = this.then(pipeline)

/**
 * Marks a pipeline definition as stateful — it captures mutable state that is shared across
 * all subscriptions.
 *
 * By default, pipeline definitions built on [Many.pipelineFrom] / [One.pipelineFrom] / [None.pipelineFrom]
 * are expected to be pure: all mutable execution state must live inside [Fusion.Available.create]
 * instances, and the definition itself must be safe for concurrent reuse.
 *
 * Use [mutablePipelineFrom] with this opt-in to explicitly break that contract. The caller takes full
 * responsibility for correct usage — concurrent subscriptions to a stateful pipeline will race.
 */
@RequiresOptIn(
    message = "This pipeline captures mutable state shared across all subscriptions. " +
              "Concurrent reuse is the caller's responsibility — subscriptions will race.",
    level = RequiresOptIn.Level.ERROR,
)
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS, AnnotationTarget.PROPERTY)
annotation class MutablePipeline

/**
 * Escape hatch for [Many] pipeline definitions that intentionally capture shared mutable state.
 *
 * Mirrors [Many.pipelineFrom] but requires [MutablePipeline] opt-in. The annotation must be
 * propagated or suppressed at every call site, making the stateful contract visible and explicit.
 *
 * Use only when single-subscription or externally-synchronised access is guaranteed by the caller.
 * Concurrent subscriptions to a pipeline built on this source will race on any captured state.
 *
 * ```kotlin
 * @OptIn(MutablePipeline::class)
 * val runningTotal: Many<Int> = Many.mutablePipelineFrom<Int>()
 *     .map { sum += it; sum }
 * ```
 */
@MutablePipeline
fun <T : Any> Many.Companion.mutablePipelineFrom(): Many<T> = pipelineFrom()

/**
 * Escape hatch for [One] pipeline definitions that intentionally capture shared mutable state.
 *
 * ```kotlin
 * @OptIn(MutablePipeline::class)
 * val cached: One<String> = One.mutablePipelineFrom<String>()
 *     .map { cache ?: it.also { v -> cache = v } }
 * ```
 */
@MutablePipeline
fun <T : Any> One.Companion.mutablePipelineFrom(): One<T> = pipelineFrom()

/**
 * Escape hatch for [None] pipeline definitions that intentionally capture shared mutable state.
 */
@MutablePipeline
fun <T : Any> None.Companion.mutablePipelineFrom(): None<T> = pipelineFrom()
