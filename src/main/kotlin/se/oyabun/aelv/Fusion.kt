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

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

sealed interface Fusion<out T : Any> {
    data object None : Fusion<Nothing>
    abstract class Available<T : Any> : Fusion<T> {
        abstract fun create(context: CoroutineContext = EmptyCoroutineContext): Available<T>?
        abstract fun poll(): T?
        open fun connectSource(upstream: Available<*>): Fusion<T> = None
    }
}

class RangeFusion(private val start: Int, private val count: Int) : Fusion.Available<Int>() {
    private val end: Long = start.toLong() + count
    private var current: Long = start.toLong()
    override fun create(context: CoroutineContext): Fusion.Available<Int> = RangeFusion(start, count)
    override fun poll(): Int? = if (current < end) current++.toInt() else null
}

class ArrayFusion<T : Any>(private val items: Array<out T>) : Fusion.Available<T>() {
    private var index = 0
    override fun create(context: CoroutineContext): Fusion.Available<T> = ArrayFusion(items)
    override fun poll(): T? = if (index < items.size) items[index++] else null
}

class IterableFusion<T : Any>(private val iterable: Iterable<T>) : Fusion.Available<T>() {
    private var iterator: Iterator<T> = iterable.iterator()
    override fun create(context: CoroutineContext): Fusion.Available<T> = IterableFusion(iterable)
    override fun poll(): T? = if (iterator.hasNext()) iterator.next() else null
}

internal class MapFusion<T : Any, R : Any>(
    internal val upstream: Fusion.Available<T>,
    internal val transform: (T) -> R,
) : Fusion.Available<R>() {
    override fun create(context: CoroutineContext): Fusion.Available<R>? =
        upstream.create(context)?.let { MapFusion(it, transform) }
    override fun poll(): R? = upstream.poll()?.let(transform)
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<R> {
        val connected = this.upstream.connectSource(upstream)
        return if (connected is Fusion.Available) MapFusion(connected, transform) else Fusion.None
    }
}

internal class FilterFusion<T : Any>(
    internal val upstream: Fusion.Available<T>,
    internal val predicate: (T) -> Boolean,
) : Fusion.Available<T>() {
    override fun create(context: CoroutineContext): Fusion.Available<T>? =
        upstream.create(context)?.let { FilterFusion(it, predicate) }
    override fun poll(): T? {
        tailrec fun next(): T? {
            val value = upstream.poll() ?: return null
            return if (predicate(value)) value else next()
        }
        return next()
    }
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<T> {
        val connected = this.upstream.connectSource(upstream)
        return if (connected is Fusion.Available) FilterFusion(connected, predicate) else Fusion.None
    }
}

internal class TakeFusion<T : Any>(
    internal val upstream: Fusion.Available<T>,
    internal val limit: Long,
) : Fusion.Available<T>() {
    private var remaining = limit
    override fun create(context: CoroutineContext): Fusion.Available<T>? =
        upstream.create(context)?.let { TakeFusion(it, limit) }
    override fun poll(): T? {
        if (remaining == 0L) return null
        val value = upstream.poll() ?: return null
        remaining--
        return value
    }
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<T> {
        val connected = this.upstream.connectSource(upstream)
        return if (connected is Fusion.Available) TakeFusion(connected, limit) else Fusion.None
    }
}

internal class SourceFusion<T : Any> : Fusion.Available<T>() {
    override fun create(context: CoroutineContext): Fusion.Available<T>? = null
    override fun poll(): T? = error("SourceFusion.poll() called on unresolved pipeline")
    @Suppress("UNCHECKED_CAST")
    override fun connectSource(upstream: Fusion.Available<*>): Fusion<T> = upstream as Fusion<T>
}
