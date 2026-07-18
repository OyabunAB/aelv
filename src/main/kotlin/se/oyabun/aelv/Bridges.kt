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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import org.reactivestreams.Publisher

/** Bridges this [Flow] to a [Many], collecting the flow on each subscription. */
fun <T : Any> Flow<T>.asMany(): Many<T> = Many.from(this)

/** Bridges this [Flow] to a [One] by taking its first emitted item. */
fun <T : Any> Flow<T>.asOne(): One<T> = One.defer {
    firstOrNull() ?: throw NoSuchElementException()
}

/** Bridges this [Flow] to a [None] by draining all items. */
fun <T : Any> Flow<T>.asNone(): None<T> = None.defer { collect { } }

/** Bridges this Reactive Streams [Publisher] to a [Many]. */
fun <T : Any> Publisher<T>.asMany(): Many<T> = Many.from(this)

/** Bridges this Reactive Streams [Publisher] to a [One] by taking its first emitted item. */
fun <T : Any> Publisher<T>.asOne(): One<T> = One.from(this)

/** Bridges this Reactive Streams [Publisher] to a [None] by draining all items. */
fun <T : Any> Publisher<T>.asNone(): None<T> = None.from(this)

/**
 * Takes the first item from this [Many] and wraps it in a [Maybe].
 *
 * Uses [Many.source] directly rather than the [Publisher] bridge, preserving the
 * coroutine context of the caller — including any context elements installed via
 * [kotlinx.coroutines.withContext].
 *
 * If the stream completes without emitting, the [Maybe] is empty.  If it emits at least one item,
 * the [Maybe] is present with that item and the remaining upstream items are discarded (the
 * subscription is cancelled).  Errors are forwarded as [Maybe] errors.
 */
fun <T : Any> Many<T>.firstMaybe(): Maybe<T> = Maybe { onNext, onComplete, onError ->
    var emitted = false
    source(
        { value ->
            if (!emitted) {
                emitted = true
                onNext(value)
                onComplete()
                Signal.Downstream.Cancel
            } else {
                Signal.Downstream.Cancel
            }
        },
        { if (!emitted) onComplete() },
        { issue -> onError(issue) },
    )
}
