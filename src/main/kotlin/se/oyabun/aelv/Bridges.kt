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
