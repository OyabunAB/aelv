package se.oyabun.aelv

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import org.reactivestreams.Publisher

fun <T : Any> Flow<T>.asMany(): Many<T> = Many.from(this)

fun <T : Any> Flow<T>.asOne(): One<T> = One.defer {
    firstOrNull() ?: throw NoSuchElementException()
}

fun <T : Any> Flow<T>.asNone(): None<T> = None.defer { collect { } }

fun <T : Any> Publisher<T>.asMany(): Many<T> = Many.from(this)

fun <T : Any> Publisher<T>.asOne(): One<T> = One.from(this)

fun <T : Any> Publisher<T>.asNone(): None<T> = None.from(this)
