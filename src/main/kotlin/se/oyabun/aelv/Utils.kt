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
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow

internal object Unset

internal fun Any.notUnset(): Boolean = this !== Unset

internal fun Any.isError(): Boolean = this is Exception
internal fun Any.asError(): Exception = this as Exception

internal fun rethrow(issue: Exception): Nothing = throw issue

fun Exception.leftUnlessCancelled(): Either<Exception, Nothing> =
    if (this is CancellationException) throw this else this.left()

internal suspend fun <T> Flow<T>.collectCancelling(block: suspend (T) -> Boolean) =
    try { coroutineScope { collect { if (!block(it)) cancel() } } } catch (_: CancellationException) {}

internal suspend inline fun <C : AutoCloseable, V> C.using(block: suspend (C) -> V): V =
    try { block(this) } finally { close() }
