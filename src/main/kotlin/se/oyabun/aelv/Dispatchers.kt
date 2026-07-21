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

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext

/**
 * A named [CoroutineDispatcher] created and managed by aelv.
 *
 * Access via [Dispatchers.cpu] and [Dispatchers.io].
 */
class AelvDispatcher internal constructor(
    val name: String,
    private val delegate: CoroutineDispatcher,
) : CoroutineDispatcher() {
    override fun dispatch(context: CoroutineContext, block: Runnable) = delegate.dispatch(context, block)
    override fun toString() = name
}

internal object AelvDispatchers {
    val cpu: AelvDispatcher = run {
        val index = AtomicInteger(0)
        AelvDispatcher(
            name = "aelv-cpu",
            delegate = Executors.newFixedThreadPool(Aelv.cpuPoolSize) { r ->
                Thread(r, "aelv-cpu-${index.incrementAndGet()}").also { it.isDaemon = true }
            }.asCoroutineDispatcher(),
        )
    }

    val io: AelvDispatcher = run {
        val factory = Thread.ofVirtual().name("aelv-io-", 0).factory()
        AelvDispatcher(
            name = "aelv-io",
            delegate = Executors.newThreadPerTaskExecutor(factory).asCoroutineDispatcher(),
        )
    }
}

/** CPU-bound dispatcher — fixed pool of platform threads sized to [Runtime.availableProcessors], named `aelv-cpu-N`. */
val Dispatchers.cpu: AelvDispatcher get() = AelvDispatchers.cpu

/** IO-bound dispatcher — one virtual thread per task, named `aelv-io-N`. Use with [subscribeOn] for blocking work. */
val Dispatchers.io: AelvDispatcher get() = AelvDispatchers.io
