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

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Global configuration for aelv defaults.
 *
 * All settings must be applied before first use — before any [Sink] is created,
 * before [subscribe] is called, before [Dispatchers.cpu] is first accessed, etc.
 *
 * ```kotlin
 * Aelv.loggingEnabled = true
 * Aelv.bufferSize     = 8192
 * Aelv.cpuPoolSize    = 4
 * ```
 */
object Aelv {

    /** Enable aelv's built-in SLF4J logging. Off by default — no logging calls are made unless enabled. */
    var loggingEnabled: Boolean = false

    /** Default ring buffer size for [Sink] variants. */
    var bufferSize: Int = 4096

    /**
     * Maximum buffer size for slow sink subscribers promoted to their own queue.
     * Defaults to 4× [bufferSize]. Set explicitly to override independently of [bufferSize].
     */
    var maxSlowBuffer: Int
        get() = _maxSlowBuffer ?: bufferSize * 4
        set(value) { _maxSlowBuffer = value }
    private var _maxSlowBuffer: Int? = null

    /** Default prefetch window for [Many.subscribe] — initial demand and replenishment batch size. */
    var prefetch: Long = 256L

    /**
     * Default timeout for [Verify] terminal assertions.
     * Increase in slow CI environments.
     */
    var verifyTimeout: Duration = 5.seconds

    /**
     * Number of threads in the [Dispatchers.cpu] pool.
     * Defaults to [Runtime.availableProcessors] — override in containers where
     * the host CPU count does not reflect the actual container limit.
     */
    var cpuPoolSize: Int = Runtime.getRuntime().availableProcessors()
}
