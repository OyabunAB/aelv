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
package se.oyabun.aelv.benchmarks

/**
 * Shared IO workload parameters used across all benchmark libraries.
 *
 * Each benchmark simulates `OUTER_COUNT` independent IO calls, each taking
 * `IO_DELAY_MS` milliseconds and producing `ITEMS_PER_CALL` result items.
 *
 * Sequential (concatMap):  OUTER_COUNT × IO_DELAY_MS ms per run
 * Concurrent (flatMap):    ~IO_DELAY_MS ms per run (all calls in parallel)
 */
internal object Workload {
    /** Simulated IO latency per inner call. */
    const val IO_DELAY_MS   = 1L

    /** Number of items emitted per inner call. */
    const val ITEMS_PER_CALL = 3
}
