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

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Scope as JmhScope
import org.openjdk.jmh.annotations.Warmup
import se.oyabun.aelv.Many
import se.oyabun.aelv.One
import se.oyabun.aelv.Sinks
import se.oyabun.aelv.await
import se.oyabun.aelv.concatMap
import se.oyabun.aelv.drain
import se.oyabun.aelv.filter
import se.oyabun.aelv.flatMap
import se.oyabun.aelv.flatMapMany
import se.oyabun.aelv.flatMapSequential
import se.oyabun.aelv.fold
import se.oyabun.aelv.map
import se.oyabun.aelv.merge
import se.oyabun.aelv.rightOrThrow
import se.oyabun.aelv.take
import se.oyabun.aelv.toList
import se.oyabun.aelv.Verify

@BenchmarkMode(Mode.Throughput)
@State(JmhScope.Thread)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
open class AelvBenchmark {

    @Param("1000", "10000")
    var size: Int = 1000

    private val scope = CoroutineScope(SupervisorJob())

    private fun <T> run(closure: suspend () -> T): T = runBlocking { closure() }

    @Benchmark
    fun many_baseline_toList(): Int =
        run { Many.range(0, size).toList().await() }.rightOrThrow().size

    @Benchmark
    fun many_map_toList(): Int =
        run { Many.range(0, size).map { it * 2 }.toList().await() }.rightOrThrow().size

    @Benchmark
    fun many_filter_toList(): Int =
        run { Many.range(0, size).filter { it % 2 == 0 }.toList().await() }.rightOrThrow().size

    @Benchmark
    fun many_take_toList(): Int =
        run { Many.range(0, size).take(size.toLong() / 2).toList().await() }.rightOrThrow().size

    @Benchmark
    fun many_chain_map_filter_take(): Int =
        run {
            Many.range(0, size)
                .map { it * 2 }
                .filter { it % 4 == 0 }
                .take(size.toLong() / 4)
                .toList().await()
        }.rightOrThrow().size

    @Benchmark
    fun many_concatMap_toList(): Int =
        run {
            Many.range(0, size / 10)
                .concatMap { i -> Many.items(i, i + 1, i + 2) }
                .toList().await()
        }.rightOrThrow().size

    @Benchmark
    fun many_flatMap_sequential_toList(): Int =
        run {
            Many.range(0, size / 10)
                .flatMapSequential { i -> Many.items(i, i + 1, i + 2) }
                .toList().await()
        }.rightOrThrow().size

    @Benchmark
    fun many_flatMap_concurrent_toList(): Int =
        run {
            Many.range(0, size / 10)
                .flatMap(concurrency = 256) { i -> Many.items(i, i + 1, i + 2) }
                .toList().await()
        }.rightOrThrow().size

    @Benchmark
    fun many_fold_sum(): Long =
        run {
            Many.range(0, size)
                .fold(0L) { acc, i -> acc + i }
                .await()
        }.rightOrThrow()

    @Benchmark
    fun one_baseline_get(): Int =
        run { One.single(42).await() }.rightOrThrow()

    @Benchmark
    fun one_map_get(): Int =
        run { One.single(42).map { it * 2 }.await() }.rightOrThrow()

    @Benchmark
    fun one_flatMapMany_toList(): Int =
        run {
            One.single(size)
                .flatMapMany { n -> Many.range(0, n) }
                .toList().await()
        }.rightOrThrow().size

    @Benchmark
    fun sink_broadcast_single_subscriber(): Int {
        val sink = Sinks.broadcast<Int>()
        return run {
            val job = scope.launch { sink.asMany().toList().await() }
            repeat(size) { sink.emit(it) }
            sink.complete()
            job.join()
            size
        }
    }

    @Benchmark
    fun sink_replay_four_subscribers(): Int {
        val sink = Sinks.replay<Int>()
        val s1 = sink.asMany().take(size.toLong())
        val s2 = sink.asMany().take(size.toLong())
        val s3 = sink.asMany().take(size.toLong())
        val s4 = sink.asMany().take(size.toLong())
        Verify.that(
            merge(s1, s2, s3, s4)
                .toList()
                .doOnSubscribe {
                    repeat(size) { sink.emit(it) }
                    sink.complete()
                }
        )
        .assertNext { list -> check(list.size == size * 4) }
        .completes()
        return size
    }
}
