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

import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import org.openjdk.jmh.annotations.*

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
open class MutinyBenchmark {

    @Param("1000", "10000")
    var size: Int = 1000

    @Benchmark
    fun many_baseline_toList(): Int =
        Multi.createFrom().range(0, size).collect().asList().await().indefinitely().size

    @Benchmark
    fun many_map_toList(): Int =
        Multi.createFrom().range(0, size).map { it * 2 }.collect().asList().await().indefinitely().size

    @Benchmark
    fun many_filter_toList(): Int =
        Multi.createFrom().range(0, size).filter { it % 2 == 0 }.collect().asList().await().indefinitely().size

    @Benchmark
    fun many_take_toList(): Int =
        Multi.createFrom().range(0, size).select().first(size.toLong() / 2).collect().asList().await().indefinitely().size

    @Benchmark
    fun many_chain_map_filter_take(): Int =
        Multi.createFrom().range(0, size)
            .map { it * 2 }
            .filter { it % 4 == 0 }
            .select().first(size.toLong() / 4)
            .collect().asList().await().indefinitely().size

    @Benchmark
    fun many_concatMap_toList(): Int =
        Multi.createFrom().range(0, size / 10)
            .onItem().transformToMultiAndConcatenate { i -> Multi.createFrom().items(i, i + 1, i + 2) }
            .collect().asList().await().indefinitely().size

    @Benchmark
    fun many_flatMap_toList(): Int =
        Multi.createFrom().range(0, size / 10)
            .onItem().transformToMultiAndMerge { i -> Multi.createFrom().items(i, i + 1, i + 2) }
            .collect().asList().await().indefinitely().size

    private fun ioWork(i: Int): io.smallrye.mutiny.Multi<Int> =
        Uni.createFrom().nullItem<Void>()
            .onItem().delayIt().by(java.time.Duration.ofMillis(Workload.IO_DELAY_MS))
            .onItem().transformToMulti { Multi.createFrom().range(i, i + Workload.ITEMS_PER_CALL) }

    @Benchmark
    fun many_flatMap_io(): Int =
        Multi.createFrom().range(0, size / 10)
            .onItem().transformToMultiAndMerge { i -> ioWork(i) }
            .collect().asList().await().indefinitely().size

    @Benchmark
    fun many_concatMap_io(): Int =
        Multi.createFrom().range(0, size / 10)
            .onItem().transformToMultiAndConcatenate { i -> ioWork(i) }
            .collect().asList().await().indefinitely().size

    @Benchmark
    fun many_fold_sum(): Long =
        Multi.createFrom().range(0, size)
            .collect().with(java.util.stream.Collectors.summingLong { it.toLong() })
            .await().indefinitely()

    @Benchmark
    fun one_baseline_get(): Int =
        Uni.createFrom().item(42).await().indefinitely()!!

    @Benchmark
    fun one_map_get(): Int =
        Uni.createFrom().item(42).map { it * 2 }.await().indefinitely()!!
}
