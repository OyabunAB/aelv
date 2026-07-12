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

import org.openjdk.jmh.annotations.*
import zio.Runtime
import zio.Trace
import zio.Unsafe
import zio.stream.ZStream

/**
 * ZIO 2.x ZStream benchmarks.
 *
 * ZIO is a Scala library; called here via Java interop.
 * By-name parameters in the Scala API become [scala.Function0] lambdas.
 * Implicit [Trace] parameters become explicit [java.lang.Object] arguments,
 * filled with [Trace.empty].
 *
 * Results are obtained by running [ZStream.runCount] (returns a [zio.ZIO]
 * effect) synchronously via [Unsafe.unsafe] + [Runtime.default].
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
open class ZIOBenchmark {

    @Param("1000", "10000")
    var size: Int = 1000

    private val runtime = Runtime.default()
    private val trace   = Trace.empty()

    /** Run a [zio.ZIO] effect synchronously and return its result. */
    @Suppress("UNCHECKED_CAST")
    private fun <A> run(effect: zio.ZIO<Any, *, A>): A =
        Unsafe.unsafe { u ->
            (effect as zio.ZIO<Any, Throwable, A>)
                .let { runtime.unsafe().run(it, trace, u) }
                .getOrThrowFiberFailure(u) as A
        }

    private fun count(stream: ZStream<Any, *, *>): Long =
        run(stream.runCount(trace)) as Long

    @Benchmark
    fun baseline_toList(): Long =
        count(ZStream.range({ 0 }, { size }, { 1 }, trace))

    @Benchmark
    fun map_toList(): Long =
        count(ZStream.range({ 0 }, { size }, { 1 }, trace)
            .map({ v -> (v as Int) * 2 }, trace))

    @Benchmark
    fun filter_toList(): Long =
        count(ZStream.range({ 0 }, { size }, { 1 }, trace)
            .filter({ v -> (v as Int) % 2 == 0 }, trace))

    @Benchmark
    fun take_toList(): Long =
        count(ZStream.range({ 0 }, { size }, { 1 }, trace)
            .take({ (size / 2).toLong() as Any }, trace))

    @Benchmark
    fun chain_map_filter_take(): Long =
        count(ZStream.range({ 0 }, { size }, { 1 }, trace)
            .map({ v -> (v as Int) * 2 }, trace)
            .filter({ v -> (v as Int) % 4 == 0 }, trace)
            .take({ (size / 4).toLong() as Any }, trace))

    @Benchmark
    fun concatMap_toList(): Long =
        count(ZStream.range({ 0 }, { size / 10 }, { 1 }, trace)
            .flatMap({ v ->
                val i = v as Int
                ZStream.range({ i }, { i + 3 }, { 1 }, trace)
            }, trace))

    @Benchmark
    fun flatMap_toList(): Long =
        count(ZStream.range({ 0 }, { size / 10 }, { 1 }, trace)
            .flatMap({ v ->
                val i = v as Int
                ZStream.range({ i }, { i + 3 }, { 1 }, trace)
            }, trace))

    @Benchmark
    fun fold_sum(): Long =
        run(ZStream.range({ 0 }, { size }, { 1 }, trace)
            .runFold({ 0L }, { acc: Any, v: Any -> (acc as Long) + (v as Int).toLong() }, trace))
            as Long
}
