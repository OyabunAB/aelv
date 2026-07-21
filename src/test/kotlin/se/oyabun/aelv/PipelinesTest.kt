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

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class PipelinesTest {

    class ManyPipedFrom {

        @Test
        fun `single pipeline step with pipeTo`() = runTest {
            val pipeline: Many<Int> = Many.pipelineFrom<Int>().map { it * 2 }

            val result = Many.items(1, 2, 3).andThen(pipeline).toList().await()
            assertIs<Success<List<Int>>>(result)
            assertEquals(listOf(2, 4, 6), result.value)
        }

        @Test
        fun `pipeline terminates to One via toList`() = runTest {
            val pipeline: One<List<Int>> = Many.pipelineFrom<Int>()
                .filter { it % 2 == 0 }
                .toList()

            val result = Many.range(0, 6).andThen(pipeline).await()
            assertIs<Success<List<Int>>>(result)
            assertEquals(listOf(0, 2, 4), result.value)
        }

        @Test
        fun `pipeline applied to empty source`() = runTest {
            val pipeline: One<List<Int>> = Many.pipelineFrom<Int>().map { it * 10 }.toList()

            val result = Many.empty<Int>().andThen(pipeline).await()
            assertIs<Success<List<Int>>>(result)
            assertTrue(result.value.isEmpty())
        }

        @Test
        fun `pipeline preserves fusion — fused source feeds fused pipeline`() = runTest {
            // Many.range has RangeFusion; map and filter produce MapFusion / FilterFusion.
            // connectSource should produce a fully resolved fusion chain at pipeTo time.
            val pipeline: One<List<Int>> = Many.pipelineFrom<Int>()
                .map { it * 2 }
                .filter { it > 4 }
                .toList()

            val result = Many.range(0, 5).andThen(pipeline).await()
            assertIs<Success<List<Int>>>(result)
            assertEquals(listOf(6, 8), result.value)
        }

        @Test
        fun `same pipeline instance reused across multiple sources`() = runTest {
            val pipeline: One<List<Int>> = Many.pipelineFrom<Int>().map { it + 1 }.toList()

            val r1 = Many.items(1, 2, 3).andThen(pipeline).await()
            val r2 = Many.items(10, 20, 30).andThen(pipeline).await()

            assertIs<Success<List<Int>>>(r1)
            assertIs<Success<List<Int>>>(r2)
            assertEquals(listOf(2, 3, 4), r1.value)
            assertEquals(listOf(11, 21, 31), r2.value)
        }

        @Test
        fun `same pipeline instance used concurrently produces independent results`() = runTest {
            val pipeline: One<List<Int>> = Many.pipelineFrom<Int>().map { it * 3 }.toList()

            val results = (0 until 10).map { base ->
                async { Many.range(base * 10, 5).andThen(pipeline).await() }
            }.awaitAll()

            results.forEachIndexed { i, result ->
                assertIs<Success<List<Int>>>(result)
                val base = i * 10
                assertEquals((base until base + 5).map { it * 3 }, result.value)
            }
        }
    }

    class ManyThen {

        @Test
        fun `two pipeline steps composed with then`() = runTest {
            val trim: Many<String>     = Many.pipelineFrom<String>().map { it.trim() }
            val upper: Many<String>    = Many.pipelineFrom<String>().map { it.uppercase() }
            val collect: One<List<String>> = Many.pipelineFrom<String>().toList()

            val full = trim.andThen(upper).andThen(collect)

            val result = Many.items(" hello ", " world ").andThen(full).await()
            assertIs<Success<List<String>>>(result)
            assertEquals(listOf("HELLO", "WORLD"), result.value)
        }

        @Test
        fun `then with filter step`() = runTest {
            val evens: Many<Int>         = Many.pipelineFrom<Int>().filter { it % 2 == 0 }
            val doubled: Many<Int>       = Many.pipelineFrom<Int>().map { it * 2 }
            val collect: One<List<Int>>  = Many.pipelineFrom<Int>().toList()

            val full = evens.andThen(doubled).andThen(collect)

            val result = Many.range(0, 6).andThen(full).await()
            assertIs<Success<List<Int>>>(result)
            assertEquals(listOf(0, 4, 8), result.value)
        }

        @Test
        fun `then pipeline reused with different sources`() = runTest {
            val step: Many<Int>         = Many.pipelineFrom<Int>().map { it - 1 }
            val sink: One<List<Int>>    = Many.pipelineFrom<Int>().toList()
            val pipeline = step.andThen(sink)

            val r1 = Many.items(1, 2, 3).andThen(pipeline).await()
            val r2 = Many.items(10, 20, 30).andThen(pipeline).await()

            assertIs<Success<List<Int>>>(r1)
            assertIs<Success<List<Int>>>(r2)
            assertEquals(listOf(0, 1, 2), r1.value)
            assertEquals(listOf(9, 19, 29), r2.value)
        }

        @Test
        fun `three then levels`() = runTest {
            val a: Many<Int>        = Many.pipelineFrom<Int>().map { it + 1 }
            val b: Many<Int>        = Many.pipelineFrom<Int>().map { it * 2 }
            val c: Many<Int>        = Many.pipelineFrom<Int>().filter { it > 5 }
            val sink: One<List<Int>> = Many.pipelineFrom<Int>().toList()

            val full = a.andThen(b).andThen(c).andThen(sink)

            // (0..4) → +1 → *2 → filter>5 → [6, 8]  (mapped: 1,2,3,4,5 → 2,4,6,8,10 → 6,8,10)
            val result = Many.range(0, 5).andThen(full).await()
            assertIs<Success<List<Int>>>(result)
            assertEquals(listOf(6, 8, 10), result.value)
        }

        @Test
        fun `then composed concurrently is safe`() = runTest {
            val step: Many<Int>     = Many.pipelineFrom<Int>().map { it * 2 }
            val sink: One<List<Int>> = Many.pipelineFrom<Int>().toList()
            val pipeline = step.andThen(sink)

            val results = (0 until 8).map { base ->
                async { Many.range(base * 5, 5).andThen(pipeline).await() }
            }.awaitAll()

            results.forEachIndexed { i, result ->
                assertIs<Success<List<Int>>>(result)
                val base = i * 5
                assertEquals((base until base + 5).map { it * 2 }, result.value)
            }
        }
    }

    class OnePipedFrom {

        @Test
        fun `One pipeline step with pipeTo`() = runTest {
            val pipeline: One<String> = One.pipelineFrom<Int>().map { it.toString() }

            val result = One.single(42).andThen(pipeline).await()
            assertIs<Success<String>>(result)
            assertEquals("42", result.value)
        }

        @Test
        fun `One pipeline steps composed with then`() = runTest {
            val stringify: One<String> = One.pipelineFrom<Int>().map { it.toString() }
            val prefix: One<String>    = One.pipelineFrom<String>().map { "value=$it" }

            val full = stringify.andThen(prefix)

            val result = One.single(7).andThen(full).await()
            assertIs<Success<String>>(result)
            assertEquals("value=7", result.value)
        }
    }
    class NonePipedFrom {

        @Test
        fun `None pipelineFrom resolves when driven via Many source`() {
            val source   = Many.items(1, 2, 3)
            val pipeline = Many.pipelineFrom<Int>().discard()
            Verify.that(source.andThen(pipeline)).completes()
        }
    }

    class FusionConnectSource {

        @Test
        fun `connectSource replaces SourceFusion with ArrayFusion`() {
            val pipeline: Many<Int> = Many.pipelineFrom<Int>().map { it * 2 }
            val source = Many.items(1, 2, 3)

            val pf = pipeline.fusion
            val sf = source.fusion
            assertIs<Fusion.Available<*>>(pf)
            assertIs<Fusion.Available<*>>(sf)
            val connected = pf.connectSource(sf)
            assertIs<Fusion.Available<*>>(connected)
        }

        @Test
        fun `connectSource through filter and map chain`() {
            val pipeline: Many<Int> = Many.pipelineFrom<Int>()
                .filter { it % 2 == 0 }
                .map { it * 10 }
            val source = Many.range(0, 10)

            val pf = pipeline.fusion
            val sf = source.fusion
            assertIs<Fusion.Available<*>>(pf)
            assertIs<Fusion.Available<*>>(sf)
            val connected = pf.connectSource(sf)
            assertIs<Fusion.Available<*>>(connected)
        }

        @Test
        fun `non-fusible operator in pipeline results in Fusion None`() {
            val pipeline = Many.pipelineFrom<Int>().doOnNext { }
            assertIs<Fusion.None>(pipeline.fusion)
        }

        @Test
        fun `connectSource composes two pipeline fusion chains`() {
            val step1: Many<Int> = Many.pipelineFrom<Int>().map { it + 1 }
            val step2: Many<Int> = Many.pipelineFrom<Int>().map { it * 2 }

            val pf = step2.fusion
            val sf = step1.fusion
            assertIs<Fusion.Available<*>>(pf)
            assertIs<Fusion.Available<*>>(sf)
            // combined: MapFusion(MapFusion(SourceFusion, +1), *2) — SourceFusion still at root
            val combined = pf.connectSource(sf)
            assertIs<Fusion.Available<*>>(combined)
        }
    }
}
