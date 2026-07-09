package se.oyabun.aelv

import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.assertEquals
import se.oyabun.aelv.InvalidDemandException
import se.oyabun.aelv.Many

class VerifyTest {

    @Test
    fun `emitsNext matches items in order`() {
        Verify.that(Many.of(1, 2, 3))
            .emitsNext(1, 2, 3)
            .completesNormally()
    }

    @Test
    fun `emitsNext fails on wrong value`() {
        assertFailsWith<IllegalStateException> {
            Verify.that(Many.of(1, 2, 3))
                .emitsNext(1, 99, 3)
                .completesNormally()
        }
    }

    @Test
    fun `emitsCount accepts n items`() {
        Verify.that(Many.of(1, 2, 3))
            .emitsCount(3)
            .completesNormally()
    }

    @Test
    fun `matchesNext asserts item`() {
        Verify.that(Many.of(42))
            .matchesNext { assertTrue(it > 40) }
            .completesNormally()
    }

    @Test
    fun `runs executes side effect between steps`() {
        var fired = false
        Verify.that(Many.of(1, 2))
            .emitsNext(1)
            .runs { fired = true }
            .emitsNext(2)
            .completesNormally()
        assertTrue(fired)
    }

    @Test
    fun `thenCancels stops subscription`() {
        Verify.that(Many.of(1, 2, 3))
            .emitsNext(1)
            .thenCancels()
            .verify()
    }

    @Test
    fun `completesNormally fails when stream errors`() {
        assertFailsWith<AssertionError> {
            Verify.that(Many.error<Int>(InvalidDemandException(-1)))
                .completesNormally()
        }
    }

    @Test
    fun `completesWithError returns the cause`() {
        val cause = InvalidDemandException(-1)
        val error = Verify.that(Many.error<Int>(cause)).completesWithError()
        assertEquals(cause, error)
    }

    @Test
    fun `isSubscribed is a no-op checkpoint`() {
        Verify.that(Many.of(1))
            .isSubscribed()
            .emitsNext(1)
            .completesNormally()
    }

    @Test
    fun `empty publisher completesNormally`() {
        Verify.that(Many.empty<Int>())
            .completesNormally()
    }
}
