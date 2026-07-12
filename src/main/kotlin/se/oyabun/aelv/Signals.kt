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

/**
 * Complete signal vocabulary for the aelv reactive streams protocol.
 *
 * Two directions, one hierarchy:
 *
 * [Upstream] — publisher → subscriber (data flow)
 * - [Upstream.Next] maps to `onNext(T)`
 * - [Upstream.Complete] maps to `onComplete()` — also [Terminal]
 * - [Upstream.Error] maps to `onError(Throwable)` — also [Terminal]
 *
 * [Downstream] — subscriber → publisher (demand / flow control)
 * - [Downstream.Request] maps to `request(n)`
 * - [Downstream.Cancel] maps to `cancel()` — also [Terminal]
 *
 * [Terminal] — supertype of [Upstream.Complete], [Upstream.Error], and [Downstream.Cancel].
 * Used by `doFinally` to express all three ways a stream can end.
 */
sealed interface Signal {

    /** Signals flowing from publisher to subscriber. */
    sealed interface Upstream<out T : Any> : Signal {
        /** Carries the next item [value] to be delivered to the subscriber. */
        data class  Next<out T : Any>(val value: T)  : Upstream<T>
        /** Signals that the publisher has finished emitting items normally. */
        data object Complete                          : Upstream<Nothing>, Terminal
        /** Signals that the publisher encountered an error described by [cause]. */
        data class  Error(val cause: Exception)               : Upstream<Nothing>, Terminal
    }

    /** Signals flowing from subscriber to publisher. */
    sealed interface Downstream : Signal {
        /** Requests the next item from the publisher. */
        data object Request : Downstream
        /** Tells the publisher to stop sending items and release any resources. */
        data object Cancel  : Downstream, Terminal
    }

    /** Supertype of [Upstream.Complete], [Upstream.Error], and [Downstream.Cancel]. */
    sealed interface Terminal : Signal
}
