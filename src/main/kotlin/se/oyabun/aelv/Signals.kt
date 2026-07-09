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
        data class  Error(val cause: AelvException)   : Upstream<Nothing>, Terminal
    }

    /** Signals flowing from subscriber to publisher. */
    sealed interface Downstream : Signal {
        /** Requests [n] additional items from the publisher (RS spec §3.9: `n` must be positive). */
        data class  Request(val n: Long) : Downstream
        /** Tells the publisher to stop sending items and release any resources. */
        data object Cancel               : Downstream, Terminal
    }

    /** Supertype of [Upstream.Complete], [Upstream.Error], and [Downstream.Cancel]. */
    sealed interface Terminal : Signal
}
