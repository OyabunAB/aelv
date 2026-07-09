package se.oyabun.aelv

/**
 * Complete signal vocabulary for the aelv reactive streams protocol.
 *
 * Two directions, one hierarchy:
 *
 *   Upstream<T>  — publisher → subscriber (data flow)
 *     Next(value)   maps to onNext(T)
 *     Complete      maps to onComplete()        — also Terminal
 *     Error(cause)  maps to onError(Throwable)  — also Terminal
 *
 *   Downstream   — subscriber → publisher (demand / flow control)
 *     Request(n)    maps to request(n)
 *     Cancel        maps to cancel()            — also Terminal
 *
 *   Terminal     — supertype of Complete, Error, Cancel.
 *                  Used by doFinally to express all three ways a stream can end.
 */
sealed interface Signal {

    sealed interface Upstream<out T : Any> : Signal {
        data class  Next<out T : Any>(val value: T)  : Upstream<T>
        data object Complete                          : Upstream<Nothing>, Terminal
        data class  Error(val cause: AelvException)   : Upstream<Nothing>, Terminal
    }

    sealed interface Downstream : Signal {
        data class  Request(val n: Long) : Downstream
        data object Cancel               : Downstream, Terminal
    }

    sealed interface Terminal : Signal
}
