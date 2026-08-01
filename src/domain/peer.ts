import type { IncarnationValue } from '../incarnation/Incarnation.js';
import type { Address, ReducerName } from './address.js';

export interface PeerRecord<Payload> {
    payload: Payload;
    version: number;
    /** The sender's restart epoch `ι`; absent for a peer that does not speak incarnations. */
    inc?: IncarnationValue | undefined;
    lastSeenAt: number;
}

/**
 * Read-only view of one `Σ` slot: who we know, at what version and lifetime, and when we
 * last heard from them. This is the object eq. (3) gates on and eq. (7) is defined over —
 * the derived view lags it by the debounce window, so anything reasoning about detection or
 * admission must read `Σ` rather than the view.
 */
export interface PeerSnapshot {
    addr: Address;
    version: number;
    inc?: IncarnationValue | undefined;
    lastSeenAt: number;
}

/**
 * Peers a TTL sweep removed from one reducer's `Σ`. Reported per reducer because `Σ` is
 * per `(node, reducer, peer)`: one sweep can evict a peer from one reducer while another
 * still holds it.
 */
export interface PeerEviction {
    reducer: ReducerName;
    addrs: Address[];
}
