import type { Clock } from '../clock/Clock.js';

/**
 * The wire form of an incarnation: an opaque scalar carrying a **total order**. Ordering
 * is the whole mechanism — the gate must be able to reject the past, not merely notice
 * that a lifetime differs, or a delayed message from a dead incarnation would be adopted
 * and resurrect stale state. That rules out unordered identifiers (a random UUID).
 */
export type IncarnationValue = number | string;

/**
 * A node's restart epoch `ι` (eq. 15): which process lifetime a version counter belongs to.
 *
 * `Σ` is keyed by sender address, so an incoming `ι` is only ever compared against the
 * previous `ι` **from that same node**. Comparisons are always intra-node, which is what
 * makes a purely node-local source (a boot timestamp) legitimate here: no cross-node clock
 * agreement is needed and the drift bound `ρ̄` never enters the gate.
 *
 * Implementations must yield values that strictly increase across restarts of the same
 * node. Comparing values issued by *different* implementations is undefined — §2.1 assumes
 * every node runs the same engine version.
 */
export interface Incarnation {
    /** This engine instance's lifetime id, stamped on every outbound message. */
    readonly value: IncarnationValue;
    /** Total order over wire values: `> 0` iff `a` is a strictly later lifetime than `b`. */
    compare(a: IncarnationValue, b: IncarnationValue): number;
}

/**
 * Default: the node's local clock reading at engine construction. Survives the cases that
 * matter (a process restart takes far longer than a clock tick) and degrades safely in the
 * ones that do not — if the clock steps backwards during downtime the node's SHAREs are
 * ignored until its record ages out, which is the pre-amendment TTL route, never worse.
 */
export class TimestampIncarnation implements Incarnation {
    readonly value: IncarnationValue;

    constructor(clock: Clock) {
        this.value = clock.now();
    }

    compare(a: IncarnationValue, b: IncarnationValue): number {
        if (a === b) return 0;
        return a < b ? -1 : 1;
    }
}

/**
 * A fixed value that never orders one lifetime above another, so eq. (15) always falls
 * through to the eq. (3) version gates — i.e. the protocol exactly as it behaved before the
 * amendment. Its purpose is experimental: it makes the pre-amendment arm reachable without
 * a feature flag, so the paired §7.3 runs differ only in which `Incarnation` is injected.
 */
export class StaticIncarnation implements Incarnation {
    constructor(readonly value: IncarnationValue) {}

    compare(_a: IncarnationValue, _b: IncarnationValue): number {
        return 0;
    }
}
