// End-to-end crash-recovery: the version-shadow failure mode and the incarnation gate
// that closes it. Both arms run the identical scenario and differ only in which
// `Incarnation` is injected, which is what makes them a paired comparison.
//
// Scenario (2 nodes, virtual time): `j` crashes without CLOSE while `a` still holds its
// record, then recovers quickly under the same address with fresh volatile state
// (Σ = ∅, v_out = 0, §2.2). Its version counter re-treads the same two steps and lands
// back on v_pre, so before the amendment its own stale record shadowed it indefinitely.
import { describe, expect, it } from 'vitest';

import type { Clock, TimerHandle } from '../../src/clock/Clock.js';
import { FakeClock } from '../../src/clock/Clock.js';
import type { Address } from '../../src/domain/address.js';
import type { Context } from '../../src/domain/context.js';
import type { WireMessage } from '../../src/domain/message.js';
import type { Engine } from '../../src/engine/Engine.js';
import { createEngine } from '../../src/engine/Engine.js';
import type { Incarnation } from '../../src/incarnation/Incarnation.js';
import { StaticIncarnation } from '../../src/incarnation/Incarnation.js';
import type { Reducer } from '../../src/reducer/Reducer.js';
import { defineReducer } from '../../src/reducer/Reducer.js';
import type { Slan, Unsubscribe } from '../../src/slan/Slan.js';

/**
 * Each engine instance carries an epoch tag, and a node's view reports only the tags of
 * the peers currently contributing to it. Deliberately NOT a union of peers' whole views:
 * a union can never forget, so a dead incarnation's tag would linger for reasons that have
 * nothing to do with Σ admission. Taking only each payload's own `tag` keeps the view a
 * faithful readout of "whose perspective is in Σ right now".
 */
interface View {
    tag: string;
    peers: string[];
}

const epochReducer = (tag: string): Reducer<unknown, View, Context> => {
    const collect = {
        aggregate: (batch: View[]): View => ({
            tag,
            peers: [...new Set(batch.map((b) => b.tag))].sort(),
        }),
    };
    return defineReducer<View, View, Context>('epoch')
        .status((p) => p.setAggregator(collect))
        .share((p) => p.setAggregator(collect))
        .close((p) => p.setAggregator({ aggregate: (_d: Address[], _c, prev) => prev ?? { tag, peers: [] } }))
        .setTranslator({
            translate: (s, prev) => ({ value: s, changed: JSON.stringify(s) !== JSON.stringify(prev) }),
        }) as Reducer<unknown, View, Context>;
};

/** In-memory bus with a fixed per-hop delay and crash-stop kill (no CLOSE, both directions). */
class DelayBus {
    private readonly handlers = new Map<Address, (m: WireMessage, from: Address) => void>();
    private readonly dead = new Set<Address>();

    constructor(
        private readonly clock: FakeClock,
        private readonly delayMs: number,
    ) {}

    register(addr: Address, h: (m: WireMessage, from: Address) => void): void {
        this.handlers.set(addr, h);
    }
    unregister(addr: Address): void {
        this.handlers.delete(addr);
    }
    kill(addr: Address): void {
        this.dead.add(addr);
    }
    revive(addr: Address): void {
        this.dead.delete(addr);
    }
    broadcast(from: Address, msg: WireMessage): void {
        if (this.dead.has(from)) return;
        for (const addr of [...this.handlers.keys()]) if (addr !== from) this.deliver(addr, from, msg);
    }
    sendTo(to: Address, from: Address, msg: WireMessage): void {
        if (this.dead.has(from)) return;
        this.deliver(to, from, msg);
    }
    private deliver(to: Address, from: Address, msg: WireMessage): void {
        this.clock.setTimer(() => {
            if (this.dead.has(to) || this.dead.has(from)) return;
            this.handlers.get(to)?.(msg, from);
        }, this.delayMs);
    }
}

class DelaySlan implements Slan {
    private readonly handlers = new Set<(m: WireMessage, from: Address) => void>();
    constructor(
        readonly address: Address,
        private readonly bus: DelayBus,
    ) {}
    init(): Promise<void> {
        this.bus.register(this.address, (m, from) => {
            for (const h of this.handlers) h(m, from);
        });
        return Promise.resolve();
    }
    close(): Promise<void> {
        this.bus.unregister(this.address);
        this.handlers.clear();
        return Promise.resolve();
    }
    broadcast(msg: WireMessage): Promise<void> {
        this.bus.broadcast(this.address, msg);
        return Promise.resolve();
    }
    sendTo(target: Address, msg: WireMessage): Promise<void> {
        this.bus.sendTo(target, this.address, msg);
        return Promise.resolve();
    }
    onMessage(h: (m: WireMessage, from: Address) => void): Unsubscribe {
        this.handlers.add(h);
        return () => this.handlers.delete(h);
    }
}

/**
 * Per-node clock over a shared master. `halt()` is crash-stop for timers: the node stops
 * doing anything at all, without the graceful `engine.stop()` that would broadcast a CLOSE
 * — a crashed node emits no CLOSE (§2.2), so it may only leave Σ via TTL or the ι gate.
 */
class HaltableClock implements Clock {
    private readonly handles = new Set<TimerHandle>();
    private halted = false;
    constructor(private readonly master: FakeClock) {}
    now(): number {
        return this.master.now();
    }
    setTimer(fn: () => void, ms: number): TimerHandle {
        if (this.halted) return -1;
        const handle = this.master.setTimer(() => {
            this.handles.delete(handle);
            if (!this.halted) fn();
        }, ms);
        this.handles.add(handle);
        return handle;
    }
    clearTimer(h: TimerHandle): void {
        this.handles.delete(h);
        this.master.clearTimer(h);
    }
    halt(): void {
        this.halted = true;
        for (const h of this.handles) this.master.clearTimer(h);
        this.handles.clear();
    }
}

const THETA = 3_000; // Θ
const cfg = {
    debounce: { delayMs: 10, maxWaitMs: 50 }, // δ, D_max
    ttlMs: THETA,
    sweepIntervalMs: 200, // T_sweep
    resyncIntervalMs: 500, // T_resync
};
const DELTA = 30; // Δ — link delay, deliberately > δ so STATUS lands after the first
// DEBATE window closes and `j` needs TWO version steps to converge.

// Virtual time has no drift, so ρ̄ = 0 and the paper's (1 − ρ̄) divisors are unity here.
const T_HB = cfg.resyncIntervalMs + DELTA; // eq. (9)
const THM_2_PRIME = T_HB + cfg.debounce.maxWaitMs; // T_hb + D_max/(1 − ρ̄)
const THM_2_UPPER = THETA + cfg.sweepIntervalMs + cfg.debounce.maxWaitMs + T_HB; // eq. (14) upper

const run = async (clock: FakeClock, engines: Engine<Context>[], forMs: number, step = 5): Promise<void> => {
    for (let elapsed = 0; elapsed < forMs; elapsed += step) {
        clock.advance(step);
        await Promise.all(engines.map((e) => e.settle()));
        await Promise.resolve();
        await Promise.resolve();
    }
};

interface Sample {
    t: number;
    peers: string[];
}

/**
 * Runs the crash/recover scenario under the given incarnation strategy and returns the
 * trajectory of `a`'s view, sampled at every convergence, plus the recovery instant.
 */
const runScenario = async (incarnationFor: (clock: Clock) => Incarnation | undefined) => {
    const master = new FakeClock(0);
    const bus = new DelayBus(master, DELTA);
    const samples: Sample[] = [];

    const spawn = (address: Address, tag: string, clock: HaltableClock) => {
        const reducer = epochReducer(tag);
        const incarnation = incarnationFor(clock);
        const engine = createEngine<Context>({
            identity: { address },
            slan: new DelaySlan(address, bus),
            reducers: [reducer],
            config: cfg,
            clock,
            ...(incarnation ? { incarnation } : {}),
        });
        return { engine, reducer };
    };

    const aClock = new HaltableClock(master);
    const a = spawn('a', 'a@1', aClock);
    a.engine.onConverged(() => {
        const view = a.engine.stateOf(a.reducer)?.value;
        if (view) samples.push({ t: master.now(), peers: [...view.peers] });
    });

    const jClock = new HaltableClock(master);
    const j1 = spawn('j', 'j@1', jClock);

    await a.engine.start();
    await j1.engine.start();
    await run(master, [a.engine, j1.engine], THETA);

    // Crash-stop: no CLOSE, nothing in or out, timers dead.
    jClock.halt();
    bus.kill('j');
    // Down for well under Θ, so `a` still holds σ_a(j) = (x_pre, v_pre, τ).
    await run(master, [a.engine], 1_000);

    const recoveredAt = master.now();
    const j2Clock = new HaltableClock(master);
    const j2 = spawn('j', 'j@2', j2Clock);
    bus.revive('j');
    await j2.engine.start();

    // 120s of virtual time — 40× Θ, ~240 resync rounds.
    await run(master, [a.engine, j2.engine], 120_000);

    aClock.halt();
    j2Clock.halt();
    return { samples, recoveredAt, viewOfA: a.engine.stateOf(a.reducer)?.value };
};

describe('Engine crash-recovery: the version shadow', () => {
    it('baseline: the pre-crash cluster sees the first epoch', async () => {
        const { samples } = await runScenario(() => undefined);
        const beforeCrash = samples.filter((s) => s.t <= THETA);

        expect(beforeCrash.at(-1)?.peers).toEqual(['j@1']);
    });

    it('AMENDED: the recovered node is adopted within a heartbeat, far inside Θ', async () => {
        const { samples, recoveredAt, viewOfA } = await runScenario(() => undefined); // default ι

        const cleared = samples.find((s) => s.t > recoveredAt && !s.peers.includes('j@1'));
        expect(cleared).toBeDefined();

        // Theorem 2′: T_rej ≤ T_hb + D_max/(1 − ρ̄), with no dependence on Θ. Adoption
        // happens on the recovered node's FIRST share, not after its record ages out.
        expect(cleared!.t - recoveredAt).toBeLessThanOrEqual(THM_2_PRIME);
        // …and comfortably inside what the un-amended TTL route would have cost.
        expect(THM_2_PRIME).toBeLessThan(THM_2_UPPER);

        // And the dead lifetime never comes back.
        const after = samples.filter((s) => s.t >= cleared!.t);
        expect(after.every((s) => !s.peers.includes('j@1'))).toBe(true);
        expect(viewOfA?.peers).toEqual(['j@2']);
    });

    it('STOCK ARM: without lifetime ordering the dead incarnation is never displaced', async () => {
        // StaticIncarnation reproduces the pre-amendment protocol exactly — the §7.3
        // control arm. σ_a(j) stays pinned to the pre-crash payload, and because every
        // equal-version SHARE refreshes τ, the TTL that should have rescued it never fires.
        const { samples, recoveredAt, viewOfA } = await runScenario(() => new StaticIncarnation(0));

        const late = samples.filter((s) => s.t > recoveredAt + 100_000);
        expect(late.length).toBeGreaterThan(0);
        expect(late.every((s) => s.peers.includes('j@1'))).toBe(true);
        expect(viewOfA?.peers).toContain('j@1');
    });
});
