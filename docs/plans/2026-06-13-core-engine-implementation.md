# @rsdp/core Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the M1 `@rsdp/core` framework — typed reducer pipelines + a memory-based convergence engine — per the design spec at `core/docs/specs/2026-06-13-core-engine-design.md`.

**Architecture:** The engine owns all convergence machinery (per-reducer `Σ` memory, a one-shot DEBATE buffer, a δ/D_max debouncer, a per-reducer single-flight run queue, TTL eviction) and hands each reducer an immutable snapshot; a reducer is a pure `f(snapshot, ctx, prev) → state` plus a translator to a wire view `V`. Transport is an injected `Slan` port; an `InMemorySlan` + `FakeClock` make convergence fully deterministic to test.

**Tech Stack:** TypeScript (ESM, `NodeNext`, strict, `verbatimModuleSyntax`), Vitest 4, ESLint 9 flat config. Node ≥ 22.

**Conventions for every task:**

- All relative imports use the `.js` extension (NodeNext) and `import type { … }` for type-only imports (`verbatimModuleSyntax`).
- Tests live under `tests/` mirroring `src/` (e.g. `src/clock/Clock.ts` → `tests/clock/Clock.test.ts`).
- Run a single test file with: `npx vitest run tests/<path>.test.ts`. Run all: `npm test`. Typecheck: `npm run typecheck`.
- Commit messages are **Conventional Commits** (commitlint enforces this via husky `commit-msg`). The husky `pre-commit` hook runs lint-staged (eslint --fix + prettier) automatically.

---

## File structure

```
src/
  domain/{address,state,message,peer,context}.ts   pure types
  errors.ts                                         RsdpError hierarchy
  clock/Clock.ts                                    Clock iface, SystemClock, FakeClock
  engine/schedule/Debouncer.ts                      δ/D_max dual-trigger
  engine/schedule/RunQueue.ts                       per-reducer single-flight
  engine/memory/MemoryMap.ts                        Σ for one reducer
  engine/memory/DebateBuffer.ts                     transient bootstrap buffer
  reducer/pipeline/stages.ts                        the 7 stage interfaces + fn types
  reducer/pipeline/Pipeline.ts                      pipeline runner
  reducer/Reducer.ts                                Reducer + defineReducer builder
  slan/Slan.ts                                      transport port
  testing/InMemorySlan.ts                           in-process bus + slan (subpath export)
  engine/phases/Fsm.ts                              phase status + transitions
  engine/Engine.ts                                  orchestrator + createEngine
  reducers/clusterMembers.ts                        reference reducer (fixture + example)
  index.ts                                          public barrel
```

---

## Task 1: Domain types + error hierarchy

**Files:**

- Create: `src/domain/address.ts`, `src/domain/state.ts`, `src/domain/message.ts`, `src/domain/peer.ts`, `src/domain/context.ts`, `src/errors.ts`
- Test: `tests/domain/errors.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/domain/errors.test.ts
import { describe, expect, it } from 'vitest';

import { ConfigError, GuardRejected, PipelineError, RsdpError } from '../../src/errors.js';

describe('error hierarchy', () => {
    it('all errors extend RsdpError and carry a name', () => {
        const errs = [
            new ConfigError('c'),
            new GuardRejected('g'),
            new PipelineError('p', { reducer: 'r', stage: 'guard', messageType: 'SHARE' }),
        ];
        for (const e of errs) {
            expect(e).toBeInstanceOf(RsdpError);
            expect(e.name).toBe(e.constructor.name);
            expect(e.message.length).toBeGreaterThan(0);
        }
    });

    it('PipelineError exposes context', () => {
        const e = new PipelineError('boom', { reducer: 'cluster-members', stage: 'aggregator', messageType: 'SHARE' });
        expect(e.context).toEqual({ reducer: 'cluster-members', stage: 'aggregator', messageType: 'SHARE' });
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/domain/errors.test.ts`
Expected: FAIL — cannot find module `../../src/errors.js`.

- [ ] **Step 3: Write the domain types**

```ts
// src/domain/address.ts
export type Address = string;
export type ReducerName = string;
export interface NodeIdentity {
    address: Address;
    metadata?: Record<string, unknown>;
}
```

```ts
// src/domain/state.ts
export interface TranslatedState<V> {
    value: V | null;
    changed: boolean;
}
```

```ts
// src/domain/message.ts
import type { Address, ReducerName } from './address.js';

export type MessageType = 'HELLO' | 'STATUS' | 'SHARE' | 'CLOSE';

export interface ReducerPayload {
    value: unknown;
    version: number;
}

export interface WireMessage {
    type: MessageType;
    from: Address;
    payloads?: Record<ReducerName, ReducerPayload>; // STATUS / SHARE composite
    closed?: Address; // CLOSE
}
```

```ts
// src/domain/peer.ts
export interface PeerRecord<P> {
    payload: P;
    version: number;
    lastSeenAt: number;
}
```

```ts
// src/domain/context.ts
import type { Address } from './address.js';

/** Threaded through every pipeline run. Reducers may use a Ctx that extends this. */
export interface Context {
    self: Address;
}
```

```ts
// src/errors.ts
import type { MessageType } from './domain/message.js';

export class RsdpError extends Error {
    constructor(message: string) {
        super(message);
        this.name = new.target.name;
    }
}

export class ConfigError extends RsdpError {}

export class GuardRejected extends RsdpError {}

export class SlanError extends RsdpError {}

export interface PipelineErrorContext {
    reducer: string;
    stage: 'middleware' | 'guard' | 'interceptor' | 'normalizer' | 'aggregator' | 'translator';
    messageType: MessageType;
}

export class PipelineError extends RsdpError {
    constructor(
        message: string,
        public readonly context: PipelineErrorContext,
    ) {
        super(message);
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run tests/domain/errors.test.ts` → PASS. Then `npm run typecheck` → no errors.

- [ ] **Step 5: Commit**

```bash
git add src/domain src/errors.ts tests/domain
git commit -m "feat(core): add domain types and error hierarchy"
```

---

## Task 2: Clock (SystemClock + FakeClock)

**Files:**

- Create: `src/clock/Clock.ts`
- Test: `tests/clock/Clock.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/clock/Clock.test.ts
import { describe, expect, it, vi } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';

describe('FakeClock', () => {
    it('fires timers only once their delay has elapsed via advance()', () => {
        const clock = new FakeClock(1000);
        const fired: string[] = [];
        clock.setTimer(() => fired.push('a'), 50);
        clock.setTimer(() => fired.push('b'), 100);

        clock.advance(49);
        expect(fired).toEqual([]);
        clock.advance(1); // now 1050 → 'a' due
        expect(fired).toEqual(['a']);
        expect(clock.now()).toBe(1050);
        clock.advance(50); // now 1100 → 'b' due
        expect(fired).toEqual(['a', 'b']);
    });

    it('clearTimer prevents firing', () => {
        const clock = new FakeClock(0);
        const fn = vi.fn();
        const h = clock.setTimer(fn, 10);
        clock.clearTimer(h);
        clock.advance(100);
        expect(fn).not.toHaveBeenCalled();
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/clock/Clock.test.ts` → FAIL (module missing).

- [ ] **Step 3: Implement**

```ts
// src/clock/Clock.ts
export type TimerHandle = number;

export interface Clock {
    now(): number;
    setTimer(fn: () => void, ms: number): TimerHandle;
    clearTimer(handle: TimerHandle): void;
}

export class SystemClock implements Clock {
    now(): number {
        return Date.now();
    }
    setTimer(fn: () => void, ms: number): TimerHandle {
        return setTimeout(fn, ms) as unknown as TimerHandle;
    }
    clearTimer(handle: TimerHandle): void {
        clearTimeout(handle as unknown as ReturnType<typeof setTimeout>);
    }
}

interface Scheduled {
    id: TimerHandle;
    fireAt: number;
    fn: () => void;
}

export class FakeClock implements Clock {
    private current: number;
    private seq = 0;
    private timers: Scheduled[] = [];

    constructor(start = 0) {
        this.current = start;
    }

    now(): number {
        return this.current;
    }

    setTimer(fn: () => void, ms: number): TimerHandle {
        const id = ++this.seq;
        this.timers.push({ id, fireAt: this.current + Math.max(0, ms), fn });
        return id;
    }

    clearTimer(handle: TimerHandle): void {
        this.timers = this.timers.filter((t) => t.id !== handle);
    }

    /** Advance virtual time, firing every timer whose deadline is reached, in order. */
    advance(ms: number): void {
        const target = this.current + ms;
        for (;;) {
            const next = this.timers
                .filter((t) => t.fireAt <= target)
                .sort((a, b) => a.fireAt - b.fireAt || a.id - b.id)[0];
            if (!next) break;
            this.timers = this.timers.filter((t) => t.id !== next.id);
            this.current = next.fireAt;
            next.fn();
        }
        this.current = target;
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run tests/clock/Clock.test.ts` → PASS. `npm run typecheck` → clean.

- [ ] **Step 5: Commit**

```bash
git add src/clock tests/clock
git commit -m "feat(core): add Clock with SystemClock and deterministic FakeClock"
```

---

## Task 3: Debouncer (δ + D_max dual trigger)

**Files:**

- Create: `src/engine/schedule/Debouncer.ts`
- Test: `tests/engine/schedule/Debouncer.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/engine/schedule/Debouncer.test.ts
import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../../src/clock/Clock.js';
import { Debouncer } from '../../../src/engine/schedule/Debouncer.js';

describe('Debouncer', () => {
    it('coalesces bursts and fires after δ of quiet', () => {
        const clock = new FakeClock(0);
        let fires = 0;
        const d = new Debouncer(clock, { delayMs: 10, maxWaitMs: 100 }, () => fires++);
        d.notifyChange(); // t=0, scheduled ~10
        clock.advance(5);
        d.notifyChange(); // reschedules to 15
        clock.advance(9); // t=14, not yet
        expect(fires).toBe(0);
        clock.advance(1); // t=15 → fire
        expect(fires).toBe(1);
    });

    it('never delays beyond D_max since the first change', () => {
        const clock = new FakeClock(0);
        let fires = 0;
        const d = new Debouncer(clock, { delayMs: 10, maxWaitMs: 25 }, () => fires++);
        d.notifyChange(); // first change at t=0 → hard cap t=25
        for (let i = 0; i < 5; i++) {
            clock.advance(8);
            d.notifyChange(); // keep pushing the δ window
        }
        expect(fires).toBe(1); // fired at t=25 despite continuous changes
    });

    it('starts a fresh window after firing', () => {
        const clock = new FakeClock(0);
        let fires = 0;
        const d = new Debouncer(clock, { delayMs: 10, maxWaitMs: 100 }, () => fires++);
        d.notifyChange();
        clock.advance(10);
        expect(fires).toBe(1);
        d.notifyChange();
        clock.advance(10);
        expect(fires).toBe(2);
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/engine/schedule/Debouncer.test.ts` → FAIL.

- [ ] **Step 3: Implement**

```ts
// src/engine/schedule/Debouncer.ts
import type { Clock, TimerHandle } from '../../clock/Clock.js';

export interface DebounceConfig {
    delayMs: number; // δ — quiet gap before firing
    maxWaitMs: number; // D_max — hard cap since first change in a window
}

export class Debouncer {
    private firstChangeAt: number | null = null;
    private timer: TimerHandle | null = null;

    constructor(
        private readonly clock: Clock,
        private readonly config: DebounceConfig,
        private readonly onFire: () => void,
    ) {}

    notifyChange(): void {
        const now = this.clock.now();
        if (this.firstChangeAt === null) this.firstChangeAt = now;
        const deadline = Math.min(now + this.config.delayMs, this.firstChangeAt + this.config.maxWaitMs);
        if (this.timer !== null) this.clock.clearTimer(this.timer);
        this.timer = this.clock.setTimer(() => this.fire(), Math.max(0, deadline - now));
    }

    cancel(): void {
        if (this.timer !== null) this.clock.clearTimer(this.timer);
        this.timer = null;
        this.firstChangeAt = null;
    }

    private fire(): void {
        this.timer = null;
        this.firstChangeAt = null;
        this.onFire();
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run tests/engine/schedule/Debouncer.test.ts` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/schedule/Debouncer.ts tests/engine/schedule/Debouncer.test.ts
git commit -m "feat(core): add dual-trigger debouncer (delta + D_max)"
```

---

## Task 4: RunQueue (per-reducer single-flight, coalescing)

**Files:**

- Create: `src/engine/schedule/RunQueue.ts`
- Test: `tests/engine/schedule/RunQueue.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/engine/schedule/RunQueue.test.ts
import { describe, expect, it } from 'vitest';

import { RunQueue } from '../../../src/engine/schedule/RunQueue.js';

const deferred = () => {
    let resolve!: () => void;
    const promise = new Promise<void>((r) => (resolve = r));
    return { promise, resolve };
};

describe('RunQueue', () => {
    it('runs serially and coalesces triggers arriving mid-run into one follow-up', async () => {
        const gates = [deferred(), deferred()];
        let started = 0;
        const q = new RunQueue(async () => {
            const gate = gates[started++];
            await gate?.promise;
        });

        q.trigger(); // starts run #1
        q.trigger(); // mid-run → marks dirty
        q.trigger(); // still dirty (coalesced)
        expect(started).toBe(1);

        gates[0]?.resolve(); // finish run #1 → should start exactly one follow-up
        await Promise.resolve();
        await Promise.resolve();
        expect(started).toBe(2);

        gates[1]?.resolve();
        await q.idle();
        expect(started).toBe(2); // no extra run
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/engine/schedule/RunQueue.test.ts` → FAIL.

- [ ] **Step 3: Implement**

```ts
// src/engine/schedule/RunQueue.ts
export class RunQueue {
    private running = false;
    private dirty = false;
    private settled: Promise<void> = Promise.resolve();

    constructor(private readonly run: () => Promise<void>) {}

    trigger(): void {
        if (this.running) {
            this.dirty = true;
            return;
        }
        this.running = true;
        this.settled = this.loop();
    }

    /** Resolves when no run is in progress or pending. */
    async idle(): Promise<void> {
        await this.settled;
    }

    private async loop(): Promise<void> {
        try {
            do {
                this.dirty = false;
                await this.run();
            } while (this.dirty);
        } finally {
            this.running = false;
        }
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run tests/engine/schedule/RunQueue.test.ts` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/schedule/RunQueue.ts tests/engine/schedule/RunQueue.test.ts
git commit -m "feat(core): add single-flight coalescing RunQueue"
```

---

## Task 5: MemoryMap (Σ for one reducer)

**Files:**

- Create: `src/engine/memory/MemoryMap.ts`
- Test: `tests/engine/memory/MemoryMap.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/engine/memory/MemoryMap.test.ts
import { describe, expect, it } from 'vitest';

import { MemoryMap } from '../../../src/engine/memory/MemoryMap.js';

describe('MemoryMap', () => {
    it('updates and snapshots non-evicted peers', () => {
        const m = new MemoryMap<string>();
        expect(m.update('a', 'x', 1, 1000)).toBe(true);
        expect(m.update('b', 'y', 1, 1000)).toBe(true);
        expect([...m.snapshot()]).toEqual([
            ['a', 'x'],
            ['b', 'y'],
        ]);
    });

    it('ignores stale or equal versions', () => {
        const m = new MemoryMap<string>();
        m.update('a', 'x', 5, 1000);
        expect(m.update('a', 'OLD', 4, 1001)).toBe(false);
        expect(m.update('a', 'SAME', 5, 1002)).toBe(false);
        expect(m.update('a', 'NEW', 6, 1003)).toBe(true);
        expect([...m.snapshot()]).toEqual([['a', 'NEW']]);
    });

    it('evicts explicitly and by TTL', () => {
        const m = new MemoryMap<string>();
        m.update('a', 'x', 1, 1000);
        m.update('b', 'y', 1, 1000);
        expect(m.evict('a')).toBe(true);
        expect(m.evict('a')).toBe(false);
        expect([...m.snapshot()]).toEqual([['b', 'y']]);

        m.update('c', 'z', 1, 2000);
        // at t=2500, ttl=600: b (lastSeen 1000) expires, c (2000) survives
        expect(m.sweepExpired(2500, 600).sort()).toEqual(['b']);
        expect([...m.snapshot()]).toEqual([['c', 'z']]);
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/engine/memory/MemoryMap.test.ts` → FAIL.

- [ ] **Step 3: Implement**

```ts
// src/engine/memory/MemoryMap.ts
import type { Address } from '../../domain/address.js';
import type { PeerRecord } from '../../domain/peer.js';

export class MemoryMap<P> {
    private readonly records = new Map<Address, PeerRecord<P>>();

    /** Version-gated. Returns true if the map changed. */
    update(addr: Address, payload: P, version: number, now: number): boolean {
        const existing = this.records.get(addr);
        if (existing && version <= existing.version) return false;
        this.records.set(addr, { payload, version, lastSeenAt: now });
        return true;
    }

    evict(addr: Address): boolean {
        return this.records.delete(addr);
    }

    /** Remove peers unseen for >= ttlMs. Returns the evicted addresses. */
    sweepExpired(now: number, ttlMs: number): Address[] {
        const evicted: Address[] = [];
        for (const [addr, rec] of this.records) {
            if (now - rec.lastSeenAt >= ttlMs) {
                this.records.delete(addr);
                evicted.push(addr);
            }
        }
        return evicted;
    }

    snapshot(): ReadonlyArray<readonly [Address, P]> {
        return [...this.records.entries()].map(([addr, rec]) => [addr, rec.payload] as const);
    }

    get size(): number {
        return this.records.size;
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run tests/engine/memory/MemoryMap.test.ts` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/memory/MemoryMap.ts tests/engine/memory/MemoryMap.test.ts
git commit -m "feat(core): add version-gated MemoryMap with TTL eviction"
```

---

## Task 6: DebateBuffer (transient bootstrap buffer)

**Files:**

- Create: `src/engine/memory/DebateBuffer.ts`
- Test: `tests/engine/memory/DebateBuffer.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/engine/memory/DebateBuffer.test.ts
import { describe, expect, it } from 'vitest';

import { DebateBuffer } from '../../../src/engine/memory/DebateBuffer.js';

describe('DebateBuffer', () => {
    it('keeps the latest payload per peer and snapshots values', () => {
        const b = new DebateBuffer<string>();
        b.set('a', 'first');
        b.set('a', 'second'); // overwrite
        b.set('b', 'x');
        expect(b.snapshot().sort()).toEqual(['second', 'x']);
    });

    it('clears', () => {
        const b = new DebateBuffer<string>();
        b.set('a', 'x');
        b.clear();
        expect(b.snapshot()).toEqual([]);
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/engine/memory/DebateBuffer.test.ts` → FAIL.

- [ ] **Step 3: Implement**

```ts
// src/engine/memory/DebateBuffer.ts
import type { Address } from '../../domain/address.js';

/** Transient, one-shot store of neighbours' payloads during the DEBATE bootstrap. */
export class DebateBuffer<P> {
    private readonly items = new Map<Address, P>();

    set(addr: Address, payload: P): void {
        this.items.set(addr, payload);
    }

    snapshot(): P[] {
        return [...this.items.values()];
    }

    clear(): void {
        this.items.clear();
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run tests/engine/memory/DebateBuffer.test.ts` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/memory/DebateBuffer.ts tests/engine/memory/DebateBuffer.test.ts
git commit -m "feat(core): add transient DebateBuffer"
```

---

## Task 7: Pipeline stage interfaces + runner

**Files:**

- Create: `src/reducer/pipeline/stages.ts`, `src/reducer/pipeline/Pipeline.ts`
- Test: `tests/reducer/pipeline/Pipeline.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/reducer/pipeline/Pipeline.test.ts
import { describe, expect, it } from 'vitest';

import type { Context } from '../../../src/domain/context.js';
import { GuardRejected } from '../../../src/errors.js';
import { Pipeline } from '../../../src/reducer/pipeline/Pipeline.js';
import type { Aggregator, Guard, Interceptor, Middleware, Normalizer } from '../../../src/reducer/pipeline/stages.js';

const ctx: Context = { self: 'self' };

describe('Pipeline', () => {
    it('runs normalizer then aggregator', async () => {
        const normalizer: Normalizer<number, number, Context> = { normalize: (b) => b.map((n) => n * 10) };
        const aggregator: Aggregator<number, number, Context> = { aggregate: (b) => b.reduce((a, n) => a + n, 0) };
        const p = new Pipeline<number, number, number, Context>('r', 'SHARE', { normalizer, aggregator });
        expect(await p.run([1, 2, 3], ctx, null)).toBe(60);
    });

    it('rejects when a guard returns false', async () => {
        const aggregator: Aggregator<number, number, Context> = { aggregate: () => 1 };
        const guard: Guard<number, number, Context> = { check: (b) => b.length > 0 };
        const p = new Pipeline<number, number, number, Context>('r', 'SHARE', { aggregator, guards: [guard] });
        await expect(p.run([], ctx, null)).rejects.toBeInstanceOf(GuardRejected);
    });

    it('wraps aggregation with interceptors and the whole run with middleware (outermost first)', async () => {
        const order: string[] = [];
        const aggregator: Aggregator<number, number, Context> = {
            aggregate: () => {
                order.push('agg');
                return 1;
            },
        };
        const interceptor: Interceptor<number, number, Context> = {
            wrap: (next) => async (b, c, prev) => {
                order.push('int:before');
                const r = await next(b, c, prev);
                order.push('int:after');
                return r;
            },
        };
        const middleware: Middleware<number, number, Context> = {
            wrap: (next) => async (b, c, prev) => {
                order.push('mw:before');
                const r = await next(b, c, prev);
                order.push('mw:after');
                return r;
            },
        };
        const p = new Pipeline<number, number, number, Context>('r', 'SHARE', {
            aggregator,
            interceptors: [interceptor],
            middleware: [middleware],
        });
        await p.run([1], ctx, null);
        expect(order).toEqual(['mw:before', 'int:before', 'agg', 'int:after', 'mw:after']);
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/reducer/pipeline/Pipeline.test.ts` → FAIL.

- [ ] **Step 3: Implement the stage interfaces**

```ts
// src/reducer/pipeline/stages.ts
export type PipelineRun<R, S, Ctx> = (batch: R[], ctx: Ctx, prev: S | null) => Promise<S>;
export type Aggregate<M, S, Ctx> = (batch: M[], ctx: Ctx, prev: S | null) => Promise<S>;

export interface Middleware<R, S, Ctx> {
    wrap(next: PipelineRun<R, S, Ctx>): PipelineRun<R, S, Ctx>;
}
export interface Guard<R, S, Ctx> {
    check(batch: R[], ctx: Ctx, prev: S | null): boolean | Promise<boolean>;
}
export interface Interceptor<M, S, Ctx> {
    wrap(next: Aggregate<M, S, Ctx>): Aggregate<M, S, Ctx>;
}
export interface Normalizer<R, M, Ctx> {
    normalize(batch: R[], ctx: Ctx): M[] | Promise<M[]>;
}
export interface Aggregator<M, S, Ctx> {
    aggregate(batch: M[], ctx: Ctx, prev: S | null): S | Promise<S>;
}
export interface ExceptionFilter<Ctx> {
    handle(err: unknown, ctx: Ctx): void | Promise<void>;
}
```

- [ ] **Step 4: Implement the runner**

```ts
// src/reducer/pipeline/Pipeline.ts
import type { MessageType } from '../../domain/message.js';
import { GuardRejected } from '../../errors.js';
import type { Aggregate, Aggregator, Guard, Interceptor, Middleware, Normalizer, PipelineRun } from './stages.js';

export interface PipelineStages<R, M, S, Ctx> {
    middleware?: Middleware<R, S, Ctx>[];
    guards?: Guard<R, S, Ctx>[];
    interceptors?: Interceptor<M, S, Ctx>[];
    normalizer?: Normalizer<R, M, Ctx>;
    aggregator: Aggregator<M, S, Ctx>;
}

export class Pipeline<R, M, S, Ctx> {
    constructor(
        private readonly reducerName: string,
        private readonly messageType: MessageType,
        private readonly stages: PipelineStages<R, M, S, Ctx>,
    ) {}

    async run(batch: R[], ctx: Ctx, prev: S | null): Promise<S> {
        const core: PipelineRun<R, S, Ctx> = async (b, c, p) => {
            for (const guard of this.stages.guards ?? []) {
                if (!(await guard.check(b, c, p))) {
                    throw new GuardRejected(`guard rejected ${this.reducerName}/${this.messageType}`);
                }
            }
            const normalized: M[] = this.stages.normalizer
                ? await this.stages.normalizer.normalize(b, c)
                : (b as unknown as M[]);

            let aggregate: Aggregate<M, S, Ctx> = (mb, mc, mp) =>
                Promise.resolve(this.stages.aggregator.aggregate(mb, mc, mp));
            for (const interceptor of [...(this.stages.interceptors ?? [])].reverse()) {
                aggregate = interceptor.wrap(aggregate);
            }
            return aggregate(normalized, c, p);
        };

        let run = core;
        for (const mw of [...(this.stages.middleware ?? [])].reverse()) {
            run = mw.wrap(run);
        }
        return run(batch, ctx, prev);
    }
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `npx vitest run tests/reducer/pipeline/Pipeline.test.ts` → PASS. `npm run typecheck` → clean.

- [ ] **Step 6: Commit**

```bash
git add src/reducer/pipeline tests/reducer/pipeline
git commit -m "feat(core): add pipeline stage interfaces and runner"
```

---

## Task 8: Reducer + defineReducer builder

**Files:**

- Create: `src/reducer/Reducer.ts`
- Test: `tests/reducer/Reducer.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/reducer/Reducer.test.ts
import { describe, expect, it } from 'vitest';

import type { Context } from '../../src/domain/context.js';
import { ConfigError } from '../../src/errors.js';
import { defineReducer } from '../../src/reducer/Reducer.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';

type V = number[];
const sumInto: Aggregator<V, V, Context> = { aggregate: (batch) => batch.flat() };

describe('defineReducer', () => {
    it('builds a reducer with three pipelines and a translator', async () => {
        const r = defineReducer<V, V, Context>('nums')
            .status((p) => p.setAggregator(sumInto))
            .share((p) => p.setAggregator(sumInto))
            .close((p) => p.setAggregator({ aggregate: (_b, _c, prev) => prev ?? [] }))
            .setTranslator({ translate: (state) => ({ value: state, changed: true }) });

        expect(r.name).toBe('nums');
        const out = await r.runShare([[1], [2, 3]], { self: 's' }, null);
        expect(out).toEqual([1, 2, 3]);
        expect(r.translate([1, 2, 3], null, { self: 's' })).resolves.toEqual({ value: [1, 2, 3], changed: true });
    });

    it('verify() throws ConfigError when a pipeline or translator is missing', () => {
        const incomplete = defineReducer<V, V, Context>('bad').status((p) => p.setAggregator(sumInto));
        expect(() => incomplete.build()).toThrow(ConfigError);
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/reducer/Reducer.test.ts` → FAIL.

- [ ] **Step 3: Implement**

```ts
// src/reducer/Reducer.ts
import type { Address, ReducerName } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import type { TranslatedState } from '../domain/state.js';
import { ConfigError } from '../errors.js';
import { Pipeline } from './pipeline/Pipeline.js';
import type { Aggregator, ExceptionFilter, Guard, Interceptor, Middleware, Normalizer } from './pipeline/stages.js';

export interface Translator<S, V, Ctx> {
    translate(state: S | null, prev: S | null, ctx: Ctx): TranslatedState<V> | Promise<TranslatedState<V>>;
}

class PipelineBuilder<R, M, S, Ctx> {
    middleware: Middleware<R, S, Ctx>[] = [];
    guards: Guard<R, S, Ctx>[] = [];
    interceptors: Interceptor<M, S, Ctx>[] = [];
    normalizer?: Normalizer<R, M, Ctx>;
    aggregator?: Aggregator<M, S, Ctx>;
    exceptionFilters: ExceptionFilter<Ctx>[] = [];

    addMiddleware(m: Middleware<R, S, Ctx>): this {
        this.middleware.push(m);
        return this;
    }
    addGuard(g: Guard<R, S, Ctx>): this {
        this.guards.push(g);
        return this;
    }
    addInterceptor(i: Interceptor<M, S, Ctx>): this {
        this.interceptors.push(i);
        return this;
    }
    setNormalizer<M2>(n: Normalizer<R, M2, Ctx>): PipelineBuilder<R, M2, S, Ctx> {
        const next = this as unknown as PipelineBuilder<R, M2, S, Ctx>;
        next.normalizer = n;
        return next;
    }
    setAggregator(a: Aggregator<M, S, Ctx>): this {
        this.aggregator = a;
        return this;
    }
    addExceptionFilter(f: ExceptionFilter<Ctx>): this {
        this.exceptionFilters.push(f);
        return this;
    }
}

type Configure<R, S, Ctx> = (p: PipelineBuilder<R, R, S, Ctx>) => PipelineBuilder<R, unknown, S, Ctx>;

export class Reducer<S, V, Ctx extends Context> {
    constructor(
        readonly name: ReducerName,
        private readonly statusPipe: Pipeline<V, unknown, S, Ctx>,
        private readonly sharePipe: Pipeline<V, unknown, S, Ctx>,
        private readonly closePipe: Pipeline<Address, unknown, S, Ctx>,
        private readonly translator: Translator<S, V, Ctx>,
        readonly exceptionFilters: {
            status: ExceptionFilter<Ctx>[];
            share: ExceptionFilter<Ctx>[];
            close: ExceptionFilter<Ctx>[];
        },
    ) {}

    runStatus(batch: V[], ctx: Ctx, prev: S | null): Promise<S> {
        return this.statusPipe.run(batch, ctx, prev);
    }
    runShare(batch: V[], ctx: Ctx, prev: S | null): Promise<S> {
        return this.sharePipe.run(batch, ctx, prev);
    }
    runClose(batch: Address[], ctx: Ctx, prev: S | null): Promise<S> {
        return this.closePipe.run(batch, ctx, prev);
    }
    translate(state: S | null, prev: S | null, ctx: Ctx): TranslatedState<V> | Promise<TranslatedState<V>> {
        return this.translator.translate(state, prev, ctx);
    }
}

export class ReducerBuilder<S, V, Ctx extends Context> {
    private statusB?: PipelineBuilder<V, unknown, S, Ctx>;
    private shareB?: PipelineBuilder<V, unknown, S, Ctx>;
    private closeB?: PipelineBuilder<Address, unknown, S, Ctx>;
    private translator?: Translator<S, V, Ctx>;

    constructor(private readonly name: ReducerName) {}

    status(fn: Configure<V, S, Ctx>): this {
        this.statusB = fn(new PipelineBuilder<V, V, S, Ctx>());
        return this;
    }
    share(fn: Configure<V, S, Ctx>): this {
        this.shareB = fn(new PipelineBuilder<V, V, S, Ctx>());
        return this;
    }
    close(fn: Configure<Address, S, Ctx>): this {
        this.closeB = fn(new PipelineBuilder<Address, Address, S, Ctx>());
        return this;
    }
    setTranslator(t: Translator<S, V, Ctx>): Reducer<S, V, Ctx> {
        this.translator = t;
        return this.build();
    }

    /** Validate and assemble. Throws ConfigError if anything required is missing. */
    build(): Reducer<S, V, Ctx> {
        const need = <T>(v: T | undefined, what: string): T => {
            if (v === undefined) throw new ConfigError(`reducer "${this.name}" is missing ${what}`);
            return v;
        };
        const status = need(this.statusB, 'a STATUS pipeline');
        const share = need(this.shareB, 'a SHARE pipeline');
        const close = need(this.closeB, 'a CLOSE pipeline');
        const translator = need(this.translator, 'a translator');
        for (const [b, label] of [
            [status, 'STATUS'],
            [share, 'SHARE'],
            [close, 'CLOSE'],
        ] as const) {
            if (!b.aggregator) throw new ConfigError(`reducer "${this.name}" ${label} pipeline has no aggregator`);
        }
        return new Reducer<S, V, Ctx>(
            this.name,
            new Pipeline('', 'STATUS', { ...status, aggregator: status.aggregator! }),
            new Pipeline('', 'SHARE', { ...share, aggregator: share.aggregator! }),
            new Pipeline('', 'CLOSE', { ...close, aggregator: close.aggregator! }),
            translator,
            {
                status: status.exceptionFilters,
                share: share.exceptionFilters,
                close: close.exceptionFilters,
            },
        );
    }
}

export function defineReducer<S, V, Ctx extends Context = Context>(name: ReducerName): ReducerBuilder<S, V, Ctx> {
    return new ReducerBuilder<S, V, Ctx>(name);
}
```

> Note: `ReducerBuilder` here uses `.build()` internally from `setTranslator`. The test's `incomplete.build()` path exercises the missing-translator branch. The reducer name is passed to `Pipeline` as `''` for brevity in M1; wire the real name through if pipeline-error context needs it (it does — see Task 11; pass `this.name` instead of `''`).

- [ ] **Step 4: Pass the reducer name into pipelines**

Replace the three `new Pipeline('', …)` with `new Pipeline(this.name, …)` so `PipelineError.context.reducer` is accurate.

- [ ] **Step 5: Run to verify it passes**

Run: `npx vitest run tests/reducer/Reducer.test.ts` → PASS. `npm run typecheck` → clean.

- [ ] **Step 6: Commit**

```bash
git add src/reducer/Reducer.ts tests/reducer/Reducer.test.ts
git commit -m "feat(core): add Reducer and closure-grouped defineReducer builder"
```

---

## Task 9: Slan port + InMemorySlan (with shared bus)

**Files:**

- Create: `src/slan/Slan.ts`, `src/testing/InMemorySlan.ts`
- Test: `tests/testing/InMemorySlan.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/testing/InMemorySlan.test.ts
import { describe, expect, it } from 'vitest';

import type { WireMessage } from '../../src/domain/message.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

const hello = (from: string): WireMessage => ({ type: 'HELLO', from });

describe('InMemorySlan', () => {
    it('broadcast reaches every other node but not the sender', async () => {
        const bus = new InMemoryBus();
        const a = new InMemorySlan('a', bus);
        const b = new InMemorySlan('b', bus);
        const c = new InMemorySlan('c', bus);
        const seenB: string[] = [];
        const seenC: string[] = [];
        b.onMessage((m) => seenB.push(m.from));
        c.onMessage((m) => seenC.push(m.from));
        await Promise.all([a.init(), b.init(), c.init()]);

        await a.broadcast(hello('a'));
        expect(seenB).toEqual(['a']);
        expect(seenC).toEqual(['a']);
    });

    it('sendTo reaches only the target', async () => {
        const bus = new InMemoryBus();
        const a = new InMemorySlan('a', bus);
        const b = new InMemorySlan('b', bus);
        const c = new InMemorySlan('c', bus);
        const seenB: string[] = [];
        const seenC: string[] = [];
        b.onMessage((m) => seenB.push(m.from));
        c.onMessage((m) => seenC.push(m.from));
        await Promise.all([a.init(), b.init(), c.init()]);

        await a.sendTo('b', hello('a'));
        expect(seenB).toEqual(['a']);
        expect(seenC).toEqual([]);
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/testing/InMemorySlan.test.ts` → FAIL.

- [ ] **Step 3: Implement the port**

```ts
// src/slan/Slan.ts
import type { Address } from '../domain/address.js';
import type { WireMessage } from '../domain/message.js';

export type Unsubscribe = () => void;

export interface Slan {
    readonly address: Address;
    init(): Promise<void>;
    close(): Promise<void>;
    broadcast(msg: WireMessage): Promise<void>;
    sendTo(target: Address, msg: WireMessage): Promise<void>;
    onMessage(handler: (msg: WireMessage, from: Address) => void): Unsubscribe;
}
```

- [ ] **Step 4: Implement the in-memory bus + slan**

```ts
// src/testing/InMemorySlan.ts
import type { Address } from '../domain/address.js';
import type { WireMessage } from '../domain/message.js';
import type { Slan, Unsubscribe } from '../slan/Slan.js';

export class InMemoryBus {
    private readonly nodes = new Map<Address, (msg: WireMessage, from: Address) => void>();

    register(addr: Address, deliver: (msg: WireMessage, from: Address) => void): void {
        this.nodes.set(addr, deliver);
    }
    unregister(addr: Address): void {
        this.nodes.delete(addr);
    }
    broadcast(from: Address, msg: WireMessage): void {
        for (const [addr, deliver] of this.nodes) {
            if (addr !== from) deliver(msg, from);
        }
    }
    sendTo(target: Address, from: Address, msg: WireMessage): void {
        this.nodes.get(target)?.(msg, from);
    }
}

export class InMemorySlan implements Slan {
    private readonly handlers = new Set<(msg: WireMessage, from: Address) => void>();

    constructor(
        readonly address: Address,
        private readonly bus: InMemoryBus,
    ) {}

    init(): Promise<void> {
        this.bus.register(this.address, (msg, from) => {
            for (const h of this.handlers) h(msg, from);
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
    onMessage(handler: (msg: WireMessage, from: Address) => void): Unsubscribe {
        this.handlers.add(handler);
        return () => this.handlers.delete(handler);
    }
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `npx vitest run tests/testing/InMemorySlan.test.ts` → PASS.

- [ ] **Step 6: Commit**

```bash
git add src/slan tests/testing src/testing/InMemorySlan.ts
git commit -m "feat(core): add Slan port and in-memory bus implementation"
```

---

## Task 10: Phase FSM

**Files:**

- Create: `src/engine/phases/Fsm.ts`
- Test: `tests/engine/phases/Fsm.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/engine/phases/Fsm.test.ts
import { describe, expect, it } from 'vitest';

import { Fsm, Phase } from '../../../src/engine/phases/Fsm.js';

describe('Fsm', () => {
    it('starts INITIAL and walks INITIAL→DEBATE→IDLE', () => {
        const fsm = new Fsm();
        expect(fsm.phase).toBe(Phase.INITIAL);
        fsm.to(Phase.DEBATE);
        fsm.to(Phase.IDLE);
        expect(fsm.phase).toBe(Phase.IDLE);
    });

    it('rejects illegal transitions', () => {
        const fsm = new Fsm();
        expect(() => fsm.to(Phase.SHARE)).toThrow(); // INITIAL→SHARE illegal
    });

    it('allows IDLE↔CLOSE and IDLE→SHARE→IDLE', () => {
        const fsm = new Fsm();
        fsm.to(Phase.DEBATE);
        fsm.to(Phase.IDLE);
        fsm.to(Phase.SHARE);
        fsm.to(Phase.IDLE);
        fsm.to(Phase.CLOSE);
        fsm.to(Phase.IDLE);
        expect(fsm.phase).toBe(Phase.IDLE);
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/engine/phases/Fsm.test.ts` → FAIL.

- [ ] **Step 3: Implement**

```ts
// src/engine/phases/Fsm.ts
import { RsdpError } from '../../errors.js';

export enum Phase {
    INITIAL = 'INITIAL',
    DEBATE = 'DEBATE',
    IDLE = 'IDLE',
    SHARE = 'SHARE',
    CLOSE = 'CLOSE',
}

const TRANSITIONS: Record<Phase, Phase[]> = {
    [Phase.INITIAL]: [Phase.DEBATE],
    [Phase.DEBATE]: [Phase.IDLE],
    [Phase.IDLE]: [Phase.SHARE, Phase.CLOSE],
    [Phase.SHARE]: [Phase.IDLE],
    [Phase.CLOSE]: [Phase.IDLE],
};

export class Fsm {
    private current: Phase = Phase.INITIAL;

    get phase(): Phase {
        return this.current;
    }

    to(next: Phase): void {
        if (!TRANSITIONS[this.current].includes(next)) {
            throw new RsdpError(`illegal phase transition ${this.current} → ${next}`);
        }
        this.current = next;
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run tests/engine/phases/Fsm.test.ts` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine/phases tests/engine/phases
git commit -m "feat(core): add phase FSM"
```

---

## Task 11: Engine — DEBATE bootstrap

**Files:**

- Create: `src/engine/Engine.ts`
- Test: `tests/engine/Engine.debate.test.ts`

This task builds `createEngine` and the DEBATE path only (HELLO → STATUS into DebateBuffer → one-shot debounce → STATUS pipeline → broadcast composite SHARE). Steady state lands in Task 12.

- [ ] **Step 1: Write the failing test**

```ts
// tests/engine/Engine.debate.test.ts
import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import type { WireMessage } from '../../src/domain/message.js';
import { createEngine } from '../../src/engine/Engine.js';
import { defineReducer } from '../../src/reducer/Reducer.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

type V = string[];
// Union of all peer id-lists plus self.
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (batch) => [...new Set([self, ...batch.flat()])].sort(),
});
const reducer = (self: string) =>
    defineReducer<V, V, Context>('members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({ translate: (state) => ({ value: state, changed: true }) });

const cfg = { debounce: { delayMs: 10, maxWaitMs: 50 }, ttlMs: 10_000, sweepIntervalMs: 1_000 };

describe('Engine DEBATE bootstrap', () => {
    it('broadcasts HELLO on start, then a SHARE after the DEBATE window', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);

        // a passive spy node records what 'a' emits.
        const spy = new InMemorySlan('spy', bus);
        const seen: WireMessage[] = [];
        spy.onMessage((m) => seen.push(m));
        await spy.init();

        const slanA = new InMemorySlan('a', bus);
        const engineA = createEngine<Context>({
            identity: { address: 'a' },
            slan: slanA,
            reducers: [reducer('a')],
            config: cfg,
            clock,
        });
        await engineA.start();

        // HELLO emitted immediately on start.
        expect(seen.map((m) => m.type)).toContain('HELLO');

        // Simulate a STATUS reply from a neighbour 'b' carrying its view.
        await spy.sendTo('a', { type: 'STATUS', from: 'b', payloads: { members: { value: ['b'], version: 1 } } });

        // Advance past the DEBATE debounce → engine aggregates and broadcasts SHARE.
        clock.advance(50);
        await engineA.settle();

        const share = seen.find((m) => m.type === 'SHARE');
        expect(share?.payloads?.['members']?.value).toEqual(['a', 'b']);

        await engineA.stop();
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/engine/Engine.debate.test.ts` → FAIL.

- [ ] **Step 3: Implement the engine (DEBATE path + scaffolding used by Task 12)**

```ts
// src/engine/Engine.ts
import { SystemClock } from '../clock/Clock.js';
import type { Clock } from '../clock/Clock.js';
import type { Address, NodeIdentity, ReducerName } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import type { ReducerPayload, WireMessage } from '../domain/message.js';
import type { TranslatedState } from '../domain/state.js';
import { PipelineError, RsdpError } from '../errors.js';
import type { Reducer } from '../reducer/Reducer.js';
import type { Slan, Unsubscribe } from '../slan/Slan.js';
import { DebateBuffer } from './memory/DebateBuffer.js';
import { MemoryMap } from './memory/MemoryMap.js';
import { Fsm, Phase } from './phases/Fsm.js';
import { Debouncer } from './schedule/Debouncer.js';
import type { DebounceConfig } from './schedule/Debouncer.js';
import { RunQueue } from './schedule/RunQueue.js';

export interface EngineConfig {
    debounce: DebounceConfig;
    ttlMs: number;
    sweepIntervalMs: number;
}

export interface StateSnapshot<Ctx extends Context> {
    get<V>(reducer: Reducer<unknown, V, Ctx>): TranslatedState<V> | null;
}

export interface Engine<Ctx extends Context> {
    start(): Promise<void>;
    stop(): Promise<void>;
    stateOf<V>(reducer: Reducer<unknown, V, Ctx>): TranslatedState<V> | null;
    stateByName(name: ReducerName): TranslatedState<unknown> | null;
    onConverged(cb: (snapshot: StateSnapshot<Ctx>) => void): Unsubscribe;
    onError(cb: (err: RsdpError) => void): Unsubscribe;
    /** Test/operational aid: resolve once all pending reducer runs are idle. */
    settle(): Promise<void>;
}

interface ReducerSlot<Ctx extends Context> {
    reducer: Reducer<unknown, unknown, Ctx>;
    memory: MemoryMap<unknown>;
    debate: DebateBuffer<unknown>;
    queue: RunQueue;
    internal: unknown | null; // prev S
    translated: TranslatedState<unknown> | null;
}

export interface CreateEngineOptions<Ctx extends Context> {
    identity: NodeIdentity;
    slan: Slan;
    reducers: Reducer<any, any, Ctx>[];
    config: EngineConfig;
    clock?: Clock;
    observer?: (snapshot: StateSnapshot<Ctx>) => void;
}

class EngineImpl<Ctx extends Context> implements Engine<Ctx> {
    private readonly clock: Clock;
    private readonly fsm = new Fsm();
    private readonly slots = new Map<ReducerName, ReducerSlot<Ctx>>();
    private readonly convergedCbs = new Set<(s: StateSnapshot<Ctx>) => void>();
    private readonly errorCbs = new Set<(e: RsdpError) => void>();
    private steadyDebouncer!: Debouncer;
    private debateDebouncer!: Debouncer;
    private sweepTimer: ReturnType<Clock['setTimer']> | null = null;
    private unsub: Unsubscribe | null = null;

    constructor(private readonly opts: CreateEngineOptions<Ctx>) {
        this.clock = opts.clock ?? new SystemClock();
        for (const r of opts.reducers) {
            const slot: ReducerSlot<Ctx> = {
                reducer: r as Reducer<unknown, unknown, Ctx>,
                memory: new MemoryMap<unknown>(),
                debate: new DebateBuffer<unknown>(),
                queue: new RunQueue(() => Promise.resolve()),
                internal: null,
                translated: null,
            };
            slot.queue = new RunQueue(() => this.runShare(slot));
            this.slots.set(r.name, slot);
        }
    }

    private ctx(): Ctx {
        return { self: this.opts.identity.address } as Ctx;
    }

    async start(): Promise<void> {
        this.unsub = this.opts.slan.onMessage((m, from) => void this.onMessage(m, from));
        await this.opts.slan.init();

        this.debateDebouncer = new Debouncer(this.clock, this.opts.config.debounce, () => void this.runDebate());
        this.steadyDebouncer = new Debouncer(this.clock, this.opts.config.debounce, () => this.triggerAll());

        this.fsm.to(Phase.DEBATE);
        await this.opts.slan.broadcast({ type: 'HELLO', from: this.opts.identity.address });
        // Anchor the one-shot DEBATE window to the HELLO broadcast.
        this.debateDebouncer.notifyChange();

        this.sweepTimer = this.clock.setTimer(() => this.sweep(), this.opts.config.sweepIntervalMs);
    }

    async stop(): Promise<void> {
        await this.opts.slan.broadcast({
            type: 'CLOSE',
            from: this.opts.identity.address,
            closed: this.opts.identity.address,
        });
        if (this.sweepTimer !== null) this.clock.clearTimer(this.sweepTimer);
        this.debateDebouncer.cancel();
        this.steadyDebouncer.cancel();
        this.unsub?.();
        await this.opts.slan.close();
    }

    private async onMessage(msg: WireMessage, from: Address): Promise<void> {
        switch (msg.type) {
            case 'HELLO':
                // Reply with our current view as STATUS (full-consensus mode).
                await this.opts.slan.sendTo(from, {
                    type: 'STATUS',
                    from: this.opts.identity.address,
                    payloads: this.composite(),
                });
                return;
            case 'STATUS':
                this.ingest(msg, (slot, payload) => slot.debate.set(from, payload.value), this.debateDebouncer);
                return;
            case 'SHARE':
                this.ingest(
                    msg,
                    (slot, payload) => slot.memory.update(from, payload.value, payload.version, this.clock.now()),
                    this.steadyDebouncer,
                );
                return;
            case 'CLOSE': {
                const closed = msg.closed ?? from;
                let changed = false;
                for (const slot of this.slots.values()) changed = slot.memory.evict(closed) || changed;
                if (changed) this.steadyDebouncer.notifyChange();
                return;
            }
        }
    }

    private ingest(
        msg: WireMessage,
        apply: (slot: ReducerSlot<Ctx>, payload: ReducerPayload) => void,
        debouncer: Debouncer,
    ): void {
        if (!msg.payloads) return;
        let changed = false;
        for (const [name, payload] of Object.entries(msg.payloads)) {
            const slot = this.slots.get(name);
            if (!slot) continue;
            apply(slot, payload);
            changed = true;
        }
        if (changed) debouncer.notifyChange();
    }

    private async runDebate(): Promise<void> {
        for (const slot of this.slots.values()) {
            try {
                const batch = slot.debate.snapshot();
                slot.internal = await slot.reducer.runStatus(batch, this.ctx(), slot.internal);
                slot.translated = await slot.reducer.translate(slot.internal, null, this.ctx());
                slot.debate.clear();
            } catch (err) {
                this.emitError(slot.reducer.name, 'STATUS', err);
            }
        }
        this.fsm.to(Phase.IDLE);
        await this.opts.slan.broadcast({ type: 'SHARE', from: this.opts.identity.address, payloads: this.composite() });
        this.notifyConverged();
    }

    private triggerAll(): void {
        for (const slot of this.slots.values()) slot.queue.trigger();
    }

    private async runShare(slot: ReducerSlot<Ctx>): Promise<void> {
        try {
            const batch = slot.memory.snapshot().map(([, payload]) => payload);
            const prev = slot.internal;
            slot.internal = await slot.reducer.runShare(batch, this.ctx(), prev);
            const translated = await slot.reducer.translate(slot.internal, prev, this.ctx());
            slot.translated = translated;
            if (translated.changed) {
                await this.opts.slan.broadcast({
                    type: 'SHARE',
                    from: this.opts.identity.address,
                    payloads: this.composite(),
                });
            }
            this.notifyConverged();
        } catch (err) {
            this.emitError(slot.reducer.name, 'SHARE', err);
        }
    }

    private sweep(): void {
        const now = this.clock.now();
        let changed = false;
        for (const slot of this.slots.values())
            changed = slot.memory.sweepExpired(now, this.opts.config.ttlMs).length > 0 || changed;
        if (changed) this.steadyDebouncer.notifyChange();
        this.sweepTimer = this.clock.setTimer(() => this.sweep(), this.opts.config.sweepIntervalMs);
    }

    private composite(): Record<ReducerName, ReducerPayload> {
        const out: Record<ReducerName, ReducerPayload> = {};
        for (const [name, slot] of this.slots) {
            out[name] = { value: slot.translated?.value ?? null, version: this.clock.now() };
        }
        return out;
    }

    private snapshotView(): StateSnapshot<Ctx> {
        const slots = this.slots;
        return {
            get<V>(reducer: Reducer<unknown, V, Ctx>): TranslatedState<V> | null {
                return (slots.get(reducer.name)?.translated as TranslatedState<V> | undefined) ?? null;
            },
        };
    }

    stateOf<V>(reducer: Reducer<unknown, V, Ctx>): TranslatedState<V> | null {
        return (this.slots.get(reducer.name)?.translated as TranslatedState<V> | undefined) ?? null;
    }
    stateByName(name: ReducerName): TranslatedState<unknown> | null {
        return this.slots.get(name)?.translated ?? null;
    }
    onConverged(cb: (s: StateSnapshot<Ctx>) => void): Unsubscribe {
        this.convergedCbs.add(cb);
        return () => this.convergedCbs.delete(cb);
    }
    onError(cb: (e: RsdpError) => void): Unsubscribe {
        this.errorCbs.add(cb);
        return () => this.errorCbs.delete(cb);
    }
    async settle(): Promise<void> {
        await Promise.all([...this.slots.values()].map((s) => s.queue.idle()));
    }

    private notifyConverged(): void {
        const snap = this.snapshotView();
        this.opts.observer?.(snap);
        for (const cb of this.convergedCbs) cb(snap);
    }
    private emitError(reducer: string, stage: 'STATUS' | 'SHARE' | 'CLOSE', err: unknown): void {
        const wrapped =
            err instanceof RsdpError
                ? err
                : new PipelineError(err instanceof Error ? err.message : String(err), {
                      reducer,
                      stage: 'aggregator',
                      messageType: stage,
                  });
        for (const cb of this.errorCbs) cb(wrapped);
    }
}

export function createEngine<Ctx extends Context>(opts: CreateEngineOptions<Ctx>): Engine<Ctx> {
    return new EngineImpl<Ctx>(opts);
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run tests/engine/Engine.debate.test.ts` → PASS. `npm run typecheck` → clean. `npm run lint` → clean.

- [ ] **Step 5: Commit**

```bash
git add src/engine/Engine.ts tests/engine/Engine.debate.test.ts
git commit -m "feat(core): add engine with DEBATE bootstrap"
```

---

## Task 12: Engine — steady-state convergence (integration)

**Files:**

- Modify: `src/engine/Engine.ts` (only if a test reveals a gap — the Task 11 implementation already wires steady state)
- Test: `tests/engine/Engine.convergence.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/engine/Engine.convergence.test.ts
import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import { createEngine } from '../../src/engine/Engine.js';
import { defineReducer } from '../../src/reducer/Reducer.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

type V = string[];
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (batch) => [...new Set([self, ...batch.flat()])].sort(),
});
const membersReducer = (self: string) =>
    defineReducer<V, V, Context>('members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({ translate: (state) => ({ value: state, changed: true }) });

const cfg = { debounce: { delayMs: 10, maxWaitMs: 50 }, ttlMs: 10_000, sweepIntervalMs: 1_000 };

const settleAll = async (clock: FakeClock, engines: { settle(): Promise<void> }[]) => {
    for (let i = 0; i < 10; i++) {
        clock.advance(60);
        await Promise.all(engines.map((e) => e.settle()));
        await Promise.resolve();
    }
};

describe('Engine steady-state convergence', () => {
    it('three nodes converge on the full membership set', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const mk = (id: string) => {
            const r = membersReducer(id);
            const engine = createEngine<Context>({
                identity: { address: id },
                slan: new InMemorySlan(id, bus),
                reducers: [r],
                config: cfg,
                clock,
            });
            return { id, r, engine };
        };
        const nodes = [mk('a'), mk('b'), mk('c')];
        await Promise.all(nodes.map((n) => n.engine.start()));

        await settleAll(
            clock,
            nodes.map((n) => n.engine),
        );

        for (const n of nodes) {
            expect(n.engine.stateOf(n.r)?.value).toEqual(['a', 'b', 'c']);
        }
        await Promise.all(nodes.map((n) => n.engine.stop()));
    });

    it('a departing node is removed after CLOSE', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const mk = (id: string) => {
            const r = membersReducer(id);
            const engine = createEngine<Context>({
                identity: { address: id },
                slan: new InMemorySlan(id, bus),
                reducers: [r],
                config: cfg,
                clock,
            });
            return { id, r, engine };
        };
        const nodes = [mk('a'), mk('b'), mk('c')];
        await Promise.all(nodes.map((n) => n.engine.start()));
        await settleAll(
            clock,
            nodes.map((n) => n.engine),
        );

        await nodes[2]!.engine.stop(); // 'c' departs → broadcasts CLOSE
        await settleAll(clock, [nodes[0]!.engine, nodes[1]!.engine]);

        expect(nodes[0]!.engine.stateOf(nodes[0]!.r)?.value).toEqual(['a', 'b']);
        expect(nodes[1]!.engine.stateOf(nodes[1]!.r)?.value).toEqual(['a', 'b']);
        await Promise.all([nodes[0]!.engine.stop(), nodes[1]!.engine.stop()]);
    });
});
```

- [ ] **Step 2: Run to verify it fails (or surfaces a wiring gap)**

Run: `npx vitest run tests/engine/Engine.convergence.test.ts`
Expected: initially FAIL if any steady-state wiring is incomplete. Fix `Engine.ts` minimally until green (likely candidates: ensuring `runShare` re-broadcast actually re-triggers peers; ensuring CLOSE eviction notifies the debouncer — both already present in Task 11, so this test mainly validates the integration).

- [ ] **Step 3: Make it pass**

If failing, the most probable fix is preventing an infinite re-broadcast loop: a node must not re-broadcast when its translated state did not actually change. Tighten the reference translator in tests, or (better, in `Engine.ts`) only re-broadcast when `translated.changed` AND the composite differs from the last sent composite. Add a `lastSentComposite` guard in `runShare`:

```ts
// in EngineImpl: add a field
private lastSent: string | null = null;
// in runShare, replace the broadcast block:
if (translated.changed) {
    const composite = this.composite();
    const encoded = JSON.stringify(composite);
    if (encoded !== this.lastSent) {
        this.lastSent = encoded;
        await this.opts.slan.broadcast({ type: 'SHARE', from: this.opts.identity.address, payloads: composite });
    }
}
```

(Real reducers return `changed: false` at the fixpoint, but this guard makes convergence robust even with a naive always-`changed` translator.)

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run tests/engine/Engine.convergence.test.ts` → PASS. Then `npm test` → all green.

- [ ] **Step 5: Commit**

```bash
git add src/engine/Engine.ts tests/engine/Engine.convergence.test.ts
git commit -m "feat(core): converge steady state and prevent re-broadcast storms"
```

---

## Task 13: Edge cases — duplicate/out-of-order SHARE, isolated node, TTL

**Files:**

- Test: `tests/engine/Engine.edges.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/engine/Engine.edges.test.ts
import { describe, expect, it } from 'vitest';

import { FakeClock } from '../../src/clock/Clock.js';
import type { Context } from '../../src/domain/context.js';
import { createEngine } from '../../src/engine/Engine.js';
import { defineReducer } from '../../src/reducer/Reducer.js';
import type { Aggregator } from '../../src/reducer/pipeline/stages.js';
import { InMemoryBus, InMemorySlan } from '../../src/testing/InMemorySlan.js';

type V = string[];
const union = (self: string): Aggregator<V, V, Context> => ({
    aggregate: (b) => [...new Set([self, ...b.flat()])].sort(),
});
const r = (self: string) =>
    defineReducer<V, V, Context>('members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) => p.setAggregator({ aggregate: (b, _c, prev) => (prev ?? []).filter((id) => !b.includes(id)) }))
        .setTranslator({ translate: (state) => ({ value: state, changed: true }) });
const cfg = { debounce: { delayMs: 10, maxWaitMs: 50 }, ttlMs: 100, sweepIntervalMs: 20 };

describe('Engine edge cases', () => {
    it('an isolated node bootstraps to just itself at D_max', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const red = r('solo');
        const engine = createEngine<Context>({
            identity: { address: 'solo' },
            slan: new InMemorySlan('solo', bus),
            reducers: [red],
            config: cfg,
            clock,
        });
        await engine.start();
        clock.advance(50); // D_max
        await engine.settle();
        expect(engine.stateOf(red)?.value).toEqual(['solo']);
        await engine.stop();
    });

    it('drops a silent peer after TTL', async () => {
        const bus = new InMemoryBus();
        const clock = new FakeClock(0);
        const red = r('a');
        const engine = createEngine<Context>({
            identity: { address: 'a' },
            slan: new InMemorySlan('a', bus),
            reducers: [red],
            config: cfg,
            clock,
        });
        const peer = new InMemorySlan('ghost', bus);
        await peer.init();
        await engine.start();
        clock.advance(50);
        await engine.settle();

        // ghost sends one SHARE then goes silent
        await peer.sendTo('a', {
            type: 'SHARE',
            from: 'ghost',
            payloads: { members: { value: ['ghost'], version: 1 } },
        });
        clock.advance(20);
        await engine.settle();
        expect(engine.stateOf(red)?.value).toContain('ghost');

        // advance well past TTL → swept
        clock.advance(200);
        await engine.settle();
        expect(engine.stateOf(red)?.value).not.toContain('ghost');
        await engine.stop();
    });
});
```

- [ ] **Step 2: Run, then fix any gaps**

Run: `npx vitest run tests/engine/Engine.edges.test.ts`. If the TTL sweep does not re-run aggregation, confirm `sweep()` calls `this.steadyDebouncer.notifyChange()` when it evicts (present in Task 11). If the isolated-node case never aggregates, confirm `runDebate` aggregates even with an empty `debate` snapshot (it does — `union` includes `self`).

- [ ] **Step 3: Commit**

```bash
git add tests/engine/Engine.edges.test.ts src/engine/Engine.ts
git commit -m "test(core): cover isolated bootstrap and TTL eviction"
```

---

## Task 14: Reference reducer + public barrel + testing subpath export

**Files:**

- Create: `src/reducers/clusterMembers.ts`, `src/index.ts`
- Modify: `package.json` (add `./testing` subpath export)
- Test: `tests/reducers/clusterMembers.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
// tests/reducers/clusterMembers.test.ts
import { describe, expect, it } from 'vitest';

import type { Context } from '../../src/domain/context.js';
import { clusterMembersReducer } from '../../src/reducers/clusterMembers.js';

describe('clusterMembersReducer', () => {
    it('unions peer ids with self and removes departed on close', async () => {
        const r = clusterMembersReducer('a');
        const ctx: Context = { self: 'a' };
        const status = await r.runStatus([['b'], ['c']], ctx, null);
        expect(status).toEqual(['a', 'b', 'c']);
        const closed = await r.runClose(['b'], ctx, ['a', 'b', 'c']);
        expect(closed).toEqual(['a', 'c']);
    });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run tests/reducers/clusterMembers.test.ts` → FAIL.

- [ ] **Step 3: Implement the reference reducer**

```ts
// src/reducers/clusterMembers.ts
import type { Context } from '../domain/context.js';
import { defineReducer } from '../reducer/Reducer.js';
import type { Aggregator } from '../reducer/pipeline/stages.js';

export type MembersView = string[];

const union = (self: string): Aggregator<MembersView, MembersView, Context> => ({
    aggregate: (batch) => [...new Set([self, ...batch.flat()])].sort(),
});

/** Reference reducer: maintains the sorted set of cluster member ids. */
export function clusterMembersReducer(self: string) {
    return defineReducer<MembersView, MembersView, Context>('cluster-members')
        .status((p) => p.setAggregator(union(self)))
        .share((p) => p.setAggregator(union(self)))
        .close((p) =>
            p.setAggregator({ aggregate: (departed, _c, prev) => (prev ?? []).filter((id) => !departed.includes(id)) }),
        )
        .setTranslator({
            translate: (state, prev) => ({ value: state, changed: JSON.stringify(state) !== JSON.stringify(prev) }),
        });
}
```

- [ ] **Step 4: Implement the public barrel**

```ts
// src/index.ts
export type { Address, NodeIdentity, ReducerName } from './domain/address.js';
export type { Context } from './domain/context.js';
export type { MessageType, ReducerPayload, WireMessage } from './domain/message.js';
export type { PeerRecord } from './domain/peer.js';
export type { TranslatedState } from './domain/state.js';
export { ConfigError, GuardRejected, PipelineError, RsdpError, SlanError } from './errors.js';
export { SystemClock } from './clock/Clock.js';
export type { Clock, TimerHandle } from './clock/Clock.js';
export { createEngine } from './engine/Engine.js';
export type { CreateEngineOptions, Engine, EngineConfig, StateSnapshot } from './engine/Engine.js';
export { defineReducer, Reducer } from './reducer/Reducer.js';
export type { Translator } from './reducer/Reducer.js';
export type {
    Aggregator,
    ExceptionFilter,
    Guard,
    Interceptor,
    Middleware,
    Normalizer,
} from './reducer/pipeline/stages.js';
export type { Slan, Unsubscribe } from './slan/Slan.js';
export { clusterMembersReducer } from './reducers/clusterMembers.js';
export type { MembersView } from './reducers/clusterMembers.js';
```

- [ ] **Step 5: Add the `./testing` subpath export to package.json**

In `core/package.json`, extend `exports`:

```json
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js"
    },
    "./testing": {
      "types": "./dist/testing/InMemorySlan.d.ts",
      "import": "./dist/testing/InMemorySlan.js"
    }
  },
```

- [ ] **Step 6: Run to verify it passes**

Run: `npx vitest run tests/reducers/clusterMembers.test.ts` → PASS. Then full gate:
`npm test` (all green) · `npm run typecheck` (clean) · `npm run lint` (clean) · `npm run build` (emits `dist/`) · `npm run format:check` (clean).

- [ ] **Step 7: Commit**

```bash
git add src/reducers src/index.ts package.json tests/reducers
git commit -m "feat(core): add reference cluster-members reducer and public API surface"
```

---

## Self-review notes (for the implementer)

- **Spec coverage:** Tasks 1–14 cover every spec section — domain/errors (§3,§10), Clock (§8.3), Debouncer (§8.3), RunQueue (§8.3), MemoryMap (§8.1), DebateBuffer (§8.1), pipeline + stages (§6), Reducer builder (§6), Slan + InMemorySlan (§9), Fsm (§8.2), Engine DEBATE + steady + edges (§5,§7,§8.2), reference reducer + barrel + `./testing` export (§4,§11).
- **Type consistency:** `Context` is `{ self: Address }`; reducers are `Reducer<S, V, Ctx extends Context>`; STATUS/SHARE raw type is `V`, CLOSE raw type is `Address`; `stateOf`/`StateSnapshot.get` key on `reducer.name`; composite payloads are `Record<ReducerName, { value: unknown; version: number }>`.
- **Known simplification to revisit post-M1:** the composite `version` uses `clock.now()` as a monotonic stamp; if two updates land in the same `FakeClock` tick, version equality makes the second a no-op. Acceptable for M1 (DEBATE advances the clock between rounds); a per-reducer monotonic counter is the clean follow-up.
- **NodeNext discipline:** every relative import ends in `.js`; type-only imports use `import type`. Run `npm run lint && npm run typecheck` after each task.
