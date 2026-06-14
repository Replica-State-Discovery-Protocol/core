# @rsdp/core — Engine & Reducer Framework Design

**Date:** 2026-06-13
**Status:** Approved (brainstorming) — pending implementation plan
**Milestone:** M1 — framework + convergence core

---

## 1. Context & goals

`@rsdp/core` is a clean, framework-grade reimplementation of the RSDP engine, extracted from the year-old `workbench/src/core` prototype. The prototype's **fluent chaining API** is kept as the surface; its internals are rebuilt.

This design realises the **authoritative memory-based convergence model** from the protocol docs (`docs/03-protocol-phases/`, `docs/07-state-convergence/`): per-peer memory holding SHARE perspectives, a bounded DEBATE that re-gathers perspectives, debounced event-driven aggregation, and TTL eviction. Because consistency is **eventual**, a converged cluster does not go silent: each node **periodically resyncs** — re-broadcasting `HELLO` to redo the discovery handshake, re-gathering peers' STATUS and re-deriving its view — so liveness stays fresh and late joiners / missed updates / healed partitions are folded in.

Goals: strong end-to-end typing, modular class-based components with real dependency injection, deterministic and fully testable convergence, transport-agnostic core.

## 2. Scope

**In scope (M1):**

- Typed reducer-pipeline framework (the chaining API).
- Memory-based convergence engine (per-reducer `Σ`, DEBATE buffer, debounce, TTL eviction, snapshot + single-flight run queue).
- The `Slan` transport **port** (interface only).
- An in-process `InMemorySlan` for tests and a runnable local cluster.
- Full-consensus single mode.

**Deferred (not M1):**

- Standby consensus (CONSENSUS/FOLLOWER) modes and split routing.
- Network-management / federation layer.
- A real transport (`@rsdp/amqp-slan`, separate repo).

## 3. Key decisions (with rationale)

1. **Core-owned `Σ`, reducers are pure functions.** The engine owns all convergence machinery and hands each reducer an immutable snapshot; a reducer is `f(snapshot, ctx, prevState) → state`. Convergence logic is written once; reducers are trivially unit-testable. (Fixes prototype: no per-peer memory existed.)
2. **Snapshot + single-flight steady run cycle; no OS mutex.** Node is single-threaded; the only real hazard is async pipelines interleaving on shared state. We take an immutable `Σ` snapshot per reducer and serialize the whole steady-state cycle through one `RunQueue`, so overlapping triggers coalesce into one trailing cycle. (Fixes prototype: buggy `TimeoutMutex` that threw from a `setTimeout`.)
3. **Class-based stages passed as instances.** Stages are classes implementing typed interfaces; the builder accepts configured **instances**, so DI is constructor injection. (Fixes prototype: `new () => T` auto-instantiation blocked DI.)
4. **Closure-grouped reducer builder.** `defineReducer(...).status(p => …).share(p => …).close(p => …).setTranslator(…)` — groups each pipeline and reads as one expression.
5. **Typed accessor for engine state (Option C).** Read state by passing the reducer object: `engine.stateOf(reducer): TranslatedState<V>`. Type-safe at point of use, no string-keyed access, simple engine generics, dynamic registration intact. A loose `stateByName()` remains only as an escape hatch. (Fixes prototype: `Record<string, unknown>` erasure + casts.)

## 4. Module layout

```
src/
  domain/                 pure types, zero logic
    address.ts            Address, NodeIdentity, metadata
    message.ts            MessageType, WireMessage (composite payloads)
    peer.ts               PeerRecord σ_i(j) = { payload, version, lastSeenAt }
    state.ts              TranslatedState<V> = { value, changed }
    context.ts            Context
  slan/
    Slan.ts               transport PORT (init/close/broadcast/sendTo/onMessage/address)
  reducer/
    Reducer.ts            public face: Reducer<S,V,Ctx> runtime class + Translator + defineReducer()
    internal/ReducerBuilder.ts  fluent builder behind defineReducer (closure-grouped); validates + assembles
    internal/PipelineBuilder.ts per-message-type pipeline accumulator + Configure callback type
    pipeline/
      Pipeline.ts         runner: middleware → guard → interceptor → normalizer → aggregator → translator → exception filter
      stages.ts           the 7 stage interfaces
  engine/
    Engine.ts             public interface + thin orchestrator: lifecycle/FSM, message routing, timers, wiring
    internal/ReducerSlot.ts   one reducer's Σ/DEBATE buffer/version + per-slot convergence (runDebate/runShare/ingest/evict/sweep)
    internal/SlotRegistry.ts  slots map + wire composition (composite) + state access (stateOf/stateByName/snapshotView) + sweepExpired
    internal/OutboundChannel.ts SLAN sends (hello/share/close/status) with uniform transport-error routing
    internal/ErrorChannel.ts  onError subscribers + error wrapping + exception-filter routing
    internal/TtlSweeper.ts    periodic TTL eviction sweep (clock timer → registry.sweepExpired → notify on change)
    memory/MemoryMap.ts   Σ per reducer: update / evict / sweepExpired / snapshot
    memory/DebateBuffer.ts transient per-round STATUS buffer (cleared after each DEBATE)
    schedule/Debouncer.ts dual-trigger δ + D_max scheduler (clock-driven)
    schedule/RunQueue.ts  single-flight serialized runner (drives the one steady convergence cycle)
    phases/Fsm.ts         INITIAL → DEBATE → IDLE ↔ DEBATE(resync); IDLE → SHARE/CLOSE
  clock/Clock.ts          injectable Clock (SystemClock + FakeClock)
  testing/InMemorySlan.ts in-process SLAN, exported via "@rsdp/core/testing"
  index.ts                public barrel
```

## 5. Engine ↔ reducer contract

- The **engine** owns, per reducer: one `MemoryMap` (Σ over peers) and one `DebateBuffer`; plus the shared `Debouncer`, a single steady-cycle `RunQueue`, TTL sweep, snapshotting.
- A **reducer** is pure: it receives an immutable snapshot of the relevant peer payloads + `ctx` + `prevState`, returns new state, which the translator condenses to a wire value + `changed` flag.
- The pipeline's "raw message batch" **is** the current snapshot of non-evicted peer payloads — unifying the docs' pipeline model with the memory-map model.

### 5.1 What crosses the wire (resolves the raw-type question)

A reducer's **wire payload is its translated view `V`** — that is exactly what the translator produces and what peers exchange. The internal state `S` may be richer than `V`; only `V` crosses the wire and only `V` is what a reducer observes _from peers_. Therefore the per-pipeline raw input types are fixed, not free parameters:

- **STATUS** pipeline raw input: `V[]` (neighbours' views, gathered each DEBATE round).
- **SHARE** pipeline raw input: `V[]` (peers' views, in steady state).
- **CLOSE** pipeline raw input: `Address` (the departed peer).

The loop per reducer is: peers' `V[]` → aggregate → `S` → translate → `V` → broadcast. So `PeerRecord` and `MemoryMap` are parameterised by `V`, and the composite message payload value is `V` (erased to `unknown` at the wire boundary, since the engine knows the reducer→`V` mapping).

## 6. The typed reducer-pipeline API

### 6.1 Stage interfaces (classes implement these; passed as instances)

```ts
type PipelineRun<R, S, Ctx> = (batch: R[], ctx: Ctx, prev: S | null) => Promise<S>;
type Aggregate<M, S, Ctx> = (batch: M[], ctx: Ctx, prev: S | null) => Promise<S>;

interface Middleware<R, S, Ctx> {
    wrap(next: PipelineRun<R, S, Ctx>): PipelineRun<R, S, Ctx>;
}
interface Guard<R, S, Ctx> {
    check(batch: R[], ctx: Ctx, prev: S | null): boolean | Promise<boolean>;
} // false ⇒ GuardRejected
interface Interceptor<M, S, Ctx> {
    wrap(next: Aggregate<M, S, Ctx>): Aggregate<M, S, Ctx>;
}
interface Normalizer<R, M, Ctx> {
    normalize(batch: R[], ctx: Ctx): M[] | Promise<M[]>;
}
interface Aggregator<M, S, Ctx> {
    aggregate(batch: M[], ctx: Ctx, prev: S | null): S | Promise<S>;
} // required
interface Translator<S, V, Ctx> {
    translate(state: S | null, prev: S | null, ctx: Ctx): TranslatedState<V> | Promise<TranslatedState<V>>;
}
interface ExceptionFilter<Ctx> {
    handle(err: unknown, ctx: Ctx): void | Promise<void>;
}
```

Execution order within a pipeline: middleware (outermost, wraps everything, sees exceptions) → guards (all must pass) → interceptors (wrap the aggregation) → normalizer (raw `R[]` → normalized `M[]`; default identity) → aggregator (required) → translator (on success) / exception filters (on error).

### 6.2 Builder

```ts
defineReducer<S, V, Ctx>(name: string): ReducerBuilder<S, V, Ctx>;

interface ReducerBuilder<S, V, Ctx> {
  status(fn: (p: PipelineBuilder<V, V, S, Ctx>) => PipelineBuilder<V, unknown, S, Ctx>): this;
  share(fn:  (p: PipelineBuilder<V, V, S, Ctx>) => PipelineBuilder<V, unknown, S, Ctx>): this;
  close(fn:  (p: PipelineBuilder<Address, Address, S, Ctx>) => PipelineBuilder<Address, unknown, S, Ctx>): this;
  setTranslator(t: Translator<S, V, Ctx>): Reducer<S, V, Ctx>; // finalizes; verify() runs at engine build
}

interface PipelineBuilder<R, M, S, Ctx> {
  addMiddleware(m: Middleware<R, S, Ctx>): this;
  addGuard(g: Guard<R, S, Ctx>): this;
  addInterceptor(i: Interceptor<M, S, Ctx>): this;
  setNormalizer<M2>(n: Normalizer<R, M2, Ctx>): PipelineBuilder<R, M2, S, Ctx>; // narrows M
  setAggregator(a: Aggregator<M, S, Ctx>): this;                                // required
  addExceptionFilter(f: ExceptionFilter<Ctx>): this;
}
```

Per §5.1 the raw input `R` is fixed: `V` for STATUS/SHARE, `Address` for CLOSE. `M` defaults to `R` until `setNormalizer` narrows it.

Usage:

```ts
const members = defineReducer<MembersState, string[], Ctx>('cluster-members')
    .status((p) => p.setAggregator(new UnionMembers()))
    .share((p) => p.addGuard(new NonEmpty()).setAggregator(new PopularVote(sha256)))
    .close((p) => p.setAggregator(new RemoveDeparted()))
    .setTranslator(new MembersTranslator());
```

### 6.3 `changed` detection

The translator returns `{ value, changed }`. `changed` is computed by a translator-supplied comparator (default: reference/shallow equality), **not** `JSON.stringify`. Only `changed === true` triggers a re-broadcast.

## 7. Engine API & state typing (Option C)

```ts
interface Clock {
    now(): number;
    setTimer(fn: () => void, ms: number): TimerHandle;
    clearTimer(h: TimerHandle): void;
}

interface EngineConfig {
    debounce: { delayMs: number; maxWaitMs: number }; // δ and D_max
    ttlMs: number; // θ — per-peer eviction timeout
    sweepIntervalMs: number; // TTL sweep cadence
    resyncIntervalMs: number; // periodic re-HELLO cadence; MUST be < ttlMs
}

function createEngine<Ctx>(opts: {
    identity: NodeIdentity;
    slan: Slan;
    reducers: Reducer<any, any, Ctx>[];
    config: EngineConfig;
    clock?: Clock; // defaults to SystemClock
    observer?: (snapshot: StateSnapshot<Ctx>) => void;
}): Engine<Ctx>;

interface Engine<Ctx> {
    start(): Promise<void>; // broadcast HELLO, begin DEBATE
    stop(): Promise<void>; // broadcast CLOSE, close SLAN, clear timers
    stateOf<V>(reducer: Reducer<unknown, V, Ctx>): TranslatedState<V> | null; // typed by reducer arg
    stateByName(name: string): TranslatedState<unknown> | null; // loose escape hatch
    onConverged(cb: (snapshot: StateSnapshot<Ctx>) => void): Unsubscribe;
    onError(cb: (err: RsdpError) => void): Unsubscribe;
}

interface StateSnapshot<Ctx> {
    get<V>(reducer: Reducer<unknown, V, Ctx>): TranslatedState<V> | null;
}
```

The view type `V` rides in on the reducer argument — no variadic-tuple generics, no string-keyed access, dynamic registration works.

## 8. Convergence engine internals

### 8.1 Memory

```ts
type PeerRecord<P> = { payload: P; version: number; lastSeenAt: number }; // σ_i(j)

class MemoryMap<P> {
    // Σ for one reducer
    update(addr: Address, payload: P, version: number, now: number): boolean; // version-gated; returns "changed"
    evict(addr: Address): boolean; // explicit CLOSE
    sweepExpired(now: number, ttlMs: number): Address[]; // TTL eviction; returns evicted
    snapshot(): ReadonlyArray<readonly [Address, P]>; // immutable pipeline input
}
// DebateBuffer<P>: transient Map<Address, P>, refilled each DEBATE round, cleared after.
```

The engine holds `Map<ReducerName, MemoryMap<unknown>>` and `Map<ReducerName, DebateBuffer<unknown>>`.

### 8.2 Run lifecycle

- **DEBATE (on `start()` and on every resync):** broadcast `HELLO` → incoming `STATUS` fills each reducer's DebateBuffer → a debounce anchored to the HELLO time fires (`min(now+δ, helloAt+D_max)`) → each reducer's STATUS pipeline runs over the **union of its DebateBuffer snapshot and its `Σ` snapshot** → `s_i*` → engine broadcasts one composite `SHARE(s_i*)` → buffers dropped → IDLE. Converging over the union (this round's freshly-pulled perspectives **and** retained state data) means a peer briefly silent for one round is held by `Σ` until its TTL genuinely lapses. An isolated node still fires at `D_max` with only its own state. The FSM allows `IDLE → DEBATE` for the recurring rounds; change detection compares against the prior converged state, so an unchanged round does not bump versions.
- **Steady state (IDLE):** each incoming `SHARE` splits its composite payload and version-gates each reducer's `Σ` slot for `from` → schedules the steady-state debounce → on fire, one single-flight cycle recomputes **every** reducer over its `Σ` snapshot → `s_i'` → translate; if **any** reducer's view `changed`, the engine broadcasts exactly **one** fresh composite `SHARE` (messaging is engine-level, never per reducer), then notifies the observer once.
- **Periodic resync (every `resyncIntervalMs`, while IDLE):** re-broadcast `HELLO` and re-anchor a fresh DEBATE round. This redoes the discovery handshake (peers reply `STATUS`), re-derives the view, and — because the resulting `SHARE` carries the unchanged version when nothing moved — refreshes this node's liveness in every peer's `Σ` (equal-version path, §8.4). A round in flight is not re-entered (the FSM guards `IDLE → DEBATE`). This is the protocol's eventual-consistency mechanism: no node goes silent, so TTL only evicts genuinely-departed peers, and late joiners / missed updates / healed partitions converge.
- **CLOSE / TTL:** `CLOSE` → `Σ.evict(from)` for every reducer; a periodic sweep (`sweepIntervalMs`, on the clock) calls `sweepExpired` (`θ` must exceed `resyncIntervalMs`, so a live peer is reproven before it could expire). Any path that actually removes a peer schedules a debounced re-run.

### 8.3 Scheduling primitives

- **`Debouncer`** — dual trigger: fires at `min(now + δ, firstChangeAt + D_max)`, driven by the injected `Clock`. Prevents both thrash (δ coalescing) and indefinite stall (D_max cap — the prototype lacked this).
- **`RunQueue` (one steady cycle)** — single-flight. A trigger arriving mid-run sets a dirty flag; exactly one follow-up cycle is scheduled after the current completes, guaranteeing a clean `prevState → state` handoff over a consistent snapshot and coalescing bursts into a single composite SHARE.

### 8.4 Versioning

Each reducer payload carries a monotonic `version`. `MemoryMap.update` ignores payloads with `version` **<** the stored one (stale/out-of-order — no-op, no liveness refresh). An **equal**-version payload is a periodic resync re-broadcast: it refreshes `lastSeenAt` (keeping the live peer past TTL) but reports no state change, so it triggers no recompute. Only a strictly greater version stores a new payload and reports `changed`.

## 9. Transport: SLAN port & messages

```ts
interface Slan {
    readonly address: Address;
    init(): Promise<void>;
    close(): Promise<void>;
    broadcast(msg: WireMessage): Promise<void>;
    sendTo(target: Address, msg: WireMessage): Promise<void>;
    onMessage(handler: (msg: WireMessage, from: Address) => void): Unsubscribe;
}

type MessageType = 'HELLO' | 'STATUS' | 'SHARE' | 'CLOSE';
interface WireMessage {
    type: MessageType;
    from: Address;
    payloads?: Record<ReducerName, { value: unknown; version: number }>; // STATUS/SHARE composite
    closed?: Address; // CLOSE
}
```

One message carries every reducer's payload (composite), keyed by reducer name. The receiver splits it and updates each reducer's `Σ`/buffer independently. `@rsdp/core/testing` ships **`InMemorySlan`** — a shared in-process bus — used by the test suite and a runnable local multi-node cluster.

## 10. Error handling

- Typed hierarchy: `RsdpError` → `GuardRejected`, `PipelineError` (carries `reducer` + `stage` + `messageType`), `ConfigError`, `SlanError`.
- `verify()` runs at engine build: every reducer must have STATUS/SHARE/CLOSE aggregators and a translator, else `ConfigError` (fail fast).
- A failing pipeline run is routed to that reducer's `ExceptionFilter`s; if still unhandled it surfaces on `engine.onError` and the node **keeps converging**. One bad run never crashes the engine.
- No timers throw (the prototype's unhandled `setTimeout` throw is designed out — `Debouncer`/`Clock` never throw into the void).

## 11. Testing strategy

- **Vitest**, deterministic (no real timers/sleeps).
- **Reducers** tested as pure functions: snapshot in → state out.
- **Convergence** tested with `InMemorySlan` + `FakeClock`: spin up N in-process nodes on one bus, advance the clock explicitly, assert all nodes converge `stateOf(reducer)`. Cases: δ/D_max coalescing, TTL eviction, CLOSE, duplicate/out-of-order SHARE, isolated-node bootstrap, multi-reducer composite messages, periodic resync (members survive past TTL; a Σ peer retained across resync rounds then evicted once TTL lapses).
- **Reference reducers** (`UnionMembers` / cluster-members, and a cluster-position reducer) double as fixtures and living examples.

## 12. Mapping to authoritative docs

| Doc                                                                  | Realised by                        |
| -------------------------------------------------------------------- | ---------------------------------- |
| `03-protocol-phases` (DEBATE + periodic resync, SHARE steady, CLOSE) | §8.2 run lifecycle, `Fsm`          |
| `05-reducer-pipelines` (7-stage pipeline)                            | §6 stages + `Pipeline` runner      |
| `07-state-convergence/memory-based-convergence` (Σ, δ/D_max, TTL θ)  | §8 `MemoryMap`, `Debouncer`, sweep |
| `04-layered-architecture` (SLAN port, pluggable carrier)             | §9 `Slan` port + `InMemorySlan`    |

## 13. Prototype weaknesses addressed

1. No per-peer memory → core-owned `Σ` per reducer (§8.1).
2. Type erasure (`Record<string, unknown>` + casts) → typed accessor, Option C (§7).
3. `new () => T` blocked DI → class instances (§6).
4. Debounce had no max-wait (could stall) → dual-trigger δ/D_max (§8.3).
5. Buggy `TimeoutMutex` throwing from a timer → no OS mutex; snapshot + single-flight queue (§8.3, decision 2).
6. Half-wired phase machine / aggregation bypassing it → explicit lifecycle (§8.2).
7. Windowed resync-SESSION → lightweight periodic re-HELLO resync layered on event-driven memory-based convergence (per authoritative docs).
8. `JSON.stringify` change detection → comparator-based `changed` (§6.3).

## 14. Open items / deferred

- Real transport (`@rsdp/amqp-slan`) implements the §9 port later.
- Standby modes, federation — future milestones.
- `Context` shape is reducer-defined; the engine threads it through unchanged (carrier/identity data merged in at message receipt).
