import type { Clock } from '../clock/Clock.js';
import { SystemClock } from '../clock/Clock.js';
import type { NodeIdentity, ReducerName } from '../domain/address.js';
import type { Context } from '../domain/context.js';
import type { TranslatedState } from '../domain/state.js';
import type { RsdpError } from '../errors.js';
import type { Reducer } from '../reducer/Reducer.js';
import type { Slan, Unsubscribe } from '../slan/Slan.js';
import { Coordinator } from './internal/Coordinator.js';
import { ErrorChannel } from './internal/ErrorChannel.js';
import { InboundRouter } from './internal/InboundRouter.js';
import { OutboundChannel } from './internal/OutboundChannel.js';
import { ReducerSlot } from './internal/ReducerSlot.js';
import { SlotRegistry } from './internal/SlotRegistry.js';
import { TtlSweeper } from './internal/TtlSweeper.js';
import type { DebounceConfig } from './schedule/Debouncer.js';

export interface EngineConfig {
    /**
     * Aggregation debounce: the quiet-gap `δ` (`delayMs`) and hard cap `D_max`
     * (`maxWaitMs`). Each burst of incoming messages coalesces into a single reducer
     * run that fires once arrivals stay quiet for `δ`, or at `D_max` after the first
     * change — whichever comes first. Anchored to the HELLO broadcast for the DEBATE
     * round and to `Σ` changes in steady state.
     */
    debounce: DebounceConfig;
    /**
     * Per-peer eviction timeout `θ`: a peer whose last SHARE was seen longer ago than
     * this is swept from `Σ`. MUST be `> resyncIntervalMs`, so a live peer is reproven
     * by its periodic resync before it could expire (otherwise healthy peers flap).
     */
    ttlMs: number;
    /**
     * How often the TTL sweep runs (clock-driven). Bounds how long an already-expired
     * peer lingers in `Σ` before removal; smaller means tighter eviction latency at the
     * cost of more frequent wake-ups. Independent of `θ` itself.
     */
    sweepIntervalMs: number;
    /**
     * Periodic resync period: every `resyncIntervalMs` a node re-broadcasts HELLO,
     * redoing the discovery handshake — peers reply with STATUS, the node re-derives
     * its view (fresh DEBATE round) and re-broadcasts SHARE. This re-gathers
     * perspectives (folding in late joiners / missed updates / healed partitions) and
     * refreshes liveness. MUST be `< ttlMs`, or TTL would evict healthy members
     * between resyncs.
     */
    resyncIntervalMs: number;
}

export interface StateSnapshot<Ctx extends Context> {
    get<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null;
}

export interface Engine<Ctx extends Context> {
    start(): Promise<void>;
    stop(): Promise<void>;
    stateOf<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null;
    stateByName(name: ReducerName): TranslatedState<unknown> | null;
    onConverged(cb: (snapshot: StateSnapshot<Ctx>) => void): Unsubscribe;
    onError(cb: (err: RsdpError) => void): Unsubscribe;
    /** Test/operational aid: resolve once all pending reducer runs are idle. */
    settle(): Promise<void>;
}

export interface CreateEngineOptions<Ctx extends Context> {
    identity: NodeIdentity;
    slan: Slan;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    reducers: Reducer<any, any, Ctx>[];
    config: EngineConfig;
    clock?: Clock;
    observer?: (snapshot: StateSnapshot<Ctx>) => void;
}

/**
 * Facade + lifecycle. Constructs and wires the collaborators, sequences start/stop, and
 * exposes the state-facing API. It holds no protocol logic of its own: inbound message
 * handling lives in {@link InboundRouter}, the convergence lifecycle in {@link Coordinator},
 * TTL eviction in {@link TtlSweeper}, transport in {@link OutboundChannel}, error routing
 * in {@link ErrorChannel}, and per-reducer state in {@link SlotRegistry}/{@link ReducerSlot}.
 */
class EngineImpl<Ctx extends Context> implements Engine<Ctx> {
    private readonly clock: Clock;
    private readonly registry = new SlotRegistry<Ctx>();
    private readonly errors = new ErrorChannel<Ctx>();
    private readonly outbound: OutboundChannel;
    private readonly coordinator: Coordinator<Ctx>;
    private readonly router: InboundRouter<Ctx>;
    private readonly sweeper: TtlSweeper;

    private unsub: Unsubscribe | null = null;

    constructor(private readonly opts: CreateEngineOptions<Ctx>) {
        this.clock = opts.clock ?? new SystemClock();
        this.outbound = new OutboundChannel(opts.slan, opts.identity.address, this.errors);

        for (const r of opts.reducers) {
            this.registry.add(new ReducerSlot<Ctx>(r as Reducer<unknown, unknown, Ctx>));
        }

        this.coordinator = new Coordinator<Ctx>({
            clock: this.clock,
            registry: this.registry,
            outbound: this.outbound,
            errors: this.errors,
            self: opts.identity.address,
            debounce: opts.config.debounce,
            resyncIntervalMs: opts.config.resyncIntervalMs,
            observer: opts.observer,
        });
        this.router = new InboundRouter<Ctx>(this.registry, this.outbound, this.clock, this.coordinator);
        this.sweeper = new TtlSweeper(this.clock, this.registry, opts.config.ttlMs, opts.config.sweepIntervalMs, () =>
            this.coordinator.scheduleSteady(),
        );
    }

    async start(): Promise<void> {
        this.unsub = this.opts.slan.onMessage((m, from) => void this.router.handle(m, from));

        await this.opts.slan.init();
        await this.coordinator.start();

        this.sweeper.start();
    }

    async stop(): Promise<void> {
        await this.outbound.close();

        this.sweeper.stop();
        this.coordinator.stop();
        this.unsub?.();

        await this.opts.slan.close();
    }

    stateOf<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null {
        return this.registry.stateOf(reducer);
    }
    stateByName(name: ReducerName): TranslatedState<unknown> | null {
        return this.registry.stateByName(name);
    }

    onConverged(cb: (s: StateSnapshot<Ctx>) => void): Unsubscribe {
        return this.coordinator.onConverged(cb);
    }
    onError(cb: (e: RsdpError) => void): Unsubscribe {
        return this.errors.subscribe(cb);
    }

    settle(): Promise<void> {
        return this.coordinator.settle();
    }
}

export function createEngine<Ctx extends Context>(opts: CreateEngineOptions<Ctx>): Engine<Ctx> {
    return new EngineImpl<Ctx>(opts);
}
