import type { Clock, TimerHandle } from '../../clock/Clock.js';

/** Anything that can evict its TTL-expired entries and report whether it did. */
export interface Sweepable {
    sweepExpired(now: number, ttlMs: number): boolean;
}

/**
 * Drives the periodic TTL eviction sweep on the injected clock. Every `intervalMs` it
 * asks the target to evict peers unseen for longer than `ttlMs`; if any were removed it
 * fires `onEvicted` (the engine schedules a re-aggregation). Owns only its own timer.
 */
export class TtlSweeper {
    private timer: TimerHandle | null = null;

    constructor(
        private readonly clock: Clock,
        private readonly target: Sweepable,
        private readonly ttlMs: number,
        private readonly intervalMs: number,
        private readonly onEvicted: () => void,
    ) {}

    start(): void {
        this.arm();
    }

    stop(): void {
        if (this.timer !== null) {
            this.clock.clearTimer(this.timer);
            this.timer = null;
        }
    }

    private arm(): void {
        this.timer = this.clock.setTimer(() => this.tick(), this.intervalMs);
    }

    private tick(): void {
        if (this.target.sweepExpired(this.clock.now(), this.ttlMs)) this.onEvicted();
        this.arm();
    }
}
