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
        clearTimeout(handle);
    }
}

interface Scheduled {
    id: TimerHandle;
    fireAt: number;
    fn: () => void;
}

/**
 * Deterministic virtual clock.
 *
 * Pending timers live in a binary min-heap ordered by `(fireAt, id)`, and cancellation is
 * lazy — a cleared timer is dropped from the live map and skipped when it surfaces. An
 * array scan per operation is fine for a handful of timers but turns a long simulation
 * quadratic in event count: at 100 simulated nodes the array version spent 291 s on a run
 * the heap completes in a fraction of that, because every tick re-scanned thousands of
 * pending deliveries.
 */
export class FakeClock implements Clock {
    private current: number;
    private seq = 0;
    private heap: Scheduled[] = [];
    private readonly live = new Map<TimerHandle, Scheduled>();

    constructor(start = 0) {
        this.current = start;
    }

    now(): number {
        return this.current;
    }

    setTimer(fn: () => void, ms: number): TimerHandle {
        const id = ++this.seq;
        const entry: Scheduled = { id, fireAt: this.current + Math.max(0, ms), fn };
        this.live.set(id, entry);
        this.push(entry);
        return id;
    }

    clearTimer(handle: TimerHandle): void {
        this.live.delete(handle);
    }

    /** Advance virtual time, firing every timer whose deadline is reached, in order. */
    advance(ms: number): void {
        const target = this.current + ms;
        while (this.tickNext(target));
        this.current = target;
    }

    /**
     * The absolute time of the earliest pending timer, or null if none is scheduled.
     * Lets a driver step from event to event instead of guessing a quantum.
     */
    nextDeadline(): number | null {
        return this.peek()?.fireAt ?? null;
    }

    /**
     * Fire exactly the next pending timer, landing `now()` precisely on its deadline;
     * returns false if there was none (or none at or before `notAfter`).
     *
     * Event-driven stepping: a driver that ticks deadline to deadline observes every effect
     * at its exact virtual instant — no quantization error — and pays per timer firing
     * rather than per unit of simulated time.
     */
    tickNext(notAfter = Infinity): boolean {
        const next = this.peek();
        if (!next || next.fireAt > notAfter) return false;

        this.pop();
        this.live.delete(next.id);
        this.current = next.fireAt;
        next.fn();
        return true;
    }

    /** Earliest live timer, discarding cancelled entries as they surface. */
    private peek(): Scheduled | undefined {
        while (this.heap.length > 0 && !this.live.has(this.heap[0]!.id)) this.pop();
        return this.heap[0];
    }

    /** Ordered by deadline, then insertion, so ties fire in a reproducible order. */
    private before(a: Scheduled, b: Scheduled): boolean {
        return a.fireAt < b.fireAt || (a.fireAt === b.fireAt && a.id < b.id);
    }

    private push(entry: Scheduled): void {
        this.heap.push(entry);
        let i = this.heap.length - 1;
        while (i > 0) {
            const parent = (i - 1) >> 1;
            if (!this.before(this.heap[i]!, this.heap[parent]!)) break;
            [this.heap[i], this.heap[parent]] = [this.heap[parent]!, this.heap[i]!];
            i = parent;
        }
    }

    private pop(): void {
        const last = this.heap.pop();
        if (this.heap.length === 0 || last === undefined) return;
        this.heap[0] = last;

        let i = 0;
        for (;;) {
            const left = 2 * i + 1;
            const right = left + 1;
            let smallest = i;
            if (left < this.heap.length && this.before(this.heap[left]!, this.heap[smallest]!)) smallest = left;
            if (right < this.heap.length && this.before(this.heap[right]!, this.heap[smallest]!)) smallest = right;
            if (smallest === i) break;
            [this.heap[i], this.heap[smallest]] = [this.heap[smallest]!, this.heap[i]!];
            i = smallest;
        }
    }
}
