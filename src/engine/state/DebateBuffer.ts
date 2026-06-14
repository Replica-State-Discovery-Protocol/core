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
