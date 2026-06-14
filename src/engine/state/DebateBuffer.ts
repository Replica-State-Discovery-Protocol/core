import type { Address } from '../../domain/address.js';

/** Transient, one-shot store of neighbours' payloads during the DEBATE bootstrap. */
export class DebateBuffer<Payload> {
    private readonly items = new Map<Address, Payload>();

    set(addr: Address, payload: Payload): void {
        this.items.set(addr, payload);
    }

    snapshot(): Payload[] {
        return [...this.items.values()];
    }

    clear(): void {
        this.items.clear();
    }
}
