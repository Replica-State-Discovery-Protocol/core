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
