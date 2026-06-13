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
