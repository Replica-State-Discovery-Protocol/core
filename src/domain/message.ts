import type { IncarnationValue } from '../incarnation/Incarnation.js';
import type { Address, ReducerName } from './address.js';

export enum MessageType {
    Hello = 'HELLO',
    Status = 'STATUS',
    Share = 'SHARE',
    Close = 'CLOSE',
}

export interface ReducerPayload {
    value: unknown;
    version: number;
}

export interface WireMessage {
    type: MessageType;
    from: Address;
    /**
     * The sender's restart epoch `ι` (eq. 15). Message-level, not per-reducer: `ι` is a
     * property of the sending node, so one value admits every reducer slot of that sender
     * at once — and costs one field per message rather than one per reducer entry.
     */
    inc?: IncarnationValue;
    payloads?: Record<ReducerName, ReducerPayload>; // STATUS / SHARE composite
    closed?: Address; // CLOSE
}
