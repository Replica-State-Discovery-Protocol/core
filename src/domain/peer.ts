import type { IncarnationValue } from '../incarnation/Incarnation.js';

export interface PeerRecord<Payload> {
    payload: Payload;
    version: number;
    /** The sender's restart epoch `ι`; absent for a peer that does not speak incarnations. */
    inc?: IncarnationValue | undefined;
    lastSeenAt: number;
}
