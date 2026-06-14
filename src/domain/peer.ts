export interface PeerRecord<Payload> {
    payload: Payload;
    version: number;
    lastSeenAt: number;
}
