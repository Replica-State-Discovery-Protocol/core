export interface PeerRecord<P> {
    payload: P;
    version: number;
    lastSeenAt: number;
}
