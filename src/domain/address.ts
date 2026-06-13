export type Address = string;
export type ReducerName = string;
export interface NodeIdentity {
    address: Address;
    metadata?: Record<string, unknown>;
}
