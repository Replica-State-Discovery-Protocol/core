import type { Address } from './address.js';

/** Threaded through every pipeline run. Reducers may use a Ctx that extends this. */
export interface Context {
    self: Address;
}
