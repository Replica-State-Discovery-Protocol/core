import type { ReducerName } from '../../domain/address.js';
import type { Context } from '../../domain/context.js';
import type { ReducerPayload } from '../../domain/message.js';
import type { TranslatedState } from '../../domain/state.js';
import type { Reducer } from '../../reducer/Reducer.js';
import type { StateSnapshot } from '../Engine.js';
import { ReducerSlot } from './ReducerSlot.js';

/** The engine's set of reducer slots, plus wire composition and state access over them. */
export class SlotRegistry<Ctx extends Context> {
    private readonly slots = new Map<ReducerName, ReducerSlot<Ctx>>();

    add(slot: ReducerSlot<Ctx>): void {
        this.slots.set(slot.name, slot);
    }
    get(name: ReducerName): ReducerSlot<Ctx> | undefined {
        return this.slots.get(name);
    }
    values(): IterableIterator<ReducerSlot<Ctx>> {
        return this.slots.values();
    }

    /** Composite wire payload: one versioned entry per reducer. */
    composite(): Record<ReducerName, ReducerPayload> {
        const out: Record<ReducerName, ReducerPayload> = {};
        for (const slot of this.slots.values()) out[slot.name] = slot.toPayload();
        return out;
    }

    stateOf<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null {
        return (this.slots.get(reducer.name)?.state as TranslatedState<V> | undefined) ?? null;
    }
    stateByName(name: ReducerName): TranslatedState<unknown> | null {
        return this.slots.get(name)?.state ?? null;
    }
    /** Read-only view passed to convergence observers. */
    snapshotView(): StateSnapshot<Ctx> {
        const slots = this.slots;
        return {
            get<S, V>(reducer: Reducer<S, V, Ctx>): TranslatedState<V> | null {
                return (slots.get(reducer.name)?.state as TranslatedState<V> | undefined) ?? null;
            },
        };
    }
}
