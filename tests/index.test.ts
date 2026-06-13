import { describe, expect, it } from 'vitest';

import { rsdpCoreReady, version } from '../src/index.js';

describe('@rsdp/core scaffold', () => {
    it('exposes a version', () => {
        expect(version).toBe('0.0.0');
    });

    it('reports ready', () => {
        expect(rsdpCoreReady()).toBe(true);
    });
});
