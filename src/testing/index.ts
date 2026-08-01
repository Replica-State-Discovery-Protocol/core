// Test doubles, published under the `@rsdp/core/testing` subpath so consumers can drive the
// engine deterministically without them leaking into the production barrel.
export { FakeClock } from '../clock/Clock.js';
export { InMemoryBus, InMemorySlan } from './InMemorySlan.js';
