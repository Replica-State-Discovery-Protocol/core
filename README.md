# @rsdp/core

The leaderless, modular distributed-coordination engine of the **Replica State Discovery Protocol (RSDP)**.

> Scaffold only. The engine implementation will be migrated from the RSDP workbench prototype.

## Requirements

- Node.js `>= 22`

## Scripts

| Script                            | Purpose                                             |
| --------------------------------- | --------------------------------------------------- |
| `npm run build`                   | Compile `src/` to `dist/` (ESM + type declarations) |
| `npm run dev`                     | Incremental compile in watch mode                   |
| `npm run typecheck`               | Type-check without emitting                         |
| `npm run lint` / `lint:fix`       | ESLint (flat config, type-checked)                  |
| `npm run format` / `format:check` | Prettier                                            |
| `npm test`                        | Run the test suite (Vitest)                         |
| `npm run test:watch`              | Vitest in watch mode                                |
| `npm run test:coverage`           | Vitest with V8 coverage                             |

## Toolchain

TypeScript (ESM, `NodeNext`, strict) · ESLint 9 flat config with type-checked `typescript-eslint` · Prettier 3 · Vitest · Husky + lint-staged + commitlint (Conventional Commits).
