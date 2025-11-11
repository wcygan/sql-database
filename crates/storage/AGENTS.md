# Storage Crate - agents.md

> This doc captures implementation guardrails for the `storage` crate so future agents can extend it without breaking workspace conventions.

## Purpose

- Provide physical persistence for heap tables: fixed-size slotted pages, tuple encoding, and `HeapFile` (append-only heap file that will later sit behind the buffer pool).
- Translate logical `Row` values to/from bytes while keeping the layout compatible with upcoming WAL and pager layers.
- Serve as the only place that talks to the filesystem directly for table data until the buffer pool crate takes over.

## Architecture checkpoints

1. **Page layout** - 4 KB pages (`PAGE_SIZE`) with a `PageHeader`, slot array, and tuple area that grows downward.
2. **Record identity** - every tuple is addressed by `RecordId { page_id: PageId, slot }` from `common`.
3. **Heap interface** - expose the `HeapTable` trait; `HeapFile` is the on-disk implementation and will later be wrapped by the pager/buffer pool.
4. **Append semantics** - inserts currently target the last page and allocate a fresh page if there is not enough space; deletes mark slots empty but do not reclaim space yet.

## Serialization rules (bincode 2)

- The workspace pins **bincode `2.0.1` with the `serde` feature enabled** in `Cargo.toml:[workspace.dependencies]`. Do not override this per crate.
- Always use the `bincode::serde` helpers: `encode_to_vec`, `encode_into_slice`, `decode_from_slice`.
- The helper `bincode_config()` returns `config::legacy()` so storage matches the rest of the database stack (little endian, fixed-width ints). Reuse it everywhere you encode/decode to avoid format drift.
- Never call deprecated v1 functions (`serialize`, `deserialize`) or mix configs per call.

## Workspace coordination

- Dependencies *must* be declared via `{ workspace = true }` (see `crates/storage/Cargo.toml`). If you need new crates, pin them in the workspace table first.
- Shared domain types (`Row`, `RecordId`, `DbResult`, etc.) come from `common`/`types`; do not duplicate them here.
- Tests should rely on `tempfile` (also workspace-managed) to avoid polluting real data directories.

## Extending the crate

- When adding page operations (compaction, free space tracking, etc.), keep slot math centralized in `Page` to prevent duplication across callers.
- Before introducing WAL/buffer-pool hooks, prefer traits or lightweight adapters so `HeapFile` can be swapped out easily.
- Whenever you change on-disk layout, update this doc and add migration notes plus tests that prove backward compatibility (or explicitly gate incompatible changes behind a new feature flag).

By following these constraints we keep the storage layer aligned with the rest of the SQL database workspace and ready for the upcoming buffer + WAL work.
