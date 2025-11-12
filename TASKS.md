# Tasks

- [x] Fix UPDATE and DELETE paths so they actually mutate storage, emit WAL records, and add regression tests proving rows change.
- [ ] Enforce primary-key uniqueness (schema metadata, insert-time checks, and tests that reject duplicate keys).
