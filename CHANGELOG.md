# Changelog

## Unreleased

### Features

- Added top-level `fruitloops olf` access to offline olfaction queries.
- Added canonical olfaction pathway, neuron-region, and cell-type summary
  tables with pre/post/neuropil side relation fields.
- Added `fruitloops olf classes`, `glomerulus`, `pathway`, and `inputs`
  commands for broad olfactory circuit queries beyond LNs.
- Added broader olfactory class detection for sensory, lateral horn, and
  mushroom body target annotations plus `fruitloops olf outputs`.

### Changes

- Updated README, agent guidance, and CLI examples to lead with broad
  olfaction workflows before LN-specific table queries.

## 0.1.3 - 2026-05-13

### Features

- Added a simplified top-level CLI around `status`, `setup`, `table`, `find`,
  `partners`, `examples`, and `admin`.
- Added `fruitloops setup` as the single explicit setup command for live cache
  directories, practical bulk imports, and derived olfaction tables.
- Added table, setup, status, and shared CLI helper modules to keep command
  behavior scoped and reusable.

### Changes

- Kept the offline/live command split, but moved advanced commands behind
  `fruitloops admin`.
- Replaced optional dependency extras with one installable runtime dependency
  set for bulk, live, plotting, pandas, and Arrow support.
- Packaged the generated CSV snapshot into wheels under `fruitloops/data` and
  made data-dir discovery prefer bundled package data.
- Updated README and release notes for the one-installable workflow and the
  `fruitloops setup` path.
- Centralized format, dataset, partner-kind, and repeated-dataset argument
  handling across CLI modules.

### Fixes

- Removed references to `fruitloops-install-extras` and extra-based install
  messages from current docs and runtime dependency errors.
- Avoided duplicate setup/build work when repeated dataset flags are provided.
- Added regression coverage for the setup wrapper, annotation caching rebuilds,
  admin passthrough, and unified table command workflows.

## 0.1.2 - 2026-05-13

### Changes

- Documented PyPI installation.
- Showed help when `fruitloops` is run without a command or with incomplete commands.
- Switched fruitloops data to stable storage paths.
- Tightened path handling and bulk setup helpers.

## 0.1.1 - 2026-05-05

### Changes

- Documented the Homebrew extras installer.
- Prepared the PyPI release flow.

## 0.1.0 - 2026-04-25

Initial release.

### Features

- Added an agent-friendly CLI for querying connectome analysis tables from hemibrain and FlyWire.
- Added generated CSV snapshot layout and snapshot workflow documentation.
- Added reusable CSV plotting CLI.
- Added live connectome access adapters and an offline-first live query cache.
- Added bulk offline data, query, partner, and optimization commands.
- Added olfaction offline cache.

### Fixes

- Fixed the hemibrain compact table hint.

### Changes

- Simplified bulk DuckDB and partner CLI helpers.
- Documented bulk offline and production setup workflows.
