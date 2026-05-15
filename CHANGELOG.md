# Changelog

## Unreleased

## 0.1.10 - 2026-05-15

### Features

- Added hemibrain live ORN-to-PN annotation caching so broad hemibrain
  glomerulus input queries can use full neuPrint ORN coverage instead of only
  the compact traced-neuron adjacency cache.
- Added stale olfaction annotation freshness checks so `fruitloops olf`
  queries rebuild derived tables when newer annotation tables are available.

### Fixes

- Fixed FlyWire ORN/PN/glomerulus filters when annotation tables were imported
  after the derived olfaction cache had already been built.
- Fixed FlyWire and hemibrain glomerulus parsing for labels such as
  `DM1 / Or42b ORN`, `sensory,DA1,ORN`, and `DM3_adPN`.
- Fixed full olfaction builds after subset builds so both FlyWire and
  hemibrain data are rebuilt when requested.
- Added a clear hemibrain compact-cache warning when ORN queries need full
  live ORN-to-PN cache data.

### Changes

- Documented setup, `--cache-annotations`, required live credentials, olfaction
  command vocabulary, and broad glomerulus coverage in README and setup docs.
- Added runnable IDs to examples so documented commands can execute without
  placeholder replacement.
- Added a local release wrapper for version sync, package validation, tagging, and release workflow verification.
- Normalized release artifact actions and documented the local release wrapper.

## 0.1.9 - 2026-05-14

### Fixes

- Classify named lateral horn target types such as `LHAV`, `LHPD`, and
  `LHCENT` as LHN targets in `olf outputs`.
- Map hemibrain mushroom body ROIs such as `CA`, `PED`, and lobe names into
  the MB region for olfaction summaries.
- Continue `setup --cache-annotations` when a live annotation service fails
  and report annotation errors after the setup rebuild.

## 0.1.8 - 2026-05-14

### Fixes

- Rebuild derived olfaction tables when an existing setup cache is missing
  newer tables, so `olf inputs` and `olf outputs` do not use stale caches.

## 0.1.7 - 2026-05-13

### Fixes

- Fixed Homebrew installs on x86_64 Linux by using Linux wheels for binary
  runtime dependencies instead of macOS wheels.

## 0.1.6 - 2026-05-13

### Changes

- Changed default `fruitloops setup` output to a compact list; CSV and JSON
  still include full paths.
- Added setup freshness checks so repeated setup runs skip current imports,
  optimization, extraction, and derived olfaction table rebuilds.

## 0.1.5 - 2026-05-13

### Changes

- Added stderr progress updates to `fruitloops setup`, with `--no-progress`
  for quiet pipeline runs.

## 0.1.4 - 2026-05-13

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
- Added README and agent examples for DA2 PN direct targets in lateral horn
  and mushroom body.
- Updated the Homebrew tap to install bulk, live API, plotting, Arrow, and
  pandas runtime dependencies by default.

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
