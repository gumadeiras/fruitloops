# Changelog

## Unreleased

### Features

- Added `fruitloops neurons` to look up whole-brain FlyWire and hemibrain
  neurons by cell type (with wildcards), class, super class, or id, including
  descending, LAL, and PFL types.
- Added `fruitloops paths` to rank the strongest paths and report the fewest
  hops from source to target neurons, split by first-hop route (`AL`, `LH`,
  `MB`, `other`, `kc`), with optional ORN-weighted seeds and a signed search.
- Added `fruitloops reach` to rank target types or neurons by hop-k reach, by
  route and by side, with a laterality index and signed ipsi-minus-contra net.
- Added ORN seed weighting by receptor family, glomerulus, and antenna side,
  using a packaged glomerulus receptor-family table that cites a primary
  source for each assignment.
- Added transmitter signs from FlyWire predictions, with a packaged override
  that sets Kenyon cells to acetylcholine.
- `fruitloops setup` now imports the FlyWire whole-brain neuron annotations
  (Schlegel et al. 2024) from a pinned commit with a sha256 check, and builds a
  cached sparse graph per dataset that `fruitloops status` reports.

### Fixes

- Applied Fruitloops storage path overrides loaded through `--env-file` before
  resolving CLI defaults.
- `fruitloops olf` queries rebuild the derived olfaction tables only when a
  connection or annotation table changed after the last build. Before, a query
  after a single-dataset setup could rebuild on every run, or report a rebuild
  that did not occur.
- `fruitloops olf build` and `fruitloops olf cache-annotations` now record
  their build, so the next query does not rebuild the tables again. Re-fetched
  annotation labels mark the tables stale even when the row count is the same.

### Changes

- Added `scipy` as a runtime dependency.
- Olfaction tables without a recorded build, for example tables from
  `fruitloops olf build` in an earlier version, rebuild once on the next `olf`
  query.
- `fruitloops admin bulk sources` now lists the pinned sha256 of each source
  that has one, and downloads of those sources are verified.
- Moved downloaded bulk data and DuckDB state to the operating system's
  application-data directory, and live-query responses to its cache directory,
  instead of using checkout-local or Unix-only defaults.

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
