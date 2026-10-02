# Changelog

## Unreleased

### Features

- `fruitloops setup` and `fruitloops setup --hemibrain` now build the hemibrain
  ORN->PN connection and olfaction annotation tables from the pinned neuPrint
  hemibrain v1.2 neo4j import bundle, without credentials. `olf glomerulus
  --hemibrain` and `olf inputs --hemibrain` then count all 2,574 hemibrain ORNs
  with PN partners. Setup reads the two needed bundle members (about 750 MB) by
  HTTP range requests, or reads a downloaded bundle after its sha256 check. It
  skips this stage when it is current, and it keeps tables that a live
  `olf cache-annotations --hemibrain` wrote. The ORN->PN pairs and weights match
  live neuPrint hemibrain:v1.2.1; one PN keeps its v1.2 label, `DM4_adPN`
  instead of `DP1m_adPN`.
- Added `fruitloops olf cache-annotations --hemibrain --source neo4j-inputs` to
  refresh only these two tables from the bundle. Without `--source`, the
  command still fetches them live from neuPrint.
- Added hemibrain transmitter signs. `fruitloops setup --hemibrain` imports the
  FlyEM hemibrain v1.2 per-body transmitter predictions (Eckstein et al. 2024
  classifier, 46 MB) after a sha256 check, as its own setup stage. `neurons
  --hemibrain` reports `top_nt`, `transmitter`, `sign`, and
  `type_sign_conflict`, and accepts `--sign-conflicts`. `paths --hemibrain`
  reports `sign`, `signed_strength`, and `transmitters`, and accepts
  `--signed`; `paths --hemibrain --by-type` reports `signed_strength`.
  `reach --hemibrain --by-side` now works. Hemibrain Kenyon cells are set to
  acetylcholine, as in FlyWire. Most hemibrain neurons are on the right side,
  so `reach --hemibrain --by-side` often leaves `ai` empty.
- `neurons`, `paths`, and `reach` with `--hemibrain` now accept `--class` and
  `--super-class` (with the `--source-` and `--target-` prefixes) in the
  FlyWire vocabulary, for example `--source-class ALPN` and
  `--target-super-class descending`. Each hemibrain type gets the class of more
  than half of the FlyWire neurons that the pinned FlyWire annotation table
  matches to it; types without such a class stay empty. 72% of the traced
  hemibrain bodies get a super class. `neurons --hemibrain` reports
  `super_class` and `cell_class`.
- `paths --hemibrain` and `reach --hemibrain` now accept `--orn-family`,
  `--orn-glomerulus`, and `--orn-side`. A PN's seed is its input fraction from
  the chosen ORNs, with ORN synapses from the hemibrain ORN->PN table that
  setup builds and other inputs from the hemibrain graph; graph weights do not
  change. The antenna side comes from the ORN instance suffix (`_R` right,
  `_L` left), as verified against Schlegel et al. (2021). Hemibrain glomeruli
  use the names of Schlegel et al. (2021), so the v1.2 types `ORN_VC3l`,
  `ORN_VC3m`, and `ORN_VC5` count as VC3, VC5, and VM6. `--orn-family amt`
  includes the undivided hemibrain VM6.

### Fixes

- `fruitloops olf cache-annotations` no longer fails with "Cannot create index
  with outstanding updates" when the store has annotation tables but no `olf`
  tables.
- `olf inputs`, `olf outputs`, `olf pathway`, `olf classes`, `olf edges`, and
  `olf orn-inputs` now list rows with equal synapse counts in a fixed order,
  so repeated runs give the same output. With `--by-side`, the order includes
  the side columns.

### Changes

- `neurons`, `paths`, and `reach` with `--hemibrain` now need the transmitter
  table and the FlyWire annotation table. On a store from an earlier setup,
  they stop with a message to run `fruitloops setup --hemibrain`, which now
  also imports the FlyWire annotation table.
- `fruitloops admin bulk download --dataset hemibrain --kind neo4j-inputs` now
  verifies the bundle sha256, and `fruitloops admin bulk sources` lists it.
- The `olf inputs --hemibrain` hint for missing ORN rows now names the offline
  bundle command first.

## 0.2.0 - 2026-10-01

### Features

- Added `fruitloops neurons` to look up whole-brain FlyWire and hemibrain
  neurons by cell type (with wildcards), class, super class, or id, including
  descending, LAL, and PFL types.
- Added `fruitloops paths` to rank the strongest paths and report the fewest
  hops from source to target neurons, split by first-hop route (`AL`, `LH`,
  `MB`, `other`, `kc`), with optional ORN-weighted seeds and a signed search.
- Added `fruitloops paths --by-type` to rank cell-type routes per target type.
  A type route's strength sums all paths with that type sequence; rows also
  give its share of all paths, the ipsilateral share, the signed strength, and
  the synapses at each step.
- Added `fruitloops reach` to rank target types or neurons by hop-k reach, by
  route and by side, with a laterality index and signed ipsi-minus-contra net.
- Added ORN seed weighting by receptor family, glomerulus, and antenna side,
  using a packaged glomerulus receptor-family table that cites a primary
  source for each assignment.
- Added transmitter signs from FlyWire predictions, with a packaged override
  that sets Kenyon cells to acetylcholine.
- `fruitloops setup` now imports the FlyWire whole-brain neuron annotations
  (Schlegel et al. 2024) from a pinned commit with a sha256 check, and builds a
  cached sparse graph per dataset that `fruitloops status` reports. If the
  annotation download fails, setup reports an error row and still builds the
  other stages.

### Fixes

- `fruitloops olf` queries rebuild the derived olfaction tables only when a
  connection or annotation table changed after the last build. Before, a query
  after a single-dataset setup could rebuild on every run, or report a rebuild
  that did not occur.
- `fruitloops olf build` and `fruitloops olf cache-annotations` now record
  their build, so the next query does not rebuild the tables again. Re-fetched
  annotation labels mark the tables stale even when the row count is the same.
- `fruitloops admin bulk import` now records the imported file, so a manual
  re-import with the same row count and columns marks the `olf` tables stale.
- `fruitloops olf edges` now rebuilds stale olfaction tables like the other
  `olf` queries.
- A setup for one dataset no longer accepts olfaction tables that also hold
  another dataset, so stale rows are no longer kept after alternating `setup`
  and `setup --flywire`.
- Conflicting dataset flags, such as `--hemibrain --flywire`, are now an error
  instead of the last flag silently winning.
- Read-only queries now stop with a clear message, instead of a traceback, when
  another process holds the store's write lock.

### Changes

- Added `scipy` as a runtime dependency.
- Olfaction tables without a recorded build, for example tables from
  `fruitloops olf build` in an earlier version, rebuild once on the next `olf`
  query.
- A rebuild triggered by an `olf` query keeps the datasets of the last build;
  run `fruitloops setup` or `fruitloops olf build` to change them.
- Moved the DuckDB, setup-state, table-import, connection-table, and archive
  helpers out of `fruitloops.bulk` into `fruitloops.duckdb_store`,
  `fruitloops.setup_state`, `fruitloops.table_import`,
  `fruitloops.connection_tables`, and `fruitloops.archives`. Python code that
  imported them from `fruitloops.bulk` must import them from these modules.
- `fruitloops admin bulk sources` now lists the pinned sha256 of each source
  that has one, and downloads of those sources are verified.

## 0.1.11 - 2026-07-11

### Fixes

- Applied Fruitloops storage path overrides loaded through `--env-file` before
  resolving CLI defaults.

### Changes

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
