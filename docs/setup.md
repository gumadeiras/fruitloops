# Setup and Data Sources

This page covers installation, setup flags, live annotation credentials, bulk
imports, and cache behavior. The README keeps the short path.

## Install

Choose one install method.

Homebrew:

```bash
brew tap gumadeiras/tap
brew install fruitloops
```

PyPI:

```bash
python -m pip install fruitloops
```

Editable checkout:

```bash
python -m pip install -e .
```

Both packaged installs include the generated CSV snapshot used by `status`,
`table`, `find`, `partners`, and comparison commands. No setup is required for
those snapshot queries.

## Basic Setup

Run setup when you want larger local offline stores and derived olfaction
tables:

```bash
fruitloops setup
```

Setup downloads/imports practical bulk connection tables, creates the DuckDB
store, creates the live cache directory, optimizes imported tables, and builds
derived `olf_*` tables.

Common variants:

```bash
# Build both practical offline datasets.
fruitloops setup

# Build only one dataset.
fruitloops setup --flywire
fruitloops setup --hemibrain
fruitloops setup --dataset flywire
fruitloops setup --dataset hemibrain

# Fetch live annotation tables before rebuilding olfaction tables.
fruitloops setup --cache-annotations

# Machine-readable setup report.
fruitloops setup --csv --no-progress
```

## Setup Flags

- `--dataset flywire|hemibrain`, `--flywire`, `--hemibrain`: limit setup to
  one dataset. Without a dataset flag, setup prepares both datasets.
- `--replace` / `--no-replace`: replace existing imported tables or keep them.
  Current stages are skipped by fingerprint.
- `--cache-annotations`: fetch live labels into DuckDB and rebuild olfaction
  tables. This requires live API credentials.
- `--chunk-size N`: live annotation fetch batch size.
- `--bulk-dir PATH`: downloaded bulk files and extracted archives.
- `--store PATH`: DuckDB file to import/build.
- `--cache-dir PATH`: live-query/offline cache directory.
- `--csv`, `--json`, `--jsonl`, `--format table|csv|json|jsonl`: setup report
  output format.
- `--progress` / `--no-progress`: progress updates on stderr.

Setup prints progress to stderr while keeping the final summary on stdout.
Re-running setup skips downloads, imports, optimization, and derived olfaction
tables when local source fingerprints still match the stored setup state.

## Annotation Caching

`fruitloops setup --cache-annotations` is the full setup pipeline plus live
annotation caching. Use it for first install or when you want setup and labels
refreshed together.

`fruitloops olf cache-annotations` only refreshes live labels/cache tables and
then rebuilds `olf_*` tables unless `--no-rebuild` is passed. Use it after setup
already exists.

Practical rule:

```bash
# First time, reset, or not sure.
fruitloops setup --cache-annotations

# Later, refresh labels only.
fruitloops olf cache-annotations
```

Default setup is offline-first. FlyWire ORN/PN/glomerulus labels are usually
usable after practical setup because public annotation tables are imported.
Hemibrain compact adjacencies only include traced neurons, so broad hemibrain
ORN glomerulus queries need live ORN->PN caching:

```bash
fruitloops olf cache-annotations --hemibrain --csv
```

## Required Environment Variables

`--cache-annotations` requires credentials for whichever dataset is being
cached.

Hemibrain accepts one of:

- `NEUPRINT_APPLICATION_CREDENTIALS`
- `NEUPRINT_AUTH_TOKEN`
- `NEUPRINT_TOKEN`

FlyWire accepts one of:

- `CAVE_AUTH_TOKEN`
- `CAVE_TOKEN`

Scope matters:

- `fruitloops setup --cache-annotations` tries both datasets, so it needs one
  hemibrain token and one FlyWire token.
- `fruitloops setup --flywire --cache-annotations` only needs a FlyWire token.
- `fruitloops setup --hemibrain --cache-annotations` only needs a hemibrain
  token.

Examples:

```bash
export NEUPRINT_AUTH_TOKEN=...
export CAVE_AUTH_TOKEN=...
fruitloops setup --cache-annotations --csv

export CAVE_AUTH_TOKEN=...
fruitloops setup --flywire --cache-annotations --csv

export NEUPRINT_AUTH_TOKEN=...
fruitloops setup --hemibrain --cache-annotations --csv
```

Optional live API defaults:

```bash
export NEUPRINT_SERVER=neuprint.janelia.org
export NEUPRINT_DATASET=hemibrain:v1.2.1
export FLYWIRE_DATASTACK=flywire_fafb_public
```

You can also put credentials in a local `.env` file and pass
`--env-file path/to/file.env`.

## Paths

Fruitloops does not depend on the current working directory. Inspect active
paths with:

```bash
fruitloops status
```

Bulk downloads and DuckDB state use the OS application-data directory. Live
query responses use the OS cache directory. On macOS these are
`~/Library/Application Support/fruitloops` and `~/Library/Caches/fruitloops`;
Linux uses XDG data/cache roots; Windows uses `%LOCALAPPDATA%\fruitloops`.

Path overrides:

- `FRUITLOOPS_DATA_DIR`: generated CSV snapshot directory with `manifest.csv`
- `FRUITLOOPS_BULK_DIR`: downloaded bulk files and extracted archives
- `FRUITLOOPS_DUCKDB_PATH`: imported DuckDB database path
- `FRUITLOOPS_CACHE_DIR`: live-query cache root

## Olfaction Tables

Setup builds derived AL/LH/MB tables:

```bash
fruitloops setup
fruitloops olf tables --csv
```

The builder creates `olf_edges_by_neuropil`, `olf_edges_total`,
`olf_neuropil_membership`, `olf_neurons`, `olf_annotations`,
`olf_neuron_regions`, `olf_pathway_edges`, `olf_pathway_summary`,
`olf_cell_type_summary`, and `olf_provenance` in the DuckDB store.

It uses imported annotation/cache tables when available:

- `hemibrain_olfaction_neuron_annotations` or `hemibrain_traced_neurons`
- `hemibrain_olfaction_orn_pn_connections`
- `flywire_hierarchical_neuron_annotations`
- `flywire_neuron_information_v2`

## Bulk Offline Releases

Bulk releases should be the primary offline source when you need broad
connectivity.

List known public release files:

```bash
fruitloops admin bulk sources
```

Download practical FlyWire connectivity:

```bash
fruitloops admin bulk download --dataset flywire --kind proofread-connections
```

Optional larger downloads:

```bash
fruitloops admin bulk download --dataset hemibrain --kind compact-adjacencies
fruitloops admin bulk download --dataset flywire --kind synapses
fruitloops admin bulk download --dataset hemibrain --kind neo4j-inputs
```

Import CSV/Parquet/Feather into local DuckDB:

```bash
flywire_path=$(fruitloops admin bulk download --dataset flywire --kind proofread-connections)
fruitloops admin bulk import --path "$flywire_path" --table flywire_proofread_connections --replace
fruitloops admin bulk tables
fruitloops admin bulk query --table flywire_proofread_connections --limit 10 --format csv
```

Optimize imported connection tables before repeated partner queries:

```bash
fruitloops admin bulk optimize --table flywire_proofread_connections --prefix flywire
fruitloops admin bulk optimize --table hemibrain_traced_roi_connections --prefix hemibrain
```

Agent-facing wrappers infer common pre/post/weight/ROI column names:

```bash
fruitloops admin bulk schema --table flywire_proofread_connections
fruitloops admin bulk connections --table flywire_proofread_connections --pre-id 720575940623636701 --limit 20 --format csv
fruitloops admin bulk inputs --table flywire_proofread_connections --body-id 720575940623636701 --format csv
fruitloops admin bulk outputs --table flywire_proofread_connections --body-id 720575940623636701 --format csv
fruitloops admin bulk partners --table flywire_proofread_connections --body-id 720575940623636701 --format json
fruitloops admin bulk views --table flywire_proofread_connections --prefix flywire
```

Hemibrain compact adjacency and Neo4j bundles are CSV archives. Extract first,
then import the CSVs you need:

```bash
hemibrain_path=$(fruitloops admin bulk download --dataset hemibrain --kind compact-adjacencies)
fruitloops admin bulk extract --path "$hemibrain_path"
fruitloops admin bulk import \
  --path "$(fruitloops status --csv | awk -F, '$2=="bulk_dir"{print $4}')/extracted/exported-traced-adjacencies-v1.2/traced-roi-connections.csv" \
  --table hemibrain_traced_roi_connections \
  --replace
fruitloops admin bulk import \
  --path "$(fruitloops status --csv | awk -F, '$2=="bulk_dir"{print $4}')/extracted/exported-traced-adjacencies-v1.2/traced-neurons.csv" \
  --table hemibrain_traced_neurons \
  --replace
```

## Live and Offline-First Cache

Live database access is optional. Use direct live commands for one-off queries:

```bash
fruitloops admin live hemibrain neurons --type-contains il3LN6 --limit 5 --format csv
fruitloops admin live hemibrain connections --upstream-body-id 5813018460 --limit 20 --format json
fruitloops admin live flywire tables --format csv
fruitloops admin live flywire synapses --pre-root-id 720575940623636701 --limit 10 --format json
```

Use `offline fetch` when you want local data first and live APIs only on cache
miss. Results are saved under the live cache directory.

```bash
fruitloops admin offline fetch \
  --dataset flywire \
  --action synapses \
  --pre-root-id 720575940623636701 \
  --limit 10 \
  --format csv
```

Repeat the same command to read the cached CSV. Use `--offline-only` to fail
instead of hitting the network, or `--refresh` to force a live re-fetch.

```bash
fruitloops admin offline list
fruitloops admin offline fetch --dataset flywire --action tables
fruitloops admin offline fetch --dataset flywire --action tables --offline-only
fruitloops admin offline fetch --dataset hemibrain --action neurons --type-contains il3LN6 --limit 5
```

## Plotting

Render from a `fruitloops` table reference:

```bash
fruitloops admin plot \
  --table comparison:matched_ln_class_similarity \
  --kind scatter \
  --x hemibrain_mean_contra_preference \
  --y flywire_mean_contra_preference \
  --label LN_class \
  --top-labels 8 \
  --output outputs/contra_preference_scatter \
  --formats png,svg
```

Render from any CSV path:

```bash
fruitloops admin plot \
  --csv path/to/table.csv \
  --kind scatter \
  --x x_column \
  --y y_column \
  --output outputs/my_scatter
```

## Rebuilding the CSV Snapshot

Fruitloops needs a generated CSV snapshot for `status`, `table`, `find`,
`partners`, and comparison commands. Release builds ship that snapshot. For a
source checkout or custom package without `data/`, point `FRUITLOOPS_DATA_DIR`
at a snapshot or rebuild it from the paper repository.

From the fruitloops repository root:

```bash
python scripts/build_data_snapshot.py \
  --source "/path/to/widespread-direction-selectivity" \
  --dest "$(fruitloops status --csv | awk -F, '$2=="data_dir"{print $4}')"
```
