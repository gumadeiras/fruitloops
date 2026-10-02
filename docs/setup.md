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
derived `olf_*` tables. FlyWire setup also imports the pinned whole-brain
neuron annotation table (`flywire_neuron_annotations`). Hemibrain setup also
imports the pinned per-body transmitter predictions
(`hemibrain_body_neurotransmitters`, 46 MB) and the same FlyWire annotation
table, which gives the hemibrain classes
([docs/paths.md](paths.md#hemibrain-classes)). It builds the ORN->PN and
olfaction annotation tables from the pinned neuPrint neo4j bundle, without
credentials; see
[Hemibrain tables from the neo4j bundle](#hemibrain-tables-from-the-neo4j-bundle).
Each dataset also gets a sparse graph cache for `paths` and `reach`; see
[docs/paths.md](paths.md).

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

- `--dataset flywire|hemibrain`, `--flywire`, `--hemibrain`: limit the
  downloads and imports of setup to one dataset. Without a dataset flag, setup
  prepares both datasets. The `olf_*` tables are one set for both datasets, so
  setup always builds them for each dataset whose tables are in the store. A
  setup for one dataset does not remove the other dataset from these tables. To
  build them for one dataset only, use `olf build --dataset flywire|hemibrain`.
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
Re-running setup skips downloads, imports, optimization, derived olfaction
tables, and graph caches when local source fingerprints still match the stored
setup state. A table counts as changed when its row count, its columns, or the
file of its last import through fruitloops (`setup` or `admin bulk import`)
changes. Edits made directly in DuckDB, for example an SQL `UPDATE`, are not
detected; re-import the table instead.

A graph cache rebuilds when its connection table changes. `olf` queries,
including `olf edges`, use the same state: when a connection or annotation
table changed after the last olfaction build, the next query rebuilds the
derived tables once and prints a note on stderr. That rebuild keeps the
datasets of the last build; run `setup` or `olf build` to change them.

If the download or the sha256 check of a pinned label source (FlyWire
annotations or hemibrain transmitter predictions) fails, setup reports an
`error` row and continues with the graph and olfaction stages. `neurons`,
`paths`, and `reach` for each dataset that needs the table stop until a later
setup imports it. Both datasets need the FlyWire annotations.

If the hemibrain neo4j bundle cannot be read or verified, setup reports an
`error` row for `neo4j-inputs` and keeps the other hemibrain tables.

## Annotation Caching

`fruitloops setup --cache-annotations` is the full setup pipeline plus live
annotation caching. Use it for first install or when you want setup and labels
refreshed together.

`fruitloops olf cache-annotations` only refreshes live labels/cache tables and
then rebuilds `olf_*` tables unless `--no-rebuild` is passed. With
`--no-rebuild`, the next `olf` query rebuilds the tables if the cached labels
changed. Use it after setup already exists.

Practical rule:

```bash
# First time, reset, or not sure.
fruitloops setup --cache-annotations

# Later, refresh labels only.
fruitloops olf cache-annotations
```

Default setup is offline-first. FlyWire ORN/PN/glomerulus labels are usually
usable after practical setup because public annotation tables are imported.
Hemibrain compact adjacencies only include traced neurons, and only 2 of them
are ORNs. Hemibrain setup therefore builds the ORN->PN and annotation tables
from the neo4j bundle, as described in the next section.

### Hemibrain tables from the neo4j bundle

`fruitloops setup` and `fruitloops setup --hemibrain` write
`hemibrain_olfaction_orn_pn_connections` and
`hemibrain_olfaction_neuron_annotations` from the bulk source
`hemibrain:neo4j-inputs`, then rebuild the `olf_*` tables. No credentials are
needed. After setup, `olf glomerulus DM1 --hemibrain` and
`olf inputs --source-class ORN --hemibrain` count all hemibrain ORNs, and
`paths --hemibrain` and `reach --hemibrain` use the ORN->PN table for ORN-weighted
seeds ([docs/paths.md](paths.md#hemibrain-orn-seeds)).

To refresh only these two tables from the bundle:

```bash
fruitloops olf cache-annotations --hemibrain --source neo4j-inputs --csv
```

`--source neo4j-inputs` requires `--hemibrain`. `--no-rebuild` works as it
does for the live fetch. Without `--source`, the command fetches the same
tables live from neuPrint, which needs a hemibrain token.

How the bundle is read:

- If `<bulk-dir>/raw/hemibrain/hemibrain_v1.2_neo4j_inputs.zip` exists, for
  example from `admin bulk download --dataset hemibrain --kind neo4j-inputs`,
  fruitloops checks its sha256 and reads it.
- Otherwise fruitloops reads only the two needed zip members, about 750 MB of
  the 6.2 GB zip, from the pinned URL with HTTP range requests. Nothing is
  written to disk.
- The members are streamed. Only the needed columns and rows are kept, so a
  read takes about one minute and 0.6 to 1.5 GB of memory.

Integrity pins:

- Whole file: 6,175,266,195 bytes, sha256
  `d6bcdba98d7fd1a41be08aff79e5725c22cc24cec4a830e6d422ddfecbb98b6e`.
- Range reads: every response must carry ETag
  `"e8443e69d1e238f8cb32168b3f6f6bab"` (the object MD5) and the pinned size.
- Members, from the zip central directory, checked again by the zip CRC-32
  while streaming: `Neuprint_Neurons_31597.csv` 10,625,728,664 bytes, CRC-32
  `3ca765b5`; `Neuprint_Neuron_Connections_31597.csv` 4,876,659,508 bytes,
  CRC-32 `70137ee6`.

The setup stage is current while the pinned bundle, the compact hemibrain
connection table, and the olfaction schema are unchanged. Setup keeps the two
tables when another writer, such as a live
`olf cache-annotations --hemibrain`, wrote them; the status is then
`existing`. Run the refresh command above to replace them from the bundle.

The tables have the same columns and meaning as the live neuPrint query.
neuPrint loads `Neuprint_Neurons_31597.csv` as nodes and
`Neuprint_Neuron_Connections_31597.csv` with
`--relationships=ConnectsTo=...`
([neuPrint load guide](https://github.com/connectome-neuprint/neuPrint/blob/master/neo4j_desktop_load.md)).

| Live query | Bundle column |
| --- | --- |
| `(n:Neuron)` | `:LABEL` of the neurons file contains `Neuron`. Labels are `;`-separated; Neuron rows have `Segment;hemibrain_Segment;Neuron;hemibrain_Neuron;Cell;hemibrain_Cell`, other segments `Segment;hemibrain_Segment`. |
| `n.bodyId` | `bodyId:long` |
| `n.type`, `n.instance`, `n.status` | `type:string`, `instance:string`, `status:string` |
| `n.cropped` | `cropped:boolean`; only the text `true` is true |
| `n.size` | `size:long` |
| `(orn)-[w:ConnectsTo]->(pn)` | one connections row, from `:START_ID(Body-ID)` to `:END_ID(Body-ID)`, matched to the node `:ID(Body-ID)` |
| `w.weight` | `weight:int` (not `weightHP:int`) |
| `roi` | the same rule as the live fetch, from the PN instance side suffix |

An empty field is null, because neo4j import sets no property for it. The
source rule is `type STARTS WITH 'ORN_'`, the target rule is
`type CONTAINS 'PN'`, and only `weight > 0` rows are kept.

Version: the bundle is hemibrain v1.2. Live neuPrint serves hemibrain:v1.2.1,
which changed no connectivity. All 17,502 ORN->PN pairs and weights match a
live fetch, and 6,574 of 6,575 annotation rows match. Two labels differ. PN
body 1006068474 is `DM4_adPN` in v1.2 and `DP1m_adPN` in v1.2.1, so its 3,110
ORN synapses count for DM4 instead of DP1m. Body 1734372986 has status
`Traced` in v1.2 and `Assign` in v1.2.1.

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

It uses imported annotation/cache tables when available. Setup writes the
two hemibrain cache tables from the neo4j bundle; `olf cache-annotations`
can refresh them from the bundle or from live neuPrint:

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

FlyWire whole-brain annotations, pinned to a commit and checked by sha256:

```bash
fruitloops admin bulk download --dataset flywire --kind neuron-annotations
```

Hemibrain v1.2 per-body transmitter predictions, checked by sha256 (source,
columns, and rule in [docs/paths.md](paths.md#hemibrain-predictions)):

```bash
fruitloops admin bulk download --dataset hemibrain --kind body-neurotransmitters
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
then import the CSVs you need. The Neo4j bundle expands to about 70 GB; setup
does not extract it, because it streams only the two members it needs. For the
compact bundle:

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
