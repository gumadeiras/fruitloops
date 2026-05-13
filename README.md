# fruitloops

Agent-friendly CLI for offline-first olfactory connectome queries across
hemibrain and FlyWire.

The repository keeps generated CSV products in a predictable layout:

```text
data/
  manifest.csv
  hemibrain/
  flywire/
  comparison/
```

## Quick Use

Choose one install method.

Install with Homebrew:

```bash
brew tap gumadeiras/tap
brew install fruitloops
```

Install from PyPI:

```bash
python -m pip install fruitloops
```

Both installs include:

- the generated CSV snapshot for `status`, `table`, `find`, and `partners`
- Python dependencies for local CSV queries, bulk imports, live access, and
  plotting

Nothing else is required for local snapshot queries:

```bash
fruitloops status
fruitloops table --flywire --contains summary --csv
```

Run setup when you want the larger local offline stores. It downloads/imports
the practical bulk connection tables, creates the DuckDB store, creates the
live cache directory, and builds derived olfaction tables:

```bash
fruitloops setup
```

Run directly from the repository:

```bash
python -m fruitloops status
python -m fruitloops olf glomerulus DM1 --flywire --csv
python -m fruitloops olf inputs --target-class PN --source-class ORN --glomerulus DM1 --by-side --csv
python -m fruitloops table --flywire --contains summary --csv
python -m fruitloops table flywire:analysis_outputs/full_summary --head
python -m fruitloops table comparison:matched_ln_class_similarity --contains LN_class=il3LN6 --json
python -m fruitloops find il3LN6 --flywire --json
python -m fruitloops partners il3LN6 --flywire --orn --csv
python -m fruitloops examples
```

For editable installation:

```bash
python -m pip install -e .
fruitloops status
fruitloops setup
```

Fruitloops does not depend on the current working directory. Inspect the active
paths with:

```bash
fruitloops status
```

Path overrides:

- `FRUITLOOPS_DATA_DIR`: generated CSV snapshot directory with `manifest.csv`
- `FRUITLOOPS_BULK_DIR`: downloaded bulk files and extracted archives
- `FRUITLOOPS_DUCKDB_PATH`: imported DuckDB database path
- `FRUITLOOPS_CACHE_DIR`: live-query cache root

## Table References

Tables can be referenced as:

- `dataset:relative/path/without_csv`
- `dataset:collection/file_stem`
- `file_id` from `data/manifest.csv`

Examples:

```bash
fruitloops table flywire:analysis_outputs/full_summary --schema --csv
fruitloops table hemibrain:analysis_outputs/full_summary --select bodyId,LN_type,input_preference
fruitloops table flywire:source_audit/ln_observations_by_hemisphere --where LN_type=il3LN6
fruitloops table flywire:source_audit/orn_partner_counts_by_hemisphere --where LN_type=il3LN6 --csv
fruitloops table comparison:matched_ln_class_similarity --path
```

## Common Agent Queries

After `fruitloops setup`, use `fruitloops olf` for broad olfaction questions:

```bash
fruitloops olf classes --flywire --region AL --csv
fruitloops olf glomerulus DM1 --flywire --csv
fruitloops olf pns --glomerulus DM1 --hemibrain --csv
fruitloops olf inputs --target-class PN --source-class ORN --glomerulus DM1 --by-side --flywire --csv
fruitloops olf outputs --source-class PN --target-class KC --region MB --flywire --csv
fruitloops olf pathway ORN LN --region AL --flywire --csv
fruitloops olf pathway PN KC --region MB --flywire --csv
```

Question recipes:

```bash
# Which PNs exist for one glomerulus?
fruitloops olf pns --flywire --glomerulus DA2 --csv
fruitloops olf pns --hemibrain --glomerulus DA2 --csv

# Which lateral horn neurons receive direct DA2 PN input?
fruitloops olf outputs --flywire --source-class PN --target-class LHN --glomerulus DA2 --region LH --by-side --csv
fruitloops olf outputs --hemibrain --source-class PN --target-class LHN --glomerulus DA2 --region LH --by-side --csv

# Which mushroom body neurons receive direct DA2 PN input?
fruitloops olf outputs --flywire --source-class PN --target-class KC --glomerulus DA2 --region MB --by-side --csv
fruitloops olf outputs --hemibrain --source-class PN --target-class KC --glomerulus DA2 --region MB --by-side --csv

# Same DA2 PN target search, without requiring target class annotations.
fruitloops olf outputs --flywire --source-class PN --glomerulus DA2 --region LH --by-side --csv
fruitloops olf outputs --flywire --source-class PN --glomerulus DA2 --region MB --by-side --csv
```

Use table aggregation when you need legacy generated CSV products:

```bash
fruitloops table flywire:source_audit/orn_partner_counts_by_hemisphere \
  --where LN_type=il3LN6 \
  --by LN_type,analysis_hemisphere,input_relation \
  --sum n_synapses \
  --csv
```

Specialized LN partner summaries are still available:

```bash
fruitloops partners il3LN6 --flywire --orn --csv
fruitloops partners il3LN6 --flywire --pn --csv
fruitloops partners il3LN6 --hemibrain --orn --csv
fruitloops partners il3LN6 --hemibrain --pn --csv
```

Pull the reconciled hemibrain/FlyWire comparison:

```bash
fruitloops table comparison:matched_ln_class_similarity --contains LN_class=il3LN6 --json
```

Useful LN workflow:

```bash
fruitloops find il3LN6 --csv
fruitloops table flywire:source_audit/ln_observations_by_hemisphere --where LN_type=il3LN6 --csv
fruitloops table flywire:source_audit/orn_partner_counts_by_hemisphere \
  --where LN_type=il3LN6 \
  --by analysis_hemisphere,input_relation \
  --sum n_synapses \
  --csv
fruitloops table comparison:matched_ln_class_similarity --contains LN_class=il3LN6 --jsonl
```

Legacy commands such as `datasets`, `files`, `schema`, `head`, `query`,
`aggregate`, and `ln` remain available for scripts. New interactive use should
prefer `status`, `setup`, `olf`, `table`, `find`, `partners`, and `examples`.

## Olfaction Offline Cache

Build derived AL/LH/MB tables after importing bulk connectivity:

```bash
fruitloops setup
fruitloops olf tables
```

For complete names/classes/glomeruli, cache annotations once from live APIs and
rebuild:

```bash
fruitloops olf cache-annotations --dataset hemibrain
fruitloops olf cache-annotations --dataset flywire
```

The builder creates `olf_edges_by_neuropil`, `olf_edges_total` aggregated over
AL/LH/MB, `olf_neuropil_membership`, `olf_neurons`, `olf_annotations`,
`olf_neuron_regions`, `olf_pathway_edges`, `olf_pathway_summary`,
`olf_cell_type_summary`, and `olf_provenance` in the DuckDB store. It uses
imported annotation tables when available:

- `hemibrain_olfaction_neuron_annotations` or `hemibrain_traced_neurons`
- `flywire_hierarchical_neuron_annotations`
- `flywire_neuron_information_v2`

Example olfaction queries:

```bash
fruitloops olf neurons --dataset flywire --region AL --class ORN --format csv
fruitloops olf classes --dataset flywire --region AL --format csv
fruitloops olf glomerulus DM1 --dataset flywire --format csv
fruitloops olf inputs --dataset hemibrain --target-class PN --source-class ORN --glomerulus DM1 --by-side --format csv
fruitloops olf outputs --dataset flywire --source-class PN --target-class KC --region MB --format csv
fruitloops olf pathway PN KC --dataset flywire --region MB --format csv
fruitloops olf edges --dataset flywire --region LH --min-synapses 5 --format csv
```

There is not yet a separate docs site or command reference. The current
documentation lives in this README, `AGENTS.md`, `RELEASE.md`, and CLI help
from `fruitloops --help` / `fruitloops olf --help`.

## Generic Plotting

Plotting is reusable and table-agnostic.

Render from any `fruitloops` table reference:

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

Or render from any CSV path:

```bash
fruitloops admin plot \
  --csv path/to/table.csv \
  --kind scatter \
  --x x_column \
  --y y_column \
  --output outputs/my_scatter
```

Other generic plot kinds:

```bash
fruitloops admin plot --table comparison:matched_ln_class_similarity --kind bar --x LN_class --y orn_input_distribution_correlation --output outputs/orn_corr_bar
fruitloops admin plot --table flywire:source_audit/orn_partner_counts_by_hemisphere --kind violin --x input_relation --value n_synapses --where LN_type=il3LN6 --output outputs/il3ln6_orn_violin
fruitloops admin plot --table flywire:source_audit/orn_partner_counts_by_hemisphere --kind heatmap --x glomerulus --y input_relation --value n_synapses --where LN_type=il3LN6 --output outputs/il3ln6_orn_heatmap
fruitloops admin plot --table comparison:matched_ln_class_similarity --kind bubble --x orn_input_distribution_correlation --y pn_output_distribution_correlation --size flywire_orn_input_total --color flywire_contra_fraction --label LN_class --output outputs/similarity_bubble
```

The wrapper script is equivalent:

```bash
python scripts/plot_csv.py --csv path/to/table.csv --kind hist --value score --output outputs/score_hist
```

## Live Connectome Access

Live database access is optional. Credentials come from environment variables or
from a local `.env` file. `.env` is ignored by git; start from `.env.example`.

```bash
cp .env.example .env
```

Use a different env file with `--env-file path/to/file.env`.

Hemibrain uses `neuprint-python`:

```bash
export NEUPRINT_SERVER=neuprint.janelia.org
export NEUPRINT_DATASET=hemibrain:v1.2.1
export NEUPRINT_APPLICATION_CREDENTIALS=<neuprint-token>

fruitloops admin live hemibrain neurons --type-contains il3LN6 --limit 5 --format csv
fruitloops admin live hemibrain connections --upstream-body-id 5813018460 --limit 20 --format json
fruitloops admin live hemibrain cypher --query 'MATCH (n:Neuron) RETURN n.bodyId AS bodyId, n.type AS type LIMIT 5'
```

FlyWire uses `caveclient`:

```bash
export FLYWIRE_DATASTACK=flywire_fafb_public
export CAVE_AUTH_TOKEN=<cave-token>

fruitloops admin live flywire tables --format csv
fruitloops admin live flywire table --table synapses_nt_v1 --in pre_pt_root_id=720575940623636701 --limit 10 --format csv
fruitloops admin live flywire synapses --pre-root-id 720575940623636701 --limit 10 --format json
```

Script shortcuts are equivalent:

```bash
python scripts/live_hemibrain.py neurons --type-contains il3LN6 --limit 5
python scripts/live_flywire.py tables
```

## Offline-First Live Cache

Use `offline fetch` when you want local data first and live APIs only on cache
miss. Results are saved under `cache/live/`, which is ignored by git.

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
fruitloops admin offline fetch --dataset flywire --action tables --offline-only
fruitloops admin offline fetch --dataset hemibrain --action neurons --type-contains il3LN6 --limit 5
```

## Bulk Offline Releases

Bulk releases should be the primary offline source when you need broad
connectivity, with live/cache queries only filling gaps.

List known public release files:

```bash
fruitloops admin bulk sources
```

Download the practical FlyWire connection table first:

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
fruitloops admin bulk connections --table flywire_proofread_connections --pre-id ROOT --limit 20 --format csv
fruitloops admin bulk inputs --table flywire_proofread_connections --body-id ROOT --format csv
fruitloops admin bulk outputs --table flywire_proofread_connections --body-id ROOT --format csv
fruitloops admin bulk partners --table flywire_proofread_connections --body-id ROOT --format json
fruitloops admin bulk views --table flywire_proofread_connections --prefix flywire
fruitloops admin bulk optimize --table flywire_proofread_connections --prefix flywire
```

Hemibrain's compact adjacency and Neo4j bundles are CSV archives; extract first,
then import the CSVs you need:

```bash
hemibrain_path=$(fruitloops admin bulk download --dataset hemibrain --kind compact-adjacencies)
fruitloops admin bulk extract --path "$hemibrain_path"
fruitloops admin bulk import \
  --path "$(fruitloops status --csv | awk -F, '$2=="bulk_dir"{print $4}')/extracted/exported-traced-adjacencies-v1.2/traced-roi-connections.csv" \
  --table hemibrain_traced_roi_connections \
  --replace
fruitloops admin bulk import \
  --path "$(fruitloops status --csv | awk -F, '$2=="bulk_dir"{print $4}')/extracted/exported-traced-adjacencies-v1.2/traced-total-connections.csv" \
  --table hemibrain_traced_total_connections \
  --replace
fruitloops admin bulk import \
  --path "$(fruitloops status --csv | awk -F, '$2=="bulk_dir"{print $4}')/extracted/exported-traced-adjacencies-v1.2/traced-neurons.csv" \
  --table hemibrain_traced_neurons \
  --replace
neo4j_path=$(fruitloops admin bulk download --dataset hemibrain --kind neo4j-inputs)
fruitloops admin bulk extract --path "$neo4j_path"
```

End-to-end offline setup:

```bash
fruitloops setup
fruitloops admin bulk tables
```

`flywire_synapses_783.feather` is much larger than the proofread connection
table. Fruitloops streams Feather imports through Arrow record batches, but the
resulting DuckDB database still needs enough local disk for the imported table
and indexes.

## Output Formats

Most commands support `--format table`, `--format csv`, `--format json`, or
`--format jsonl`. CSV and JSONL are intended for downstream agent pipelines.

## Rebuilding the Data Snapshot

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

The script copies generated CSVs and rewrites `data/manifest.csv`.

## Test

```bash
python -m unittest discover -s tests
```
