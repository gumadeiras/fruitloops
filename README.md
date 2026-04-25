# fruitloops

Agent-friendly CLI for querying connectome analysis tables from hemibrain and
FlyWire.

The repository keeps generated CSV products in a predictable layout:

```text
data/
  manifest.csv
  hemibrain/
  flywire/
  comparison/
```

## Quick Use

Run directly from the repository:

```bash
python -m fruitloops datasets
python -m fruitloops files --dataset flywire --contains summary
python -m fruitloops head --table flywire:analysis_outputs/full_summary
python -m fruitloops query --table comparison:matched_ln_class_similarity --contains LN_class=il3LN6 --format json
python -m fruitloops ln il3LN6 --dataset flywire --format json
python -m fruitloops partners il3LN6 --dataset flywire --kind orn --format csv
python -m fruitloops compare il3LN6 --format json
```

For editable installation:

```bash
python -m pip install -e .
fruitloops datasets
```

## Table References

Tables can be referenced as:

- `dataset:relative/path/without_csv`
- `dataset:collection/file_stem`
- `file_id` from `data/manifest.csv`

Examples:

```bash
fruitloops schema --table flywire:analysis_outputs/full_summary
fruitloops query --table hemibrain:analysis_outputs/full_summary --select bodyId,LN_type,input_preference
fruitloops query --table flywire:source_audit/ln_observations_by_hemisphere --where LN_type=il3LN6
fruitloops query --table flywire:source_audit/orn_partner_counts_by_hemisphere --where LN_type=il3LN6 --format csv
fruitloops path --table comparison:matched_ln_class_similarity
```

## Common Agent Queries

Aggregate any table without pandas:

```bash
fruitloops aggregate \
  --table flywire:source_audit/orn_partner_counts_by_hemisphere \
  --where LN_type=il3LN6 \
  --by LN_type,analysis_hemisphere,input_relation \
  --sum n_synapses \
  --format csv
```

Summarize ORN or PN partners for one LN:

```bash
fruitloops partners il3LN6 --dataset flywire --kind orn --format csv
fruitloops partners il3LN6 --dataset flywire --kind pn --format csv
fruitloops partners il3LN6 --dataset hemibrain --kind orn --format csv
fruitloops partners il3LN6 --dataset hemibrain --kind pn --format csv
```

Pull the reconciled hemibrain/FlyWire comparison:

```bash
fruitloops compare il3LN6 --format json
```

## Generic Plotting

Plotting is reusable and table-agnostic. Install the plotting extra when needed:

```bash
python -m pip install -e '.[plot]'
```

Render from any `fruitloops` table reference:

```bash
fruitloops plot \
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
fruitloops plot \
  --csv path/to/table.csv \
  --kind scatter \
  --x x_column \
  --y y_column \
  --output outputs/my_scatter
```

Other generic plot kinds:

```bash
fruitloops plot --table comparison:matched_ln_class_similarity --kind bar --x LN_class --y orn_input_distribution_correlation --output outputs/orn_corr_bar
fruitloops plot --table flywire:source_audit/orn_partner_counts_by_hemisphere --kind violin --x input_relation --value n_synapses --where LN_type=il3LN6 --output outputs/il3ln6_orn_violin
fruitloops plot --table flywire:source_audit/orn_partner_counts_by_hemisphere --kind heatmap --x glomerulus --y input_relation --value n_synapses --where LN_type=il3LN6 --output outputs/il3ln6_orn_heatmap
fruitloops plot --table comparison:matched_ln_class_similarity --kind bubble --x orn_input_distribution_correlation --y pn_output_distribution_correlation --size flywire_orn_input_total --color flywire_contra_fraction --label LN_class --output outputs/similarity_bubble
```

The wrapper script is equivalent:

```bash
python scripts/plot_csv.py --csv path/to/table.csv --kind hist --value score --output outputs/score_hist
```

## Live Connectome Access

Live database access is optional. Credentials come from environment variables or
from a local `.env` file. `.env` is ignored by git; start from `.env.example`.

```bash
python -m pip install -e '.[live]'
cp .env.example .env
```

Use a different env file with `--env-file path/to/file.env`.

Hemibrain uses `neuprint-python`:

```bash
export NEUPRINT_SERVER=neuprint.janelia.org
export NEUPRINT_DATASET=hemibrain:v1.2.1
export NEUPRINT_APPLICATION_CREDENTIALS=<neuprint-token>

fruitloops live hemibrain neurons --type-contains il3LN6 --limit 5 --format csv
fruitloops live hemibrain connections --upstream-body-id 5813018460 --limit 20 --format json
fruitloops live hemibrain cypher --query 'MATCH (n:Neuron) RETURN n.bodyId AS bodyId, n.type AS type LIMIT 5'
```

FlyWire uses `caveclient`:

```bash
export FLYWIRE_DATASTACK=flywire_fafb_public
export CAVE_AUTH_TOKEN=<cave-token>

fruitloops live flywire tables --format csv
fruitloops live flywire table --table synapses_nt_v1 --in pre_pt_root_id=720575940623636701 --limit 10 --format csv
fruitloops live flywire synapses --pre-root-id 720575940623636701 --limit 10 --format json
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
fruitloops offline fetch \
  --dataset flywire \
  --action synapses \
  --pre-root-id 720575940623636701 \
  --limit 10 \
  --format csv
```

Repeat the same command to read the cached CSV. Use `--offline-only` to fail
instead of hitting the network, or `--refresh` to force a live re-fetch.

```bash
fruitloops offline list
fruitloops offline fetch --dataset flywire --action tables --offline-only
fruitloops offline fetch --dataset hemibrain --action neurons --type-contains il3LN6 --limit 5
```

## Output Formats

Most commands support `--format table`, `--format csv`, `--format json`, or
`--format jsonl`. CSV and JSONL are intended for downstream agent pipelines.

## Rebuilding the Data Snapshot

From the paper repository root:

```bash
python scripts/build_data_snapshot.py \
  --source "/path/to/widespread-direction-selectivity" \
  --dest data
```

The script copies generated CSVs and rewrites `data/manifest.csv`.

## Test

```bash
python -m unittest discover -s tests
```
