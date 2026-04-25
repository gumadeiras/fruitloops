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
