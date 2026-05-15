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

Run setup when you want the larger local offline stores and derived olfaction
tables:

```bash
fruitloops setup
```

For the best labels/glomerulus coverage, fetch live annotations while setting
up. This requires neuPrint/FlyWire tokens:

```bash
export NEUPRINT_AUTH_TOKEN=...
export CAVE_AUTH_TOKEN=...
fruitloops setup --cache-annotations
```

Setup is offline-first. Hemibrain compact adjacencies do not include most ORNs,
so broad hemibrain ORN glomerulus queries need `--cache-annotations` or a later
`fruitloops olf cache-annotations --hemibrain`.

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

Full install/setup details, flags, credentials, paths, bulk imports, and live
cache behavior are documented in [docs/setup.md](docs/setup.md).

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

Usable `olf` vocabulary:

- Datasets: `--flywire`, `--hemibrain`, or `--dataset flywire|hemibrain`
- Regions: `AL`, `LH`, `MB`
- Neuron classes: `ORN`, `PN`, `LN`, `LHN`, `KC`, `MBON`, `APL`, `DAN`
- Formats: `--csv`, `--json`, `--jsonl`, or `--format table|csv|json|jsonl`
- Side breakdown: `--by-side`
- Body ids: `--pre-id`, `--post-id`, `--source-id`, `--target-id`

Discover values present in the current store:

```bash
fruitloops olf classes --csv
fruitloops olf glomerulus --flywire --limit 1000 --csv
fruitloops olf glomerulus --hemibrain --limit 1000 --csv
fruitloops olf tables --csv
```

Full local stores typically expose roughly 60-80 glomerulus labels per
dataset, depending on imported annotations and whether hemibrain live ORN->PN
edges have been cached.

Command shapes:

```bash
fruitloops olf neurons --class ORN --region AL --glomerulus DM1 --flywire --csv
fruitloops olf classes --region AL --flywire --csv
fruitloops olf glomerulus DM1 --flywire --csv
fruitloops olf pathway ORN PN --source-glomerulus DM1 --target-glomerulus DM1 --by-side --flywire --csv
fruitloops olf inputs --target-class PN --source-class ORN --glomerulus DM1 --by-side --flywire --csv
fruitloops olf outputs --source-class PN --target-class KC --region MB --by-side --flywire --csv
fruitloops olf edges --region AL --pre-id 720575940623636701 --min-synapses 5 --flywire --csv
fruitloops olf pns --glomerulus DM1 --hemibrain --csv
fruitloops olf orn-inputs --glomerulus DM1 --by-side --flywire --csv
```

Hemibrain compact adjacencies only include traced neurons, so most hemibrain
ORNs are absent from the compact cache. For hemibrain ORN->PN glomerulus
queries across the full ORN set, cache live neuPrint ORN->PN edges once:

```bash
fruitloops olf cache-annotations --hemibrain --csv
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

Build/query derived AL/LH/MB tables:

```bash
fruitloops setup
fruitloops olf tables
fruitloops olf neurons --dataset flywire --region AL --class ORN --format csv
fruitloops olf classes --dataset flywire --region AL --format csv
fruitloops olf glomerulus DM1 --dataset flywire --format csv
fruitloops olf inputs --dataset hemibrain --target-class PN --source-class ORN --glomerulus DM3 --by-side --format csv
fruitloops olf outputs --dataset flywire --source-class PN --target-class KC --region MB --format csv
fruitloops olf pathway PN KC --dataset flywire --region MB --format csv
fruitloops olf edges --dataset flywire --region LH --min-synapses 5 --format csv
```

Full olfaction table details, live access, offline cache behavior, bulk imports,
and plotting examples live in [docs/setup.md](docs/setup.md).

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
