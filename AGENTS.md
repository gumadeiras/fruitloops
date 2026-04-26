# AGENTS.md

Fruitloops is an offline-first connectome query repo for hemibrain and FlyWire.
Prefer local data before live APIs.

## Rules

- Do not commit secrets. `.env` and `cache/` are ignored.
- Use `.env.example` for required env names.
- Use `python3 -m fruitloops ...` from repo root.
- Prefer CSV/JSON/JSONL output for downstream analysis.
- For large live/API results, use `offline fetch` so results are cached.
- If data is missing locally, fetch once, cache it, then reuse cache.
- For broad connectivity, use `bulk` DuckDB tables before live APIs.

## Local Snapshot

CSV snapshot layout:

```text
data/
  manifest.csv
  hemibrain/
  flywire/
  comparison/
```

Find tables:

```bash
python3 -m fruitloops datasets
python3 -m fruitloops files --dataset flywire --contains summary
python3 -m fruitloops schema --table comparison:matched_ln_class_similarity
python3 -m fruitloops path --table flywire:analysis_outputs/full_summary
```

Query tables:

```bash
python3 -m fruitloops query \
  --table comparison:matched_ln_class_similarity \
  --contains LN_class=il3LN6 \
  --format json

python3 -m fruitloops aggregate \
  --table flywire:source_audit/orn_partner_counts_by_hemisphere \
  --where LN_type=il3LN6 \
  --by LN_type,analysis_hemisphere,input_relation \
  --sum n_synapses \
  --format csv
```

## Common Connectome Queries

```bash
python3 -m fruitloops ln il3LN6 --dataset flywire --format json
python3 -m fruitloops partners il3LN6 --dataset flywire --kind orn --format csv
python3 -m fruitloops partners il3LN6 --dataset hemibrain --kind pn --format csv
python3 -m fruitloops compare il3LN6 --format json
```

## Olfaction Cache

Use `olfaction` for AL/LH/MB questions. Build once from imported DuckDB bulk
tables, then query offline:

```bash
python3 -m fruitloops olfaction build
python3 -m fruitloops olfaction neurons --region AL --class ORN --format csv
python3 -m fruitloops olfaction pns --glomerulus DM1 --format csv
python3 -m fruitloops olfaction orn-inputs --glomerulus DM1 --by-side --format csv
python3 -m fruitloops olfaction edges --region LH --min-synapses 5 --format csv
```

For complete labels, cache annotations once from live APIs, then query offline:

```bash
python3 -m fruitloops olfaction cache-annotations --dataset hemibrain
python3 -m fruitloops olfaction cache-annotations --dataset flywire
```

Expected source tables:

- `flywire_proofread_connections`
- `hemibrain_traced_roi_connections`
- `hemibrain_olfaction_neuron_annotations` or `hemibrain_traced_neurons`
- optional FlyWire annotations: `flywire_hierarchical_neuron_annotations`,
  `flywire_neuron_information_v2`

## Offline-First Live Fetch

Requires local `.env` and live extras:

```bash
python3 -m pip install -e '.[live]'
cp .env.example .env
```

Fetch with cache:

```bash
python3 -m fruitloops offline fetch \
  --dataset flywire \
  --action synapses \
  --pre-root-id 720575940623636701 \
  --limit 10 \
  --format csv
```

Then reuse without network:

```bash
python3 -m fruitloops offline fetch ... --offline-only
python3 -m fruitloops offline list
```

Force update:

```bash
python3 -m fruitloops offline fetch ... --refresh
```

## Bulk Offline Releases

For broad connectivity, prefer public bulk releases over live APIs.

```bash
python3 -m fruitloops bulk sources
python3 -m fruitloops bulk download --dataset flywire --kind proofread-connections
python3 -m pip install -e '.[bulk]'
python3 -m fruitloops bulk import \
  --path bulk/raw/flywire/proofread_connections_783.feather \
  --table flywire_proofread_connections \
  --replace
python3 -m fruitloops bulk optimize --table flywire_proofread_connections --prefix flywire
python3 -m fruitloops bulk query --table flywire_proofread_connections --limit 10 --format csv
python3 -m fruitloops bulk inputs --table flywire_proofread_connections --body-id ROOT --format csv
python3 -m fruitloops bulk outputs --table flywire_proofread_connections --body-id ROOT --format csv
python3 -m fruitloops bulk partners --table flywire_proofread_connections --body-id ROOT --format json
```

Hemibrain compact setup:

```bash
python3 -m fruitloops bulk download --dataset hemibrain --kind compact-adjacencies
python3 -m fruitloops bulk extract --path bulk/raw/hemibrain/exported-traced-adjacencies-v1.2.tar.gz
python3 -m fruitloops bulk import \
  --path bulk/extracted/exported-traced-adjacencies-v1.2/traced-roi-connections.csv \
  --table hemibrain_traced_roi_connections \
  --replace
python3 -m fruitloops bulk optimize --table hemibrain_traced_roi_connections --prefix hemibrain
```

LN workflow:

```bash
python3 -m fruitloops ln il3LN6 --format csv
python3 -m fruitloops query --table flywire:source_audit/ln_observations_by_hemisphere --where LN_type=il3LN6 --format csv
python3 -m fruitloops compare il3LN6 --format jsonl
```

Known large sources:

- FlyWire `proofread-connections`: practical neuron-neuron connectivity table.
- FlyWire `synapses`: full synapse-level table, very large.
- Hemibrain `compact-adjacencies`: practical compact traced-neuron CSV bundle.
- Hemibrain `neo4j-inputs`: full neuPrint import CSV bundle.

## Live APIs

Use live APIs only when local snapshot/cache lacks the answer.

```bash
python3 -m fruitloops live flywire tables --format csv
python3 -m fruitloops live flywire synapses --pre-root-id ROOT --limit 10 --format json

python3 -m fruitloops live hemibrain neurons --type-contains il3LN6 --limit 5 --format csv
python3 -m fruitloops live hemibrain connections --upstream-body-id BODY --limit 20 --format json
```

## Plotting

Requires plotting extra:

```bash
python3 -m pip install -e '.[plot]'
```

Render from a table or any CSV:

```bash
python3 -m fruitloops plot \
  --table comparison:matched_ln_class_similarity \
  --kind scatter \
  --x hemibrain_mean_contra_preference \
  --y flywire_mean_contra_preference \
  --label LN_class \
  --output outputs/contra_preference_scatter
```

## Rebuild Snapshot

From this repo:

```bash
python3 scripts/build_data_snapshot.py \
  --source "/path/to/widespread-direction-selectivity" \
  --dest data
```

## Verify

```bash
python3 -m unittest discover -s tests
```
