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
