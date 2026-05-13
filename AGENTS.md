# AGENTS.md

## Git

- Commit with `scripts/committer "<subject>" -- <path>...`; it stages only listed paths. Use `--body` or `--body-file` for commit bodies.


Fruitloops is an offline-first olfactory connectome query repo for hemibrain
and FlyWire. Prefer local data before live APIs.

## Rules

- Do not commit secrets. `.env` and `cache/` are ignored.
- Use `.env.example` for required env names.
- Use `python3 -m fruitloops ...` from repo root.
- Prefer CSV/JSON/JSONL output for downstream analysis.
- Prefer top-level `status`, `setup`, `olf`, `table`, `find`, `partners`, and `examples`.
- Use `admin` for advanced commands: `bulk`, `live`, `offline`, and `plot`.
- Do not install extras; one editable install includes runtime dependencies: `python3 -m pip install -e .`.
- Run `python3 -m fruitloops setup` to create cache dirs, import practical bulk tables, and build olfaction tables.
- For large live/API results, use `admin offline fetch` so results are cached.
- If data is missing locally, fetch once, cache it, then reuse cache.
- For broad connectivity, use `admin bulk` DuckDB tables before live APIs.

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
python3 -m fruitloops status --csv
python3 -m fruitloops table --flywire --contains summary --csv
python3 -m fruitloops table comparison:matched_ln_class_similarity --schema --csv
python3 -m fruitloops table flywire:analysis_outputs/full_summary --path
```

Query tables:

```bash
python3 -m fruitloops table comparison:matched_ln_class_similarity \
  --contains LN_class=il3LN6 \
  --json

python3 -m fruitloops table flywire:source_audit/orn_partner_counts_by_hemisphere \
  --where LN_type=il3LN6 \
  --by LN_type,analysis_hemisphere,input_relation \
  --sum n_synapses \
  --csv
```

## Common Connectome Queries

```bash
python3 -m fruitloops olf classes --flywire --region AL --csv
python3 -m fruitloops olf glomerulus DM1 --flywire --csv
python3 -m fruitloops olf inputs --target-class PN --source-class ORN --glomerulus DM1 --by-side --csv
python3 -m fruitloops olf outputs --source-class PN --target-class KC --region MB --flywire --csv
python3 -m fruitloops olf pathway PN KC --region MB --flywire --csv
python3 -m fruitloops olf outputs --source-class PN --target-class LHN --glomerulus DA2 --region LH --flywire --by-side --csv
python3 -m fruitloops olf outputs --source-class PN --target-class KC --glomerulus DA2 --region MB --flywire --by-side --csv
python3 -m fruitloops find il3LN6 --flywire --json
python3 -m fruitloops partners il3LN6 --flywire --orn --csv
python3 -m fruitloops partners il3LN6 --hemibrain --pn --csv
python3 -m fruitloops table comparison:matched_ln_class_similarity --contains LN_class=il3LN6 --json
```

## Olfaction Cache

Use `olf` for AL/LH/MB questions. Build once with setup from imported DuckDB
bulk tables, then query offline:

```bash
python3 -m fruitloops setup
python3 -m fruitloops olf neurons --region AL --class ORN --format csv
python3 -m fruitloops olf pns --glomerulus DM1 --format csv
python3 -m fruitloops olf inputs --target-class PN --source-class ORN --glomerulus DM1 --by-side --format csv
python3 -m fruitloops olf edges --region LH --min-synapses 5 --format csv
```

For complete labels, cache annotations once from live APIs, then query offline:

```bash
python3 -m fruitloops olf cache-annotations --dataset hemibrain
python3 -m fruitloops olf cache-annotations --dataset flywire
```

Expected source tables:

- `flywire_proofread_connections`
- `hemibrain_traced_roi_connections`
- `hemibrain_olfaction_neuron_annotations` or `hemibrain_traced_neurons`
- optional FlyWire annotations: `flywire_hierarchical_neuron_annotations`,
  `flywire_neuron_information_v2`

## Offline-First Live Fetch

Requires local `.env`; runtime dependencies are included by the editable install:

```bash
python3 -m pip install -e .
cp .env.example .env
```

Fetch with cache:

```bash
python3 -m fruitloops admin offline fetch \
  --dataset flywire \
  --action synapses \
  --pre-root-id 720575940623636701 \
  --limit 10 \
  --format csv
```

Then reuse without network:

```bash
python3 -m fruitloops admin offline fetch ... --offline-only
python3 -m fruitloops admin offline list
```

Force update:

```bash
python3 -m fruitloops admin offline fetch ... --refresh
```

## Bulk Offline Releases

For broad connectivity, prefer public bulk releases over live APIs.

```bash
python3 -m fruitloops admin bulk sources
python3 -m fruitloops admin bulk download --dataset flywire --kind proofread-connections
python3 -m pip install -e .
python3 -m fruitloops admin bulk import \
  --path bulk/raw/flywire/proofread_connections_783.feather \
  --table flywire_proofread_connections \
  --replace
python3 -m fruitloops admin bulk optimize --table flywire_proofread_connections --prefix flywire
python3 -m fruitloops admin bulk query --table flywire_proofread_connections --limit 10 --format csv
python3 -m fruitloops admin bulk inputs --table flywire_proofread_connections --body-id ROOT --format csv
python3 -m fruitloops admin bulk outputs --table flywire_proofread_connections --body-id ROOT --format csv
python3 -m fruitloops admin bulk partners --table flywire_proofread_connections --body-id ROOT --format json
```

Hemibrain compact setup:

```bash
python3 -m fruitloops admin bulk download --dataset hemibrain --kind compact-adjacencies
python3 -m fruitloops admin bulk extract --path bulk/raw/hemibrain/exported-traced-adjacencies-v1.2.tar.gz
python3 -m fruitloops admin bulk import \
  --path bulk/extracted/exported-traced-adjacencies-v1.2/traced-roi-connections.csv \
  --table hemibrain_traced_roi_connections \
  --replace
python3 -m fruitloops admin bulk optimize --table hemibrain_traced_roi_connections --prefix hemibrain
```

LN workflow:

```bash
python3 -m fruitloops find il3LN6 --csv
python3 -m fruitloops table flywire:source_audit/ln_observations_by_hemisphere --where LN_type=il3LN6 --csv
python3 -m fruitloops table comparison:matched_ln_class_similarity --contains LN_class=il3LN6 --jsonl
```

Known large sources:

- FlyWire `proofread-connections`: practical neuron-neuron connectivity table.
- FlyWire `synapses`: full synapse-level table, very large.
- Hemibrain `compact-adjacencies`: practical compact traced-neuron CSV bundle.
- Hemibrain `neo4j-inputs`: full neuPrint import CSV bundle.

## Live APIs

Use live APIs only when local snapshot/cache lacks the answer.

```bash
python3 -m fruitloops admin live flywire tables --format csv
python3 -m fruitloops admin live flywire synapses --pre-root-id ROOT --limit 10 --format json

python3 -m fruitloops admin live hemibrain neurons --type-contains il3LN6 --limit 5 --format csv
python3 -m fruitloops admin live hemibrain connections --upstream-body-id BODY --limit 20 --format json
```

## Plotting

Runtime dependencies include plotting support:

```bash
python3 -m pip install -e .
```

Render from a table or any CSV:

```bash
python3 -m fruitloops admin plot \
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

## Changelog

- Keep `CHANGELOG.md` updated for user-facing changes. If a commit adds a feature, fix, behavior change, CLI change, GUI change, output-format change, install/release change, or other user-visible change, add or update an entry under the top `Unreleased` section in the same commit.
- Never edit released changelog sections for current work. Corrections, renames, and behavior changes after a release must be recorded only under the top `Unreleased` section unless Gustavo explicitly asks for release-history repair.
- Use these sections when they apply: `Features`, `Fixes`, and `Changes`.
- Omit empty sections.
- Write user-facing entries instead of repository chore notes.
- Do not include pure tests, internal refactors, CI-only changes, or docs-only changes unless they affect user behavior, API, installation, or usage.
