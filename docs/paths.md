# Whole-Brain Paths and Reach

`fruitloops neurons`, `fruitloops paths`, and `fruitloops reach` answer route
questions on the offline FlyWire v783 and hemibrain v1.2 connectomes. For
example: what are the shortest and strongest olfactory routes from projection
neurons (PNs) to DNa02, split by first-synapse region and by side?

All three commands run offline after setup.

## Setup

```bash
fruitloops setup --flywire
fruitloops setup --hemibrain
fruitloops status
```

FlyWire setup imports two sources:

- `flywire_proofread_connections`: v783 proofread connections, one row per
  neuron pair and neuropil (Zenodo record 10676866).
- `flywire_neuron_annotations`: whole-brain annotations from Schlegel et al.
  (2024), `Supplemental_file1_neuron_annotations.tsv`. The file is pinned to
  commit `a83b2776d60d5764cef36b927f5f9679c16c47a2` of
  `flyconnectome/flywire_annotations`. Setup checks its sha256
  (`b214970b55d2fbe0853bba536fdcb9e28730f4eb7ab06f600491df795da683cd`) and
  refuses a file that does not match. If the download or the check fails,
  setup reports an `error` row and still builds the graph and `olf` stages.
  `neurons`, `paths`, and `reach` then stop until a later setup imports the
  table.

Hemibrain setup imports the v1.2 compact traced-neuron tables, such as
`hemibrain_traced_roi_connections` and `hemibrain_traced_neurons`, and
`hemibrain_body_neurotransmitters`, the pinned per-body transmitter
predictions (see [Hemibrain predictions](#hemibrain-predictions)). If the
prediction download or its sha256 check fails, setup reports an `error` row
and still builds the graph and `olf` stages. `neurons`, `paths`, and `reach`
with `--hemibrain` then stop until a later setup imports the table.

Setup then builds a sparse graph cache for each dataset, at
`<store stem>.graphs/<dataset>.npz` next to the DuckDB store, for example
`fruitloops.graphs/flywire.npz`. The FlyWire cache is about 200 MB and builds
in a few seconds; the hemibrain cache is about 53 MB. A build only reads the
store.

The cache records the fingerprint of the dataset's connection table: its row
count, its columns, and the file of its last import through fruitloops
(`setup` or `admin bulk import`). When the fingerprint changes, or the cache
file is unreadable, the next setup or query rebuilds the cache. Edits made
directly in DuckDB, for example an SQL `UPDATE`, do not change the fingerprint;
re-import the table instead. `fruitloops status` lists each cache as `current`,
`stale`, `unreadable`, or `missing`, or as `unavailable` while another process
holds the store's write lock.

The existing `olf` tables do not use the whole-brain annotations, so their
output does not change.

## Selectors

Each selector flag has one vocabulary. `paths` and `reach` use `--source-*`
and `--target-*` flags. `neurons` uses the same flags without a prefix.

| Flag | Vocabulary | Example |
| --- | --- | --- |
| `--source-type`, `--target-type` | FlyWire `cell_type` or hemibrain `type`. Exact names or shell wildcards. | `DNa02`, `'DNa*'` |
| `--source-class`, `--target-class` | FlyWire `cell_class` | `ALPN`, `Kenyon_Cell`, `MBON` |
| `--source-super-class`, `--target-super-class` | FlyWire `super_class` | `descending`, `central` |
| `--source-id`, `--target-id` | FlyWire root ids or hemibrain body ids | `720575940604737708` |

Values in one flag combine with OR. Repeat the flag or separate values with
commas. Different flags combine with AND, so
`--target-super-class descending --target-type 'DNa*'` selects descending
neurons whose type starts with `DNa`.

A selector that matches nothing stops the command with an error. For unknown
names the error suggests close matches. If you pass an `olf` class name, the
error gives the whole-brain equivalent: `PN` -> `ALPN`, `LN` -> `ALLN`,
`KC` -> `Kenyon_Cell`, and `ORN` -> `olfactory`. Hemibrain has no class
annotations, so there the error names a type pattern instead.

```bash
fruitloops neurons --flywire --type DNa02 --csv
fruitloops neurons --flywire --type 'PFL*' --csv
fruitloops neurons --flywire --type 'LAL030*' --csv
```

The hemibrain compact export has types and instances only. For hemibrain, use
`--*-type` and `--*-id`. Antennal-lobe PN types follow the pattern
`<glomeruli>_<tract>PN<suffix>`, for example `DA1_lPN`, `M_l2PNl20`, and
`VP1d+VP4_l2PN1`. The wildcard `'*_*PN*'` selects these types. It does not
select WEDPN or LPN types. Hemibrain side comes from the `_R`/`_L` instance
suffix.

## Definitions

- Pair synapses are the sum over neuropils. A directed edge is kept when its
  pair synapses are at least `--min-synapses` (default 5).
- Edge weight = pair synapses / all input synapses of the postsynaptic neuron.
  The denominator counts every presynaptic partner in the connection table,
  with no threshold. For hemibrain, this means all traced partners.
- Source neurons contribute out-edges only on the first hop. A path never
  passes through a source neuron after the first hop.
- Route: the neuropil class of the first-hop synapses.

  | Route | FlyWire neuropils | Hemibrain ROIs |
  | --- | --- | --- |
  | `AL` | `AL_*` | `AL(R)`, `AL(L)` |
  | `LH` | `LH_*` | `LH(R)`, `LH(L)` |
  | `MB` | `MB_*` | `CA`, `PED`, `aL`, `a'L`, `bL`, `b'L`, `gL` |
  | `other` | all other neuropils | all other ROIs |
  | `kc` | the first relay is a Kenyon cell (any neuropil) | type starts with `KC` |
  | `all` | every first-hop synapse | every first-hop synapse |

  For `AL`, `LH`, `MB`, and `other`, the first-hop weight uses only the
  synapses in that neuropil class. The edge must still pass the pair-synapse
  threshold. Thus a 17-synapse pair with 1 synapse in MB gives an `MB`
  first hop of weight 1 / (input synapses). The four neuropil routes add up to
  `all`.
- Strongest path = maximum product of weights. The search finds the minimum
  sum of -log(weight) within `--max-hops` hops (default 6). With weighted
  seeds, the cost also includes -log(seed weight), so `strength` includes the
  seed weight.
- Shortest path = fewest hops over kept edges, from sources with a positive
  seed (`shortest_hops`).
- Reach at hop k = sum over all length-k paths of the product of weights:
  `v_k = W^T v_(k-1)`, with `v_0` = seed weights. The first step uses only the
  route-restricted source out-edges. Later steps use `W` without source
  out-edges. This is a structural index, not a model of activity.
- Type values are the mean over all target neurons of the type. Target neurons
  without connections count as 0. Ranks are among the target set's types
  (or neurons with `--per-neuron`). Rank 1 is the largest value, and tied
  values share the best rank.
- Untyped neurons are grouped under a bracketed label such as `[central]`.

## ORN Seed Weighting (FlyWire)

Without ORN options, each source neuron has seed weight 1. With ORN options,
each source neuron is seeded by its input fraction from a chosen ORN set:

seed(PN) = synapses from the ORN set onto the PN / all input synapses of the PN.

All ORN synapses count, with no threshold.

- `--orn-family orco|ir|gr|amt|thermo|hygro`: all verified glomeruli of a
  receptor family.
- `--orn-glomerulus NAME`: one glomerulus, for example `DA1` or `VP2`.
- `--orn-side left|right`: only ORNs from one antenna side. Use it with a
  family or a glomerulus. `reach --by-side` sets the side itself, so it rejects
  `--orn-side`.

ORNs are FlyWire sensory neurons of type `ORN_<glomerulus>`,
`TRN_<glomerulus>`, or `HRN_<glomerulus>`. Families come from the packaged
table `fruitloops/curated/glomerulus_receptor_families.csv`. Each row gives
the receptor, the primary source, the DOI, and the location of the evidence.
Rows marked `verified=false` are never used for family seeds.

- `orco`: glomeruli whose tuning receptor is an odorant receptor (Or). The 38
  verified Orco glomeruli are all olfactory ORN glomeruli except the IR,
  GR, and Amt glomeruli below.
- `ir`: DC4, DL2d, DL2v, DP1l, DP1m, VC5, VL1, VL2a, VL2p, VM1, VM4. VL1 is
  an Ir75d glomerulus (Silbering et al. 2011).
- `gr`: V (Gr21a/Gr63a).
- `amt`: VM6v, VM6m, VM6l. These are ammonium-transporter (Amt, Rh50+)
  neurons. The Orco knock-in does not label them (Task et al. 2022; Vulpe et
  al. 2021).
- `thermo`: VP2, VP3a. `hygro`: VP1d, VP4, VP5.
- Unverified: VP1l and VP1m. FlyWire types them `HRN_VP1l` and `TRN_VP1m`,
  but their receptors (Ir21a and Ir68a; Marin et al. 2020) suggest the
  opposite modalities. VP3b is also unverified, because its receptor was not
  tested at the subtype level.
- Orco coexpression in IR ORNs is not modelled. Each glomerulus has one
  family, set by its tuning receptor.

Orco-weighted values depend on this set. For example, the multiglomerular
`M_l2PNl20` receives VM6 input, so counting VM6 as Orco raises its seed weight.

ORN weighting is FlyWire-only. The hemibrain compact export lacks most ORNs and
their glomerulus labels.

## Transmitter Signs

Signed mode uses the top transmitter prediction of each presynaptic neuron.
Both datasets use predictions of the classifier of Eckstein et al. (2024):
FlyWire uses `top_nt` of the pinned annotation table, and hemibrain uses the
pinned per-body file described [below](#hemibrain-predictions). Signs:

- acetylcholine: +1
- GABA: -1
- glutamate: -1
- any other or unknown transmitter, including `neither`: 0

The packaged table `fruitloops/curated/transmitter_overrides.csv` sets all
Kenyon cells to acetylcholine (Barnstedt et al. 2016). The classifier predicts
dopamine for most FlyWire Kenyon cells and for all 1,927 hemibrain Kenyon
cells. Each row sets the transmitter of the neurons whose label field has
exactly the row's value. The FlyWire row matches `cell_class` `Kenyon_Cell`.
Hemibrain has no class annotations, so its rows list the 14 hemibrain v1.2
Kenyon-cell types, from `KCa'b'-ap1` to `KCg-t`. `neurons` reports the
prediction as `top_nt` and the result as `transmitter`.

- `paths` always reports `sign` (the product over presynaptic neurons),
  `signed_strength`, and `transmitters`. With `--signed`, the search uses only
  presynaptic neurons with sign +1 or -1, so every path has a known sign.
  Source neurons with sign 0 are left out, and a note on stderr gives their
  count.
- `reach --by-side` reports `signed_ipsi`, `signed_contra`, and `signed_net`
  (signed ipsi - signed contra).
- Some types have neurons with different signs. For example, one FlyWire
  il3LN6 neuron is predicted GABA and the other acetylcholine. Signs apply per
  neuron. The `sign_conflict_types` column of `paths` names such types on a
  path. To list their neurons, run `fruitloops neurons --flywire
  --sign-conflicts` or `fruitloops neurons --hemibrain --sign-conflicts`; it
  prints at most `--limit` neurons (default 200). Hemibrain has 330 such
  types.

### Hemibrain predictions

`hemibrain_body_neurotransmitters` comes from the bulk source
`hemibrain:body-neurotransmitters`, the FlyEM file
`https://storage.googleapis.com/hemibrain/v1.2/hemibrain-v1.2-body-mean-neurotransmitters.feather`
(45,591,786 bytes). Setup checks its sha256
(`aab49d858415f559f469a9293adfb4d58e423db83a5debb24272ee4d66e059ad`) and
refuses a file that does not match.

The file has one row for each of the 837,710 hemibrain v1.2 bodies with
predictions. fruitloops reads these columns:

- `body`: the body id.
- `gaba`, `acetylcholine`, `glutamate`, `serotonin`, `octopamine`, `dopamine`,
  `neither`: the per-body mean, as the file name states, of the classifier
  scores at the body's presynaptic sites (T-bars). `neither` is the score for
  none of the six transmitters. The seven values of a row add up to 1.

`top_nt` is the class with the largest mean score, `neither` included. No
confidence threshold applies. An exact tie goes to the class listed first
above; the pinned file has no ties. This rule gives the file's
`predicted_nt` column for every row. The other columns (`type`, `instance`,
`statusLabel`, `predicted_nt`) are not used.

The per-site scores are the hemibrain synapse-level predictions of Eckstein et
al. (2024): `hemibrain-v1.2-tbar-neurotransmitters.feather.bz2`, in the same
bucket, has the same MD5 as that file in their Zenodo record 10593546. FlyEM
has not published the code that computes the per-body means.

Coverage and limits:

- 21,709 of the 21,739 traced bodies (99.86%) have a prediction. The other 30
  get an empty `top_nt` and `transmitter`, and sign 0.
- 125 traced bodies have `neither` as the top class, for example the hemibrain
  DNa02 neuron. Their sign is 0.
- Eckstein et al. report lower accuracy for hemibrain than for FAFB: 78%
  against 87% per synapse, and 91% against 94% per neuron.
- 4,155 cell types are in both datasets, matched by FlyWire `hemibrain_type`.
  For 3,659 of them (88%), the most common hemibrain `top_nt` equals the most
  common FlyWire `top_nt`. Most of the largest differences are optic-lobe and
  central-complex types: for example, LC12 and LC17 are GABA in hemibrain and
  acetylcholine in FlyWire, and LLPC2b and EPG are glutamate in hemibrain and
  acetylcholine in FlyWire.
- The file is for hemibrain v1.2. Hemibrain v1.2.1 has the same connectome.

## Laterality

`reach --by-side` splits seeds by side. With ORN weighting, the side is the
ORN (antenna) side. Without it, the side is the source neuron's soma side.

- ipsi = mean over left target neurons of reach from left seeds, plus mean
  over right target neurons of reach from right seeds.
- contra = the same with the seed sides swapped.
- AI = (ipsi - contra) / (ipsi + contra).

ipsi and contra each add two side means, so they are not on the scale of
`reach`. Seeds whose side is neither left nor right count in `reach` but in
neither side. With ORN weighting, each side needs ORNs of the chosen
glomeruli; a glomerulus with ORNs on one antenna side only stops the command.
AI is empty when a type lacks neurons on one side or has no reach.

Most hemibrain neurons are on the right side: 14,346 traced bodies have the
`_R` instance suffix and 2,497 have `_L`. Thus hemibrain AI is often empty:

- Of the 337 antennal-lobe PNs that `'*_*PN*'` selects, 322 are right, 3 are
  left, and 12 have no side.
- With these PNs as sources, 822 of the 5,554 hemibrain types get an AI at
  hop 2. The other types lack neurons on one side, or have no reach.
- Where AI is present, ipsi and contra compare reach from 3 left seeds with
  reach from 322 right seeds. Thus a hemibrain AI mostly shows the unequal
  seed sets, not a property of the circuit.

## Output Columns

`paths` returns one row per target neuron, route, and rank:

- `route`, `rank`, `hops`, `shortest_hops`, `strength`, `seed_weight`
- target and source: `*_id`, `*_type`, `*_side`; `relation` (`ipsi` or
  `contra` between the source and target soma sides, `unknown` when either
  side is missing). With ORN weighting, `relation` still uses the PN's soma
  side, while `reach --by-side` uses the ORN side.
- `path_types`, `path_ids`, `path_sides`
- `step_synapses`, `step_weights`: per step, separated by ` > `. The first step
  uses the route's synapses.
- `sign`, `signed_strength`, `transmitters`, `sign_conflict_types`

Rank 1 is the strongest path within `--max-hops`. Ranks 2 to `--top` are the
strongest paths that reach the target through a different last presynaptic
neuron. No path passes through the target before its last step: when the
strongest path to a presynaptic neuron does, that neuron's strongest path
that avoids the target is used. A target without a path within `--max-hops`
has no rows. Notes on stderr count such targets and the targets that have no
connections in the graph.

`paths --by-type` groups paths by cell-type sequence. It returns one row per
route, target type, and rank: `target_type`, `rank`, `hops`, `path_types`,
`strength`, `share`, `ipsi_share`, `signed_strength`, `step_synapses`.

- A type route's `strength` is the sum, over every path of its length whose
  neurons have its types, of the product of weights (times the seed weight).
  As in `reach`, paths may revisit neurons, so the strengths of all type routes
  of length k add up to the hop-k reach summed over the target type's neurons.
- `share` = strength / the summed strength of all paths of 1 to `--max-hops`
  hops into the target type's neurons.
- `ipsi_share` = the part of the strength from sources on the target neuron's
  soma side, out of the part whose source and target sides are both known.
- `signed_strength` sums the signed products over the same paths.
- `step_synapses`: per step, the synapses between the neurons that the route's
  paths pass through on their way to a target.
- Untyped neurons are grouped under their bracketed label, such as `[central]`.

The ranking is exact. The search expands a partial route only while the summed
strength of all its completions, known from backward reach, can still beat the
routes already found. Longer `--max-hops` costs more: 4 hops into all 473
descending types take about 10 s.

`reach` returns one row per route, hop, and target type: `target_type`,
`neurons`, `reach`, `rank`, `rank_of`. With `--per-neuron`, it returns one row
per target neuron instead. `--by-side` adds `left_neurons`, `right_neurons`,
`ipsi`, `contra`, `ai`, `signed_ipsi`, `signed_contra`, and `signed_net`.
`--by-route` adds rows for `AL`, `LH`, `MB`, `other`, and `kc`.

## Recipe: Olfactory Routes to DNa02 and DNa03

These commands use FlyWire v783, the default `--min-synapses 5`, and
Orco-weighted PN seeds. The values come from the setup above. Each `paths`
command prints a seed summary on stderr.

No PN has a kept edge onto DNa02 or DNa03. A one-hop search finds no path:

```bash
fruitloops paths --flywire --source-class ALPN --target-type DNa02,DNa03 --max-hops 1 --csv
# stderr: no path within 1 hops for 4 target/route combinations
```

Strongest LH-route path to each DNa02:

```bash
fruitloops paths --flywire --source-class ALPN --target-type DNa02 \
  --orn-family orco --via LH --top 1 --csv
```

| target_side | path_types | strength | step_synapses |
| --- | --- | --- | --- |
| left | DA1_lPN > CB2424 > DNa02 | 1.46066e-05 | 7 > 20 |
| right | DA1_lPN > CB2424 > DNa02 | 2.23176e-05 | 11 > 22 |

Strongest path over all routes:

```bash
fruitloops paths --flywire --source-class ALPN --target-type DNa02,DNa03 \
  --orn-family orco --top 1 --csv
```

| target | path_types | strength | seed_weight |
| --- | --- | --- | --- |
| DNa02 left | M_l2PNl20 > LAL030b > DNa02 | 2.06967e-05 | 0.181063 |
| DNa02 right | DA1_lPN > CB2424 > DNa02 | 2.23176e-05 | 0.43202 |
| DNa03 left | M_l2PNl20 > LAL030b > DNa03 | 2.1391e-05 | 0.181063 |
| DNa03 right | M_l2PNl20 > SIP022 > AOTU019 > DNa03 | 8.57264e-06 | 0.181063 |

Strongest Kenyon-cell route to DNa03:

```bash
fruitloops paths --flywire --source-class ALPN --target-type DNa03 \
  --orn-family orco --via kc --top 1 --csv
```

The right DNa03 path is DL1_adPN > KCapbp-ap1 > MBON31 > DNa03, with strength
1.04604e-06 and sign -1 (MBON31 is GABAergic).

Type routes from the PNs of one glomerulus, here DL5:

```bash
fruitloops paths --flywire --source-type DL5_adPN --target-type DNa02,DNa03 \
  --by-type --max-hops 4 --top 5 --csv
```

The five strongest type routes into DNa03:

| rank | path_types | strength | share | ipsi_share |
| --- | --- | --- | --- | --- |
| 1 | DL5_adPN > KCapbp-ap1 > MBON26 > LAL171,LAL172 > DNa03 | 8.49169e-06 | 0.102265 | 0.407737 |
| 2 | DL5_adPN > KCapbp-ap1 > MBON31 > DNa03 | 7.67215e-06 | 0.092395 | 0.67436 |
| 3 | DL5_adPN > KCapbp-ap1 > MBON26 > LAL051 > DNa03 | 6.78316e-06 | 0.0816888 | 0.339537 |
| 4 | DL5_adPN > KCapbp-ap1 > MBON26 > DNa03 | 4.25752e-06 | 0.0512728 | 0.0443104 |
| 5 | DL5_adPN > CB3185 > CRE011 > LAL112 > DNa03 | 3.06317e-06 | 0.0368894 | 0.606618 |

Without `--by-type`, the same sources give the strongest single-neuron paths.

Hop-2 reach and laterality of DNa02 among the 473 descending types:

```bash
fruitloops reach --flywire --source-class ALPN --target-super-class descending \
  --orn-family orco --hops 2 --by-side --csv | grep -E '^dataset|,DNa02,'
```

DNa02 at hop 2: reach 0.000156449 (mean of the two DNa02 neurons), rank 83 of
473, AI 0.37548, signed net 2.87695e-05.

Hemibrain, with all antennal-lobe PN types as unweighted sources:

```bash
fruitloops paths --hemibrain --source-type '*_*PN*' --target-type DNa02,DNa03 --max-hops 1 --csv
# stderr: no path within 1 hops for 2 target/route combinations
fruitloops paths --hemibrain --source-type '*_*PN*' --target-type DNa02 --max-hops 2 --top 3 --csv
```

The three strongest 2-hop paths to DNa02 go through different relays:

| rank | path_types | strength |
| --- | --- | --- |
| 1 | M_l2PNl20 > SIP022 > DNa02 | 0.000219977 |
| 2 | M_l2PNl20 > LAL030_a > DNa02 | 9.95924e-05 |
| 3 | M_l2PNl20 > SIP023 > DNa02 | 8.83072e-05 |

All three paths have sign 1, because M_l2PNl20 and the three relays are
predicted cholinergic. `--signed` gives the same three paths.

To split the routes by first-synapse region, repeat `--via`, for example
`--via AL --via LH --via MB --via other --via kc`. For reach by route, use
`--by-route`.

## Sources

- Dorkenwald et al. 2024, Nature, doi:10.1038/s41586-024-07558-y (FlyWire
  connectome).
- Schlegel et al. 2024, Nature, doi:10.1038/s41586-024-07686-5 (FlyWire
  whole-brain annotations).
- Eckstein et al. 2024, Cell, doi:10.1016/j.cell.2024.03.016 (transmitter
  predictions), and its supplemental files, doi:10.5281/zenodo.10593546
  (hemibrain synapse-level predictions).
- Scheffer et al. 2020, eLife, doi:10.7554/eLife.57443 (hemibrain).
- Barnstedt et al. 2016, Neuron, doi:10.1016/j.neuron.2016.02.015 (Kenyon
  cells are cholinergic).
- Larsson et al. 2004, Neuron, doi:10.1016/j.neuron.2004.08.019 (Or83b/Orco is
  required for Or function).
- Glomerulus receptor assignments: Couto et al. 2005
  (doi:10.1016/j.cub.2005.07.034), Silbering et al. 2011
  (doi:10.1523/JNEUROSCI.2360-11.2011), Prieto-Godino et al. 2017
  (doi:10.1016/j.neuron.2016.12.024), Kwon et al. 2007
  (doi:10.1073/pnas.0700079104), Task et al. 2022
  (doi:10.7554/eLife.72599), Vulpe et al. 2021
  (doi:10.1016/j.cub.2021.05.025), Benton et al. 2025
  (doi:10.1038/s44319-025-00476-8), Schlegel et al. 2021
  (doi:10.7554/eLife.66018), Marin et al. 2020
  (doi:10.1016/j.cub.2020.06.028), Knecht et al. 2017
  (doi:10.7554/eLife.26654). The packaged table gives the source for each row.
