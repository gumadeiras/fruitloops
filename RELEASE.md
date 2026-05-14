# Release

Fruitloops releases are tag-driven. A `v*` tag runs `.github/workflows/release.yml`,
which builds distributions, publishes the GitHub release assets, publishes to
PyPI, and updates `gumadeiras/homebrew-tap`.

## Prerequisites

- `pyproject.toml` and `fruitloops/__init__.py` have the same version.
- `data/manifest.csv` exists if the release should include the generated CSV
  snapshot in the sdist.
- GitHub Actions has PyPI trusted publishing configured for environment `pypi`.
- GitHub Actions has `HOMEBREW_TAP_TOKEN`, a token that can push to
  `gumadeiras/homebrew-tap`.

## Cut A Release

```bash
version=X.Y.Z

./scripts/release check "$version"
./scripts/release run "$version"
```

Use `run` only after explicit release approval. The wrapper updates version
files, runs the local preflight, commits, tags, pushes `main`, pushes `vX.Y.Z`,
and waits for release CI.

## What The Workflow Does

- Builds the wheel and sdist.
- Runs tests and `twine check`.
- Publishes GitHub release assets.
- Publishes to PyPI.
- Computes the sdist sha256 from the built artifact.
- Checks out `gumadeiras/homebrew-tap`.
- Runs `homebrew-tap/scripts/update_formula.py`.
- Commits and pushes `Formula/fruitloops.rb` if it changed.

## Changelog Rules

- Every release must update `CHANGELOG.md` before the release tag is created.
- `CHANGELOG.md` must always keep an `Unreleased` section at the top for future entries.
- New user-facing changes should be added to `Unreleased` as they land.
- Use user-facing language whenever possible. Describe what changed for people using fruitloops, not repository maintenance.
- Use these sections when they apply: `Features`, `Fixes`, and `Changes`.
- Omit empty sections.
- Do not include release chores unless the change affects how users install or use fruitloops.

## Homebrew Formula Requirements

The formula should install the single runtime environment declared in
`pyproject.toml`; there is no separate extras install step.

The package build copies the generated CSV snapshot into `fruitloops/data`, so
the formula should not need a separate data install step.

Do not ship the bulk DuckDB database through Homebrew. Users should build it
locally:

```bash
fruitloops setup
```

Manual tap fallback:

```bash
version=X.Y.Z
sha256=$(curl -L --fail --silent \
  "https://github.com/gumadeiras/fruitloops/releases/download/v${version}/fruitloops-${version}.tar.gz" |
  shasum -a 256 | awk '{print $1}')

python ../homebrew-tap/scripts/update_formula.py \
  --formula ../homebrew-tap/Formula/fruitloops.rb \
  --url "https://github.com/gumadeiras/fruitloops/releases/download/v${version}/fruitloops-${version}.tar.gz" \
  --sha256 "$sha256"
```
