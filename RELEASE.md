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
version=0.1.2

python3 -m unittest discover -s tests
python -m build
python -m twine check dist/*

git add pyproject.toml fruitloops/__init__.py
git commit -m "Bump fruitloops to ${version}"
git push origin HEAD:refs/heads/main

git tag -a "v${version}" -m "fruitloops ${version}"
git push origin "refs/tags/v${version}"

gh run watch --workflow release --exit-status
```

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

The formula must install the generated CSV snapshot from the sdist:

```ruby
(libexec/"share/fruitloops/data").install Dir["data/*"]
```

Do not ship the bulk DuckDB database through Homebrew. Users should build it
locally:

```bash
fruitloops-install-extras
fruitloops bulk setup
fruitloops olfaction build
```

Manual tap fallback:

```bash
version=0.1.2
sha256=$(curl -L --fail --silent \
  "https://github.com/gumadeiras/fruitloops/releases/download/v${version}/fruitloops-${version}.tar.gz" |
  shasum -a 256 | awk '{print $1}')

python ../homebrew-tap/scripts/update_formula.py \
  --formula ../homebrew-tap/Formula/fruitloops.rb \
  --url "https://github.com/gumadeiras/fruitloops/releases/download/v${version}/fruitloops-${version}.tar.gz" \
  --sha256 "$sha256"
```
