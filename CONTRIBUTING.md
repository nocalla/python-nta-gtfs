# Contributing to nta-gtfs

Thanks for your interest in contributing!

## Development setup

The project uses [uv](https://docs.astral.sh/uv/) for dependency management and
hatchling as the build backend. Python 3.12+ is required.

```bash
git clone https://github.com/nocalla/nta-gtfs.git
cd nta-gtfs
uv sync
```

## Before submitting a pull request

Run the full check suite locally — CI enforces all of these:

```bash
uv run pytest                 # tests, with a 90% coverage floor
uv run ruff check .           # lint
uv run ruff format --check .  # formatting
```

Conventions:

- Type hints on every function signature.
- Google-style docstrings on all public and private functions.
- No live network calls in tests — mock all HTTP.
- The library must not import `homeassistant` or create its own
  `aiohttp.ClientSession` (sessions are caller-supplied).

## Release process (maintainers)

Releases are fully automated from a version tag. To publish a new version:

1. Bump `version` in `pyproject.toml`.
2. Add a dated section for the new version to `CHANGELOG.md`.
3. Commit and push to `main`, then tag and push the tag:

   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

Pushing the `v*` tag triggers `.github/workflows/publish.yml`, which:

1. Runs the full CI suite (test matrix + lint) — publishing is blocked if it fails.
2. Builds the sdist and wheel and publishes to PyPI via Trusted Publishing
   (OIDC — no API tokens involved).
3. Creates a GitHub release for the tag with auto-generated notes.

The tag version must match the `pyproject.toml` version — PyPI rejects
re-uploads of an existing version, so a mismatched or reused version number
will fail the publish step.

To dry-run the pipeline against TestPyPI first, trigger the
"Publish to TestPyPI" workflow manually from the Actions tab (or
`gh workflow run publish-testpypi.yml`).
