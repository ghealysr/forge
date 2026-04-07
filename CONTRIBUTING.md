# Contributing to FORGE

We welcome contributions. Here is how to get started.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/ghealysr/forge.git
cd forge

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

This installs FORGE plus pytest, ruff, mypy, hypothesis, and coverage.

## Running Tests

```bash
# Run the full test suite
pytest forge/tests/ -v

# Run a specific test file
pytest forge/tests/test_smoke.py -v

# Run with coverage
coverage run -m pytest forge/tests/ -v
coverage report --show-missing
```

All tests use SQLite in-memory or temporary files -- no PostgreSQL server needed to run the test suite.

## Running Lint

```bash
# Check for lint issues
ruff check forge/

# Auto-fix what can be fixed
ruff check forge/ --fix

# Type checking
mypy forge/
```

## Pull Request Expectations

1. **One concern per PR.** A PR that adds a new importer should not also refactor the dashboard. Keep changes focused.
2. **Tests for new behavior.** If you add a feature, add a test. If you fix a bug, add a test that would have caught it.
3. **No broken tests.** `pytest forge/tests/` must pass before submitting.
4. **No new lint errors.** `ruff check forge/` should not introduce new warnings.
5. **Update documentation** if your change affects user-facing behavior (CLI flags, config options, API changes).

## Commit Message Convention

```
<type>: <short description>

<optional body explaining why, not what>
```

Types:
- `feat` -- New feature
- `fix` -- Bug fix
- `refactor` -- Code change that does not add a feature or fix a bug
- `test` -- Adding or updating tests
- `docs` -- Documentation changes
- `perf` -- Performance improvement
- `security` -- Security fix

Examples:
```
feat: add Yelp importer for restaurant discovery
fix: prevent COALESCE from treating empty string as non-null
test: add edge cases for FCC phone normalization
security: escape HTML in dashboard search results
```

## Areas Where Help Is Appreciated

- Additional technology detections (the current list is in `tools/web_scraper.py`)
- New government data source importers (state licensing boards, county records)
- Alternative LLM backend adapters (vLLM, llama.cpp, Mistral)
- Performance optimizations for the web scraping pipeline
- Test coverage expansion
- Documentation and usage examples

## Questions?

Open an issue on GitHub or email [hello@nuclearmarmalade.com](mailto:hello@nuclearmarmalade.com).
