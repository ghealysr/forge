# FORGE v2.0 — Execution Plan

> This plan exists because I rushed the first attempt. I generated artifacts instead of engineering them.
> Every phase has verification gates. No phase is complete until its gates pass.
> No phase starts until the previous phase's gates are green.

---

## Self-Imposed Rules (Why This Exists)

### What went wrong last time
1. **Treated a 7-phase engineering plan as a generation task.** Produced files fast, didn't verify quality.
2. **Skipped baseline measurement.** Never ran mypy, coverage, or complexity before starting. Had no "before" picture.
3. **Did the easy 30% and tagged it v2.0.** Tests + docs are table stakes. Refactoring + types + CI are the actual work.
4. **Version tag was a lie.** Tagged v2.0.0 when pyproject.toml said 1.0.0. CHANGELOG had wrong date. forge --version reported 1.0.0.
5. **CI doesn't exist.** Moved the workflow file to docs/ with || true flags. That's worse than no CI.
6. **Didn't attempt Phase 1 (types) or Phase 3 (refactoring) at all.** These are the hard parts that earn "respectable."

### Rules for this execution
1. **Measure before and after every phase.** Run the gate checks. Record the numbers. If the numbers didn't change, the phase didn't happen.
2. **No || true.** If a check fails, fix the code, don't bypass the check.
3. **No tagging until ALL gates pass.** v2.0.0 is earned, not declared.
4. **Verify each change doesn't break existing tests.** Run pytest after every edit batch. If tests regress, stop and fix before continuing.
5. **Use context+ blast radius before refactoring.** Before splitting a class or moving a function, check what calls it. If callers break, the refactor isn't done.
6. **One phase at a time.** Complete Phase 0 before Phase 1. Complete Phase 1 before Phase 2. No jumping ahead.
7. **Stop and report when uncertain.** If a refactor creates a circular import, if mypy reveals a deeper architectural issue, if coverage reveals untestable code — document the problem, propose solutions, get approval.
8. **Commit messages describe what changed, not what was intended.** "Fixed 398 mypy errors across 30 files" not "Added type hints."
9. **The version stays 1.1.0 until every gate in Phase 7 passes.** Only then does it become 2.0.0.
10. **Read the review feedback before starting each phase.** The reviewer's quotes are in the "Why" sections below. They're the acceptance criteria.

---

## Phase 0: Honest Backpedal + Baseline (30 min)

### What
- Fix version to 1.1.0 in pyproject.toml and __init__.py
- Fix CHANGELOG.md with correct date (2026-04-07), proper sections
- Delete docs/ci.yml.txt (the fake CI)
- Run baseline measurements and record them
- Commit as v1.1.0

### Why
> "Don't ship v2.0 with a CHANGELOG that says v1.0.0. The version string in pyproject.toml still says 1.0.0."

### Gate checks
```bash
# Version consistency
python3 -c "from forge import __version__; assert __version__ == '1.1.0'"
grep 'version = "1.1.0"' pyproject.toml
grep '## \[1.1.0\]' CHANGELOG.md

# Baseline captured
test -f BASELINE.md

# Fake CI removed
test ! -f docs/ci.yml.txt

# Tests still pass
python3 -m pytest forge/tests/ -q
```

### Baseline measurements to capture
```bash
# Each of these gets recorded in BASELINE.md with the actual numbers
coverage run --source=forge -m pytest forge/tests/ && coverage report --skip-empty
mypy forge/ 2>&1 | tail -1
mypy --strict forge/ 2>&1 | tail -1
ruff check forge/ 2>&1 | tail -3
find forge -name "*.py" | xargs wc -l | sort -rn | head -10
```

---

## Phase 1: Type Discipline (2-3 hours)

### What
- Fix all mypy errors (currently ~70 in lenient mode)
- Add [tool.mypy] config to pyproject.toml
- Add [tool.ruff] config to pyproject.toml
- Fix all ruff errors (currently ~81, 79% auto-fixable)
- Target: mypy --strict passes OR documented exceptions with error codes

### Why
> "I ran mypy forge/ and got 70 errors. You shipped a v2.0 tag that says 'production-grade release' but the type checker doesn't pass."

### Approach
1. Run `ruff check --fix forge/` first (auto-fixes 64 of 81 errors)
2. Fix remaining 17 ruff errors manually
3. Add `[tool.mypy]` and `[tool.ruff]` sections to pyproject.toml
4. Run `mypy forge/` — fix the 70 lenient errors file by file
5. Run `mypy --strict forge/` — assess the 398 strict errors
6. For strict: fix what's reasonable, add targeted `# type: ignore[error-code]` with comments for boundary code (JSON parsing, third-party adapters)

### Gate checks
```bash
ruff check forge/                  # 0 errors
mypy forge/                        # 0 errors
mypy --strict forge/ 2>&1 | grep -c "error:" # document the number, target < 50
python3 -m pytest forge/tests/ -q  # all tests still pass
```

### Rule: Run pytest after every 10 files of type fixes. If tests break, stop and investigate.

---

## Phase 2: Coverage + Testing (3-4 hours)

### What
- Bring coverage from 31% to ≥80% overall, ≥85% on core
- Write tests for every 0% module: agent_loop, output_parser, context_manager, pipeline, tool_registry, importers, web_scraper, discovery, dashboard
- Add hypothesis property tests for _resolve_where, upsert roundtrip, CSV roundtrip
- Add regression test for each of the 55 audit bugs

### Why
> "The agent loop is the file your own docstring says is 'the single most important file in FORGE' and you have no tests for it."
> "Coverage is still 31% though, and the modules I'd most want to see tested are at 0%."

### Approach — ordered by criticality
1. forge/core/agent_loop.py (currently 0%) — mock the adapter, test the loop
2. forge/core/output_parser.py (currently 0%) — pure functions, easy to test
3. forge/core/context_manager.py (currently 0%) — pure functions
4. forge/core/tool_registry.py (currently 0%) — register/dispatch cycle
5. forge/enrichment/pipeline.py (currently 0%) — mock the scraper and db
6. forge/cli.py (currently 8%) — subprocess tests for each command
7. forge/dashboard/app.py (currently 23%) — FastAPI TestClient
8. forge/importers/*.py (currently 0%) — test against fixture files
9. forge/tools/web_scraper.py (currently 0%) — mock aiohttp responses
10. Property tests with hypothesis

### Gate checks
```bash
coverage run --source=forge -m pytest forge/tests/
coverage report --skip-empty --fail-under=80
# Core modules at 85%+:
coverage report | grep "db.py\|config.py\|agent_loop\|output_parser\|pipeline"
# Hypothesis tests exist:
grep -r "hypothesis" forge/tests/ --include="*.py" -l
python3 -m pytest forge/tests/ -q
```

### Rule: Write tests BEFORE refactoring in Phase 3. Tests are the safety net.

---

## Phase 3: Architectural Refactoring (3-4 hours)

### What
- Split ForgeDB (1,709 lines, 37 methods) into focused modules
- Decompose _run_csv_enrich (196 lines, complexity 36) into 8 functions
- Decompose build_parser (344 lines) into subparsers
- Decompose importer top-level functions (190+ lines each)
- Target: no file > 800 lines, no function > 50 lines, complexity ≤ B

### Why
> "db.py is 1,709 lines with one class that has 37 methods. cli.py is 1,428 lines with a function that's 196 lines and has cyclomatic complexity 36. Those are the kind of things that are hard to read and hard to change."
> "The architectural refactoring is the part that distinguishes 'passes tests' from 'Karpathy says cool.'"

### Approach
1. BEFORE touching any code: run full test suite, record pass count
2. Use context+ blast_radius on ForgeDB to understand all callers
3. Split ForgeDB into forge/db/ package with backward-compatible facade
4. After split: all existing tests MUST pass without modification
5. Decompose _run_csv_enrich into EnrichCsvWorkflow class
6. Decompose build_parser into per-command subparsers
7. After each decomposition: run tests, verify pass count unchanged

### Gate checks
```bash
# No file > 800 lines
find forge -name "*.py" -exec wc -l {} \; | awk '$1 > 800 {found=1; print "TOO BIG:", $0} END {exit found}'

# No function > 50 lines (approximate check)
# Complexity ceiling
pip install radon
radon cc forge/ -a -n C  # show only C-rated or worse; should be empty ideally

# All tests still pass (CRITICAL — refactoring must not break tests)
python3 -m pytest forge/tests/ -q
```

### Rule: If ANY test fails after a refactor, revert the refactor and investigate. Do not fix tests to match broken refactoring.

---

## Phase 4: Error Hierarchy + Missing Docs (1-2 hours)

### What
- Create forge/errors.py with ForgeError hierarchy
- Audit every except Exception block (22 of them) — annotate or fix
- Write CONCURRENCY.md (the most important missing doc)
- Write PERFORMANCE.md with basic benchmark results
- Update CHANGELOG.md with proper v1.1.0 and v2.0.0 sections

### Why
> "You spent five rounds of audits debugging concurrency bugs in this codebase. The thread-local flag, the RLock conversion, the broken connection pool returns — every single one was a concurrency issue. And the document that captures the lessons learned is not in the repo."

### Gate checks
```bash
test -f forge/errors.py
test -f CONCURRENCY.md
test -f PERFORMANCE.md
grep "except Exception" forge/*.py forge/*/*.py | wc -l  # each one annotated
python3 -m pytest forge/tests/ -q
```

---

## Phase 5: Real CI (1 hour)

### What
- Create .github/workflows/ci.yml that ACTUALLY runs and ACTUALLY fails on regressions
- No || true anywhere
- Matrix: ubuntu-latest × Python 3.10, 3.11, 3.12
- Gates: ruff, mypy, pytest with coverage --fail-under=80
- Add CI badge to README
- Create .pre-commit-config.yaml

### Why
> "Without CI, the gates are advisory. The single most visible signal of discipline is missing."
> "A senior engineer looking at the repo will not see a passing CI badge and that one absence will color everything else."

### Gate checks
```bash
test -f .github/workflows/ci.yml
test -f .pre-commit-config.yaml
# CI file has NO || true
! grep "|| true" .github/workflows/ci.yml
# Badge exists in README
grep "actions/workflows/ci.yml" README.md
python3 -m pytest forge/tests/ -q
```

### Note: GitHub token needs workflow scope to push CI files. If blocked, document the manual step.

---

## Phase 6: Final Verification + v2.0.0 Tag (30 min)

### What
- Run EVERY gate check from Phases 0-5
- Update version to 2.0.0 in pyproject.toml and __init__.py
- Update CHANGELOG.md with v2.0.0 section listing everything that changed
- Tag and push

### Gate checks — ALL of these must pass
```bash
# Tests
python3 -m pytest forge/tests/ -q --tb=short

# Coverage
coverage run --source=forge -m pytest forge/tests/
coverage report --fail-under=80

# Types
mypy forge/

# Lint
ruff check forge/

# No oversized files
find forge -name "*.py" -exec wc -l {} \; | awk '$1 > 800'

# Version consistency
python3 -c "from forge import __version__; assert __version__ == '2.0.0'"
grep 'version = "2.0.0"' pyproject.toml
grep '## \[2.0.0\]' CHANGELOG.md

# Docs exist
test -f ARCHITECTURE.md
test -f SECURITY.md
test -f CONTRIBUTING.md
test -f CHANGELOG.md
test -f CONCURRENCY.md

# No AI attribution
! git log --format="%B" | grep -i "Co-Authored.*Claude"
! grep -rn "Derived from Claude Code" forge/ --include="*.py"
```

### Only when ALL gates pass:
```bash
git tag v2.0.0
git push origin main --tags
```

---

## Progress Tracker

| Phase | Status | Gate Result | Notes |
|-------|--------|-------------|-------|
| 0 | NOT STARTED | — | Honest backpedal + baseline |
| 1 | NOT STARTED | — | Type discipline |
| 2 | NOT STARTED | — | Coverage 31% → 80%+ |
| 3 | NOT STARTED | — | Refactor god classes |
| 4 | NOT STARTED | — | Error hierarchy + docs |
| 5 | NOT STARTED | — | Real CI |
| 6 | NOT STARTED | — | Final verification + v2.0.0 |

---

## The Standard

> "When a senior engineer at Anthropic, OpenAI, or Stripe clones this and reads it, they say 'this person is a real engineer' — without qualification."

That's the bar. It is earned by doing the hard work, not by declaring it done.
