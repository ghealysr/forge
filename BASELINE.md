# FORGE Baseline Measurements

> Captured: 2026-04-07
> Purpose: "Before" picture for the v2.0 quality transformation

## Tests
- **131 passing** in 0.98s across 7 test files

## Coverage
- **Overall: 31%** (6,185 statements, 4,262 missed)
- Core modules at 0%: agent_loop, output_parser, context_manager, tool_registry, pipeline, web_scraper, all importers
- Best coverage: db.py (59%), config.py (74%), mcp_server.py (53%)

## Type Checking
- **mypy (lenient): 74 errors in 16 files**
- **mypy --strict: 402 errors in 30 files**

## Linting
- **ruff: 81 errors** (64 auto-fixable)
- 50 unused imports, 11 unused variables, 8 multi-imports, 5 type comparisons

## File Sizes
- forge/db.py: **1,709 lines** (ceiling: 800)
- forge/cli.py: **1,428 lines** (ceiling: 800)
- forge/dashboard/app.py: **796 lines** (at ceiling)
- forge/mcp_server.py: **757 lines** (at ceiling)
- 6 files above 600 lines

## Function Sizes
- **55 functions exceed 50-line ceiling**
- Worst: build_parser (345 lines), _run_csv_enrich (197), import_sam_gov (190)
- 10 functions exceed 130 lines

## Targets
| Metric | Baseline | Target |
|--------|----------|--------|
| Tests | 131 | 250+ |
| Coverage | 31% | ≥80% |
| mypy lenient | 74 errors | 0 |
| mypy strict | 402 errors | <50 |
| ruff | 81 errors | 0 |
| Files >800 lines | 2 | 0 |
| Functions >50 lines | 55 | 0 |
