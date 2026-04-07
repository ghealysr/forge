# Changelog

All notable changes to FORGE will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-04-07

### Added
- Test suite expanded: 5 → 131 tests across 7 files (db, config, security, MCP, CLI)
- Security regression tests covering all 55 bugs from audit rounds
- ARCHITECTURE.md — component map, data model, transaction model, design decisions
- SECURITY.md — threat model, hardening measures, audit history
- CONTRIBUTING.md — dev setup, testing, PR expectations
- BASELINE.md — quality metrics snapshot for v2.0 transformation
- Module docstrings on all 38 Python files

### Fixed
- Eliminated all bare `except:` clauses from production code
- Removed AI attribution from source files and git history

## [1.0.0] - 2026-04-06

### Added
- **Dual database backend** -- SQLite (zero-config default) and PostgreSQL (production scale) behind a unified `ForgeDB` interface
- **6-layer email extraction pipeline** -- mailto parsing, regex extraction, Cloudflare email decode, JSON-LD extraction, obfuscation decode, contact page crawling
- **SMTP email verification** -- candidate generation and deliverability checks via RCPT TO
- **Technology detection** -- 30+ technologies across CMS, analytics, frameworks, chat, payments, and email marketing
- **AI enrichment via Ollama/Gemma** -- business summaries, industry classification, health scores, pain point identification, all running locally
- **FCC ULS importer** -- bulk import from FCC Universal Licensing System dump files with phone and name+state matching
- **NPI Registry importer** -- healthcare provider lookup via the free NPPES API
- **SAM.gov importer** -- federal contractor data via the Entity Management API
- **SMTP verifier** -- email deliverability verification without sending mail
- **COALESCE write pattern** -- enrichment never overwrites existing non-null data
- **Checkpoint-based resume** -- crash-safe pipeline that restarts from last completed batch
- **Agent loop** -- core execution engine with context compaction, circuit breaker, and tool dispatch
- **MCP server** -- Model Context Protocol integration exposing 5 tools (discover, enrich, stats, search, export)
- **Dashboard** -- FastAPI + Jinja2 + HTMX web interface for browsing, searching, importing, and exporting data
- **CLI** -- `forge enrich`, `forge dashboard`, `forge discover`, `forge config`, `forge mcp-server`
- **CSV import/export** -- auto-detection of column names via alias mapping
- **Process monitor** -- watchdog that detects stalls, OOM, and crashes with auto-restart
- **Claude Haiku audit agent** -- spot-checks AI output quality at configurable intervals (PostgreSQL only)
- **Rollback engine** -- per-batch change logging for reversal of bad enrichment runs (PostgreSQL only)
- **Multi-source config loading** -- CLI args > env vars > .env file > ~/.forge/config.toml > defaults
- **Field validation** -- email format, URL normalization, score ranges, array deduplication, type coercion

### Security
- 9 rounds of security audits, 55 bugs found and fixed
- Parameterized queries throughout (no string interpolation in SQL)
- `ENRICHABLE_FIELDS` whitelist prevents SQL injection via dynamic column names
- Jinja2 auto-escaping for all dashboard output
- Content Security Policy headers on dashboard
- Dashboard binds to 127.0.0.1 only (localhost)
- File upload size limits and type validation
