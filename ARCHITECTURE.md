# FORGE Architecture

## What Forge Is

FORGE (Free Open-source Runtime for data enrichment) is a self-hosted business data enrichment engine. It takes a database of business records -- names, addresses, phone numbers, websites -- and systematically fills in the blanks: emails extracted from websites, technology stacks detected from page source, SSL status, site speed, industry classification, AI-generated summaries, health scores, and pain points. It does this using free public data sources (FCC licensing database, NPI healthcare registry, SAM.gov federal contractors), open-source AI via Ollama/Gemma, and direct web scraping. No API keys to data brokers, no per-record fees. Everything runs on your machine.

---

## Data Model

### The `businesses` Table

Every record in FORGE lives in a single `businesses` table with ~35 columns. The table is defined in `forge/db.py` in the `BUSINESS_COLUMNS` list, which specifies each column's name, SQLite type, and PostgreSQL type. Core identity fields:

| Column | Purpose |
|--------|---------|
| `id` | UUID primary key (gen_random_uuid on PG, Python uuid4 on SQLite) |
| `name` | Business legal name |
| `dba_name` | "Doing business as" name |
| `phone`, `email`, `website_url` | Primary contact info |
| `city`, `state`, `zip`, `county` | Location |
| `latitude`, `longitude` | Geocoordinates |

Enrichment fields are populated by the pipeline over time:

| Column | Source |
|--------|--------|
| `tech_stack`, `cms_detected` | Web scraping (technology detection) |
| `ssl_valid`, `site_speed_ms` | Web scraping (site audit) |
| `ai_summary`, `health_score`, `pain_points`, `opportunities` | AI enrichment (Ollama/Gemma) |
| `industry`, `sub_industry` | AI classification |
| `npi_number` | NPI Registry importer |
| `email_source` | Tracks which extraction layer found the email |
| `contact_name`, `contact_email`, `contact_phone` | Contact page crawling |
| `all_emails` | JSON array of every email found |

### The COALESCE Pattern

FORGE never overwrites existing data. The write layer uses COALESCE semantics: a field is only updated if the current value is NULL or empty. This is enforced in `write_enrichment()` via SQL like:

```sql
UPDATE businesses SET email = COALESCE(NULLIF(email, ''), ?)
WHERE id = ?
```

If you manually correct a record, the enrichment pipeline will not clobber your work. The `ENRICHABLE_FIELDS` whitelist in `db.py` controls which columns can be written by the pipeline -- this also prevents SQL injection through field names.

---

## Component Map

```
forge/
 |
 +-- config.py              ForgeConfig dataclass, multi-source loading
 +-- db.py                  ForgeDB + _SQLiteBackend + _PostgresBackend + _Transaction
 +-- cli.py                 CLI entry point (argparse)
 +-- mcp_server.py          MCP JSON-RPC server (stdin/stdout)
 +-- monitor.py             Process health watchdog
 |
 +-- core/
 |    +-- agent_loop.py     AgentLoop + AgentConfig + AgentResult
 |    +-- context_manager.py  Sliding context window + compaction
 |    +-- output_parser.py    Extract tool calls from model output
 |    +-- tool_registry.py    Register/lookup tools by name
 |
 +-- adapters/
 |    +-- ollama.py          OllamaAdapter (local Gemma inference)
 |    +-- claude.py          ClaudeAdapter (Anthropic API, audit only)
 |
 +-- tools/
 |    +-- database.py        DB tool (SQL queries as agent actions)
 |    +-- web_scraper.py     Async HTTP fetch + email extraction + tech detection
 |
 +-- enrichment/
 |    +-- pipeline.py        Batch orchestration, checkpoint/resume
 |    +-- prompts.py         AI prompt templates for summary/classification
 |
 +-- importers/
 |    +-- fcc_uls.py         FCC Universal Licensing System bulk import
 |    +-- npi_registry.py    NPI Registry API import (healthcare)
 |    +-- sam_gov.py         SAM.gov Entity API import (gov contractors)
 |    +-- smtp_verifier.py   SMTP RCPT TO email verification
 |
 +-- safety/
 |    +-- audit_agent.py     Claude Haiku spot-check agent (PG only)
 |    +-- error_recovery.py  Retry logic, rollback engine
 |
 +-- dashboard/
 |    +-- app.py             FastAPI + Jinja2 + HTMX web UI
 |    +-- templates/         Server-rendered HTML
 |    +-- static/            CSS, JS assets
 |
 +-- discovery/              Overture Maps business discovery by ZIP
 +-- plugins/                Plugin extension points
 +-- tests/                  Smoke tests + unit tests
```

Data flows downward: `cli.py` or `mcp_server.py` -> `enrichment/pipeline.py` -> `tools/` + `adapters/` -> `db.py`. The `core/agent_loop.py` orchestrates AI enrichment by cycling through model calls and tool executions. Everything converges on `db.py` for persistence.

---

## Two Backends: SQLite vs PostgreSQL

FORGE ships with two database backends behind a single `ForgeDB` interface.

**SQLite** (`_SQLiteBackend`): The zero-config default. Ships with Python, no server to install. Uses a single connection with `check_same_thread=False`, WAL journal mode, and a `threading.RLock` for serialized writes. Ideal for: local enrichment of up to ~500K records, CSV-in/CSV-out workflows, quick experiments, development. Limitations: no audit agent, no rollback engine, no connection pooling.

**PostgreSQL** (`_PostgresBackend`): The production backend. Uses `psycopg2`'s `ThreadedConnectionPool` (default 2-10 connections). Supports the audit agent (Haiku spot-checks), rollback engine (per-batch change logging), and handles millions of records. Required for: large-scale enrichment, multi-user access, production deployments.

Backend selection is automatic: if config has `db_host`, PostgreSQL is used; if it has `db_path`, SQLite is used. Both backends implement the same interface: `connection()`, `write_connection()`, `close()`, `placeholder()`, `now_expr()`, `uuid_default()`, `json_cast()`, `uuid_cast()`.

---

## Transaction Model

`ForgeDB.transaction()` is the single mechanism for multi-statement atomicity.

**How it works:**

```python
with db.transaction() as tx:
    tx.execute("INSERT INTO ...", params)
    tx.execute("UPDATE ...", params)
    rows = tx.fetch_dicts("SELECT ...", params)
# auto-commit on clean exit, auto-rollback on exception
```

**Implementation details:**

- A thread-local `_in_transaction` flag (stored on `threading.local()`) tracks whether the current thread is inside a transaction block.
- **PostgreSQL path**: Acquires a connection from the pool, yields a `_Transaction` wrapper, commits on clean exit, rollbacks on exception, returns the connection to the pool (closing it if broken).
- **SQLite path**: Acquires the `_write_lock` (a `threading.RLock`), uses the single shared connection, commits on clean exit, rollbacks on exception, releases the lock.
- When `_in_transaction` is active and code calls `ForgeDB.execute()` directly (outside the transaction object), the auto-commit is suppressed on SQLite to preserve the outer transaction's atomicity. On PostgreSQL this is not an issue because pool connections are separate.
- The `_Transaction` class wraps a single connection and exposes `execute()`, `fetch_dicts()`, `commit()`, and `rollback()`.

---

## Agent Loop

The agent loop in `core/agent_loop.py` is the execution engine for AI enrichment. Pseudocode:

```
initialize conversation with system_prompt
add user message (the enrichment task)
get tool definitions from tool_registry

while running AND turn_count < max_turns:
    if context needs compaction:
        compact context via model summarization

    send messages + tools to model
    on model error:
        increment consecutive_error_count
        if consecutive_errors >= threshold: BREAK (circuit breaker)
        exponential backoff, continue

    parse response for tool_calls and text
    if stopping condition met (TASK_COMPLETE, TASK_FAILED, NEED_HUMAN):
        BREAK

    if tool_calls found:
        for each tool_call:
            validate against registry
            execute tool with arguments
            record result
            add tool result to conversation
    else:
        add text response to conversation

    call on_turn_complete callback

return AgentResult(status, turns, time, errors, final_output)
```

Key design choices: the context manager implements a sliding window with compaction (model-assisted summarization when context hits 75% capacity); the circuit breaker stops after 5 consecutive errors; each tool call has up to 3 retries; and the agent signals completion via stop sequences in its text output.

---

## MCP Integration

FORGE exposes five tools to external AI assistants via the Model Context Protocol (MCP). The server is implemented in `mcp_server.py` as a JSON-RPC transport over stdin/stdout -- no external MCP SDK required.

**Tools exposed:**

| Tool | Function |
|------|----------|
| `forge_discover` | Search for businesses by ZIP code via Overture Maps |
| `forge_enrich_record` | Add and enrich a single business record |
| `forge_stats` | Get database statistics (total records, emails found, etc.) |
| `forge_search` | Search enriched database by name, state, industry |
| `forge_export` | Export filtered results to CSV |

**How it works:** The MCP server starts when you run `forge mcp-server`. It reads JSON-RPC messages from stdin, dispatches to the appropriate handler, and writes responses to stdout. All logging goes to stderr so it does not corrupt the transport. Configuration goes in `~/.claude.json` under `mcpServers`.

---

## Importer Pipeline

All four importers (`fcc_uls.py`, `npi_registry.py`, `sam_gov.py`, `smtp_verifier.py`) follow the same shape:

1. **Source acquisition** -- Download or query the external data source (FCC bulk files, NPI REST API, SAM.gov API, SMTP protocol).
2. **Parsing** -- Extract business-relevant fields from the source format (pipe-delimited FCC files, JSON API responses, SMTP reply codes).
3. **Normalization** -- Clean phone numbers to 10 digits, normalize business names (strip LLC/Inc/Corp, lowercase), validate email format.
4. **Matching** -- Match external records to existing businesses in the database by phone number (highest confidence) or name + state (medium confidence).
5. **COALESCE write** -- Write matched data using the standard `write_enrichment()` method, which never overwrites existing values.
6. **Checkpoint** -- Save progress to a checkpoint file so the import can resume after interruption.

Each importer can be run standalone via `python -m forge.importers.<name>` with its own CLI arguments, or invoked programmatically from the enrichment pipeline.

---

## Dashboard

The dashboard is a FastAPI + Jinja2 + HTMX web interface defined in `forge/dashboard/app.py`. No React, no npm, no build step -- it is entirely server-rendered with HTMX for interactivity.

**Features:** View enrichment statistics, browse business records with pagination, search and filter, trigger enrichment runs, import CSV files, export results.

**Security model:** The dashboard binds to `127.0.0.1` (localhost only) and has no authentication. It is intended as a local development tool, similar to Jupyter Notebook -- not for deployment on a public network. A Content Security Policy header restricts script sources to self, Tailwind CDN, and unpkg (for HTMX).

---

## Decisions Log

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | **SQLite as default backend** | Zero-config is the highest-leverage feature for adoption. Users should be enriching data within 60 seconds of install, not configuring PostgreSQL. |
| 2 | **COALESCE write pattern everywhere** | Prevents data loss from automated pipelines. Human-corrected data is sacred. This single pattern eliminated an entire class of "the enricher overwrote my fix" bugs. |
| 3 | **No external MCP SDK** | The MCP protocol is simple JSON-RPC over stdio. Adding an SDK dependency for 200 lines of JSON parsing was not justified. Fewer dependencies = fewer supply chain risks. |
| 4 | **Ollama/Gemma for AI enrichment, not cloud APIs** | The whole point of FORGE is zero cost. Sending millions of records through a cloud API defeats the purpose. Local inference is slower but free. |
| 5 | **Claude Haiku only for audit, not enrichment** | The audit agent spot-checks a sample of AI output (every 50 records). This is the one place where cloud API quality justifies the cost -- catching hallucinations before they pollute the dataset. |
| 6 | **Field name whitelist (`ENRICHABLE_FIELDS`)** | SQL injection via dynamic column names is a real risk when field names come from CSV headers or API responses. The whitelist is the primary defense. |
| 7 | **Thread-local `_in_transaction` flag** | SQLite shares one connection across all threads. Without this flag, a `ForgeDB.execute()` call inside a transaction block would auto-commit, breaking atomicity. The flag suppresses auto-commit when a transaction is active. |
| 8 | **Checkpoint-based resume, not WAL replay** | Simple file-based checkpointing (write the last processed ID to disk) is robust and debuggable. WAL replay would be more efficient but adds complexity that is not justified at this scale. |
| 9 | **Dashboard has no auth** | FORGE is a single-developer local tool. Adding auth to a localhost dashboard creates friction without meaningful security benefit. The threat model is explicit: if someone has access to your machine, auth will not save you. |
| 10 | **Dual backend behind one interface** | Users start with SQLite and graduate to PostgreSQL when they need scale. The `ForgeDB` class abstracts the backend so no other module needs to know which database is in use. Migration is a config change, not a code change. |
