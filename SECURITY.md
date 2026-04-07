# FORGE Security

## Threat Model

FORGE is a **single-developer local tool**. It runs on your machine, accesses your database, scrapes public websites, and queries free government APIs. It is not a SaaS product, not a multi-tenant system, and not intended for deployment on public networks.

The primary threat actor is **untrusted input data** -- CSV files from unknown sources, HTML from arbitrary websites, API responses from government endpoints. A secondary threat actor is anyone with network access to the dashboard if it is accidentally exposed beyond localhost.

FORGE does not handle authentication tokens, payment data, or credentials for third-party services (with the exception of optional API keys stored in `~/.forge/config.toml`).

---

## In-Scope Attack Vectors

### SQL Injection
Business data arrives from CSV imports, web scraping, and government APIs. Any of these could contain crafted payloads targeting the database layer.

**Mitigations:**
- All database queries use parameterized placeholders (`?` for SQLite, `%s` for PostgreSQL). No string interpolation in SQL.
- The `ENRICHABLE_FIELDS` whitelist in `db.py` restricts which column names can appear in dynamic UPDATE statements. Field names from external input are validated against this set before being used in SQL.
- CSV column headers are mapped through `COLUMN_ALIASES` to canonical names -- unknown headers are ignored, not interpolated into queries.

### Path Traversal
CSV file imports accept user-provided file paths. The dashboard accepts file uploads.

**Mitigations:**
- File upload size limits enforced in the dashboard.
- Uploaded files are written to `tempfile` locations, not user-controlled paths.
- CSV import validates file existence and extension before processing.

### Cross-Site Scripting (XSS)
The dashboard renders business data (names, summaries, emails) that originates from web scraping and could contain malicious HTML/JavaScript.

**Mitigations:**
- Jinja2 templates auto-escape all variables by default. No `|safe` filters on user-controlled data.
- Content Security Policy header restricts script sources to `'self'`, the Tailwind CDN, and unpkg (HTMX).
- `X-Content-Type-Options: nosniff` header prevents MIME-type sniffing.

### File Upload Abuse
The dashboard allows CSV file uploads for bulk import.

**Mitigations:**
- Upload size limits enforced at the FastAPI layer.
- Only CSV files are processed; other file types are rejected.
- Uploaded files are parsed with Python's `csv` module, not executed.

---

## Dashboard Warning

The FORGE dashboard binds to `127.0.0.1` (localhost only) and **has no authentication**. It is a local development tool, analogous to Jupyter Notebook or pgAdmin running locally.

**Do not expose the dashboard to a public network.** If you need remote access, use SSH tunneling:

```bash
ssh -L 8765:127.0.0.1:8765 your-server
```

If someone can reach the dashboard, they can read your entire business database, trigger enrichment runs, import/export CSV files, and execute searches. The lack of auth is a deliberate design choice for a single-developer tool -- not an oversight.

---

## Audit History

FORGE has undergone **9 rounds of security audits** during development, covering:

- SQL injection through CSV headers, API responses, and enrichment field names
- Path traversal in file import/export operations
- XSS through scraped business data rendered in the dashboard
- SSRF potential in the web scraping pipeline
- Dependency supply chain review
- Secret handling in configuration files
- Race conditions in the transaction model
- Error message information leakage
- Input validation across all importers

**55 bugs were found and fixed** across these audits. The most common categories were: improper input validation (field names not whitelisted), missing HTML escaping in dashboard templates, and overly permissive file path handling.

---

## Data Storage and PII

FORGE stores its database at `~/.forge/forge.db` (SQLite) or in a PostgreSQL database you configure.

**What constitutes PII in FORGE:**
- Business owner names (`contact_name`)
- Email addresses (`email`, `contact_email`, `all_emails`)
- Phone numbers (`phone`, `contact_phone`)
- Physical addresses (`address_line1`, `city`, `state`, `zip`)
- NPI numbers (healthcare provider identifiers)

This is business contact information sourced from public records, public websites, and government databases. It is not consumer PII in the GDPR sense, but it should be treated with care. If you are operating in the EU or processing EU business data, consult legal counsel regarding GDPR applicability.

FORGE does not transmit data to any external service except:
- **Ollama** (local, on your machine)
- **Anthropic API** (only if you enable the audit agent, and only AI-generated summaries are sent -- not raw business records)
- **Government APIs** (NPI Registry, SAM.gov) -- these are outbound queries, not data uploads
