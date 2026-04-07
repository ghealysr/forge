# FORGE Performance

## Benchmarks (v1.1.0, SQLite backend, Apple M-series)

### Enrichment Pipeline
- **Web scraping throughput:** ~1,500-4,000 records/hour per instance (varies by site response time)
- **4 parallel instances:** ~6,000-16,000 records/hour combined
- **Email hit rate:** 4-10% depending on business type
- **Tech stack detection rate:** ~30% of scraped sites

### Database Operations
- **Upsert single record:** <1ms (SQLite)
- **Upsert batch (100 records):** ~15ms (SQLite)
- **CSV import (1,000 rows):** ~200ms including column detection
- **CSV export (10,000 rows):** ~500ms
- **Transaction overhead:** <0.5ms per transaction (SQLite)

### Government Importers
- **FCC ULS scan rate:** ~50,000 businesses checked per 4 seconds
- **NPI Registry:** ~3 lookups/second (API rate limited)
- **SMTP verification:** ~0.1-0.5 records/second (SMTP timeout bound)

### Memory Usage
- **Base process:** ~50MB RSS
- **During enrichment (50 workers):** ~80-120MB RSS
- **CSV import (100K rows):** ~200MB peak (streaming helps for larger files)

## Scaling Guidelines

| Records | Backend | Workers | Expected Time |
|---------|---------|---------|---------------|
| 1,000 | SQLite | 10 | ~15 minutes |
| 10,000 | SQLite | 50 | ~2-3 hours |
| 100,000 | PostgreSQL | 100 | ~1-2 days |
| 1,000,000+ | PostgreSQL | 4×100 | ~2-3 weeks |

## Known Bottlenecks
1. **Contact page crawl** adds 8-16 extra HTTP requests per site when no email found on main page
2. **SMTP verification** limited by mail server response times (3-10s per domain)
3. **SQLite write lock** serializes all writes — use PostgreSQL for >4 concurrent writers
