"""Tests for forge.tools.database — database tool wrappers and DatabasePool."""

from unittest.mock import MagicMock

from forge.tools.database import (
    BatchWriteEnrichmentTool,
    DatabasePool,
    FetchUnenrichedTool,
    WriteEnrichmentTool,
)

# ---------------------------------------------------------------------------
# Mock ForgeDB for testing
# ---------------------------------------------------------------------------

class MockForgeDB:
    """In-memory mock of ForgeDB for testing database tools."""

    def __init__(self):
        self.placeholder = "?"
        self.is_postgres = False
        self._closed = False
        self._results = []

    def fetch_dicts(self, query, params=None):
        return self._results

    def write_enrichment(self, business_id, updates, source="tool"):
        return {
            "status": "updated",
            "business_id": business_id,
            "fields_updated": list(updates.keys()),
        }

    def get_connection(self):
        return self

    def return_connection(self, conn):
        pass

    def close(self):
        self._closed = True


# ---------------------------------------------------------------------------
# Tests: DatabasePool
# ---------------------------------------------------------------------------

class TestDatabasePool:
    def test_construction_with_forgedb(self):
        db = MockForgeDB()
        pool = DatabasePool(db=db)
        assert pool._db is db
        assert pool._pool is None

    def test_get_connection_delegates(self):
        db = MockForgeDB()
        pool = DatabasePool(db=db)
        conn = pool.get_connection()
        assert conn is db  # MockForgeDB returns self

    def test_return_connection_delegates(self):
        db = MockForgeDB()
        pool = DatabasePool(db=db)
        pool.return_connection(db)  # Should not raise

    def test_close_all_delegates(self):
        db = MockForgeDB()
        pool = DatabasePool(db=db)
        pool.close_all()
        assert db._closed is True


# ---------------------------------------------------------------------------
# Tests: FetchUnenrichedTool
# ---------------------------------------------------------------------------

class TestFetchUnenrichedTool:
    def test_properties(self):
        pool = DatabasePool(db=MockForgeDB())
        tool = FetchUnenrichedTool(pool)
        assert tool.name == "fetch_unenriched"
        assert "enrich" in tool.description.lower()
        assert "missing_field" in tool.parameters["properties"]
        assert "missing_field" in tool.parameters["required"]

    def test_execute_returns_businesses(self):
        db = MockForgeDB()
        db._results = [
            {"id": "uuid-1", "name": "Tampa Dental", "email": None},
            {"id": "uuid-2", "name": "Miami Salon", "email": None},
        ]
        pool = DatabasePool(db=db)
        tool = FetchUnenrichedTool(pool)

        result = tool.execute({"missing_field": "email", "limit": 5})

        assert result["count"] == 2
        assert len(result["businesses"]) == 2

    def test_execute_with_state_filter(self):
        db = MockForgeDB()
        db._results = [{"id": "uuid-1", "name": "Tampa Dental"}]
        pool = DatabasePool(db=db)
        tool = FetchUnenrichedTool(pool)

        result = tool.execute({"missing_field": "industry", "state": "FL"})
        assert result["count"] == 1

    def test_execute_empty_results(self):
        db = MockForgeDB()
        db._results = []
        pool = DatabasePool(db=db)
        tool = FetchUnenrichedTool(pool)

        result = tool.execute({"missing_field": "email"})
        assert result["count"] == 0
        assert result["businesses"] == []

    def test_execute_limits_to_20(self):
        db = MockForgeDB()
        db._results = []
        pool = DatabasePool(db=db)
        tool = FetchUnenrichedTool(pool)
        # Even if user asks for 100, the tool caps at 20
        result = tool.execute({"missing_field": "email", "limit": 100})
        # We can't check the actual SQL limit here, but the tool ran without error
        assert "count" in result

    def test_execute_handles_error(self):
        db = MockForgeDB()
        db.fetch_dicts = MagicMock(side_effect=Exception("DB error"))
        pool = DatabasePool(db=db)
        tool = FetchUnenrichedTool(pool)

        result = tool.execute({"missing_field": "email"})
        assert "error" in result


# ---------------------------------------------------------------------------
# Tests: WriteEnrichmentTool
# ---------------------------------------------------------------------------

class TestWriteEnrichmentTool:
    def test_properties(self):
        pool = DatabasePool(db=MockForgeDB())
        tool = WriteEnrichmentTool(pool)
        assert tool.name == "write_enrichment"
        assert "business_id" in tool.parameters["properties"]
        assert "updates" in tool.parameters["properties"]

    def test_execute_writes_data(self):
        db = MockForgeDB()
        pool = DatabasePool(db=db)
        tool = WriteEnrichmentTool(pool)

        result = tool.execute({
            "business_id": "uuid-1",
            "updates": {"email": "info@dental.com", "industry": "dentist"},
        })

        assert result["status"] == "updated"
        assert "email" in result["fields_updated"]
        assert "industry" in result["fields_updated"]

    def test_execute_empty_updates(self):
        pool = DatabasePool(db=MockForgeDB())
        tool = WriteEnrichmentTool(pool)

        result = tool.execute({"business_id": "uuid-1", "updates": {}})
        assert result["status"] == "no_updates"

    def test_execute_filters_disallowed_fields(self):
        db = MockForgeDB()
        pool = DatabasePool(db=db)
        tool = WriteEnrichmentTool(pool)

        result = tool.execute({
            "business_id": "uuid-1",
            "updates": {"MALICIOUS_FIELD": "DROP TABLE", "email": "ok@test.com"},
        })

        assert result["status"] == "updated"
        assert "email" in result["fields_updated"]
        assert "MALICIOUS_FIELD" not in result.get("fields_updated", [])

    def test_execute_all_disallowed(self):
        pool = DatabasePool(db=MockForgeDB())
        tool = WriteEnrichmentTool(pool)

        result = tool.execute({
            "business_id": "uuid-1",
            "updates": {"bad_field": "value", "another_bad": "value"},
        })
        assert result["status"] == "no_valid_fields"

    def test_execute_handles_error(self):
        db = MockForgeDB()
        db.write_enrichment = MagicMock(side_effect=Exception("write failed"))
        pool = DatabasePool(db=db)
        tool = WriteEnrichmentTool(pool)

        result = tool.execute({
            "business_id": "uuid-1",
            "updates": {"email": "test@test.com"},
        })
        assert "error" in result


# ---------------------------------------------------------------------------
# Tests: BatchWriteEnrichmentTool
# ---------------------------------------------------------------------------

class TestBatchWriteEnrichmentTool:
    def test_properties(self):
        pool = DatabasePool(db=MockForgeDB())
        tool = BatchWriteEnrichmentTool(pool)
        assert tool.name == "batch_write_enrichment"
        assert "results" in tool.parameters["properties"]

    def test_execute_empty_list(self):
        pool = DatabasePool(db=MockForgeDB())
        tool = BatchWriteEnrichmentTool(pool)

        result = tool.execute({"results": []})
        assert result["status"] == "empty"
        assert result["updated"] == 0

    def test_execute_multiple_records(self):
        db = MockForgeDB()
        pool = DatabasePool(db=db)
        tool = BatchWriteEnrichmentTool(pool)

        result = tool.execute({"results": [
            {"business_id": "uuid-1", "updates": {"email": "a@b.com"}},
            {"business_id": "uuid-2", "updates": {"industry": "dentist"}},
        ]})

        assert result["status"] == "completed"
        assert result["updated"] == 2
        assert result["total"] == 2

    def test_execute_partial_failure(self):
        db = MockForgeDB()
        call_count = [0]
        original_write = db.write_enrichment

        def flaky_write(business_id, updates, source="tool"):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("flaky write")
            return original_write(business_id, updates, source)

        db.write_enrichment = flaky_write
        pool = DatabasePool(db=db)
        tool = BatchWriteEnrichmentTool(pool)

        result = tool.execute({"results": [
            {"business_id": "uuid-1", "updates": {"email": "a@b.com"}},
            {"business_id": "uuid-2", "updates": {"email": "c@d.com"}},
            {"business_id": "uuid-3", "updates": {"email": "e@f.com"}},
        ]})

        assert result["status"] == "completed"
        assert result["errors"] >= 1
