"""Tests for forge.monitor — monitor utility functions."""



from forge.monitor import (
    load_previous_status,
    save_status,
    tail_log,
    SERVICES,
    SERVICE_PREFIX,
)


# ---------------------------------------------------------------------------
# Tests: save_status / load_previous_status
# ---------------------------------------------------------------------------

class TestStatusPersistence:
    def test_save_and_load_roundtrip(self, tmp_path):
        status_file = str(tmp_path / "test_status.json")
        status = {"timestamp": "2025-01-01", "services": {"a": "ok"}}

        # Patch STATUS_FILE to use temp path
        import forge.monitor as mon
        original = mon.STATUS_FILE
        mon.STATUS_FILE = status_file
        try:
            save_status(status)
            loaded = load_previous_status()
            assert loaded["timestamp"] == "2025-01-01"
            assert loaded["services"]["a"] == "ok"
        finally:
            mon.STATUS_FILE = original

    def test_load_missing_file_returns_empty(self, tmp_path):
        import forge.monitor as mon
        original = mon.STATUS_FILE
        mon.STATUS_FILE = str(tmp_path / "nonexistent.json")
        try:
            result = load_previous_status()
            assert result == {}
        finally:
            mon.STATUS_FILE = original

    def test_load_corrupt_json_returns_empty(self, tmp_path):
        status_file = tmp_path / "bad.json"
        status_file.write_text("{invalid json!!!")

        import forge.monitor as mon
        original = mon.STATUS_FILE
        mon.STATUS_FILE = str(status_file)
        try:
            result = load_previous_status()
            assert result == {}
        finally:
            mon.STATUS_FILE = original


# ---------------------------------------------------------------------------
# Tests: tail_log
# ---------------------------------------------------------------------------

class TestTailLog:
    def test_tail_existing_file(self, tmp_path):
        logfile = tmp_path / "test.log"
        lines = [f"Line {i}" for i in range(20)]
        logfile.write_text("\n".join(lines))

        output = tail_log(str(logfile), lines=5)
        assert "Line 19" in output
        assert "Line 15" in output

    def test_tail_nonexistent_file(self):
        output = tail_log("/tmp/this_file_does_not_exist_at_all.log")
        # On macOS, tail returns empty string for nonexistent files (not an exception)
        # The function returns "(no log)" only on exception
        assert output == "" or output == "(no log)"


# ---------------------------------------------------------------------------
# Tests: SERVICES config
# ---------------------------------------------------------------------------

class TestServicesConfig:
    def test_services_not_empty(self):
        assert len(SERVICES) > 0

    def test_each_service_has_required_keys(self):
        for label, info in SERVICES.items():
            assert "name" in info, f"Missing 'name' for service {label}"
            assert "log" in info, f"Missing 'log' for service {label}"
            assert "plist" in info, f"Missing 'plist' for service {label}"

    def test_service_prefix_used(self):
        for label in SERVICES:
            assert label.startswith(SERVICE_PREFIX)
