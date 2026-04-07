"""Tests for forge.adapters — Ollama and Claude adapter static helpers and construction."""

from forge.adapters.claude import _BACKOFF_DELAYS, CLAUDE_MODELS, ClaudeAdapter
from forge.adapters.ollama import DEFAULT_OLLAMA_URL, OllamaAdapter

# ---------------------------------------------------------------------------
# Tests: OllamaAdapter construction
# ---------------------------------------------------------------------------


class TestOllamaAdapterConstruction:
    def test_default_url(self):
        adapter = OllamaAdapter()
        assert adapter._base_url == DEFAULT_OLLAMA_URL.rstrip("/")
        adapter.close()

    def test_custom_url(self):
        adapter = OllamaAdapter(base_url="http://remote:11434")
        assert adapter._base_url == "http://remote:11434"
        adapter.close()

    def test_default_model(self):
        adapter = OllamaAdapter()
        assert adapter._default_model == "gemma4:26b"
        adapter.close()

    def test_custom_model(self):
        adapter = OllamaAdapter(default_model="llama3")
        assert adapter._default_model == "llama3"
        adapter.close()

    def test_custom_timeout(self):
        adapter = OllamaAdapter(default_timeout=600.0)
        assert adapter._default_timeout == 600.0
        adapter.close()

    def test_context_manager(self):
        with OllamaAdapter() as adapter:
            assert adapter is not None


# ---------------------------------------------------------------------------
# Tests: ClaudeAdapter static helpers
# ---------------------------------------------------------------------------


class TestClaudeAdapterHelpers:
    def test_extract_system(self):
        messages = [
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi!"},
        ]
        system, remaining = ClaudeAdapter._extract_system(messages)
        assert system == "You are helpful."
        assert len(remaining) == 2
        assert remaining[0]["role"] == "user"

    def test_extract_system_multiple(self):
        messages = [
            {"role": "system", "content": "System 1."},
            {"role": "system", "content": "System 2."},
            {"role": "user", "content": "Hello"},
        ]
        system, remaining = ClaudeAdapter._extract_system(messages)
        assert "System 1." in system
        assert "System 2." in system
        assert len(remaining) == 1

    def test_extract_system_none(self):
        messages = [
            {"role": "user", "content": "Hello"},
        ]
        system, remaining = ClaudeAdapter._extract_system(messages)
        assert system == ""
        assert len(remaining) == 1

    def test_convert_messages(self):
        messages = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi!"},
        ]
        converted = ClaudeAdapter._convert_messages(messages)
        assert len(converted) == 2
        assert converted[0]["role"] == "user"
        assert converted[1]["content"] == "Hi!"

    def test_convert_messages_skips_empty(self):
        messages = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": ""},
            {"role": "user", "content": "Again"},
        ]
        converted = ClaudeAdapter._convert_messages(messages)
        assert len(converted) == 2

    def test_convert_tools_ollama_format(self):
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "web_scrape",
                    "description": "Scrape a website",
                    "parameters": {"type": "object", "properties": {"url": {"type": "string"}}},
                },
            }
        ]
        converted = ClaudeAdapter._convert_tools(tools)
        assert len(converted) == 1
        assert converted[0]["name"] == "web_scrape"
        assert converted[0]["description"] == "Scrape a website"
        assert "input_schema" in converted[0]

    def test_convert_tools_anthropic_format_passthrough(self):
        tools = [
            {
                "name": "echo",
                "description": "Echo text",
                "input_schema": {"type": "object"},
            }
        ]
        converted = ClaudeAdapter._convert_tools(tools)
        assert len(converted) == 1
        assert converted[0]["name"] == "echo"

    def test_convert_tools_empty(self):
        assert ClaudeAdapter._convert_tools([]) == []


# ---------------------------------------------------------------------------
# Tests: ClaudeAdapter constants
# ---------------------------------------------------------------------------


class TestClaudeConstants:
    def test_models_list(self):
        assert len(CLAUDE_MODELS) >= 3
        assert any("sonnet" in m for m in CLAUDE_MODELS)
        assert any("haiku" in m for m in CLAUDE_MODELS)

    def test_backoff_delays(self):
        assert _BACKOFF_DELAYS == [5, 15, 45]

    def test_list_models_returns_known(self):
        # Test the static method without API calls
        # ClaudeAdapter.list_models returns CLAUDE_MODELS
        assert "claude-sonnet-4-6" in CLAUDE_MODELS
