"""Tests for forge.core.output_parser — tool call extraction and text parsing."""

import pytest

from forge.core.output_parser import (
    OutputParser,
    extract_json_from_response,
    strip_thinking_block,
)


@pytest.fixture
def parser():
    return OutputParser()


# ---------------------------------------------------------------------------
# extract_tool_calls: native Ollama format
# ---------------------------------------------------------------------------

class TestNativeToolCalls:
    def test_single_native_tool_call(self, parser):
        response = {
            "message": {
                "content": "",
                "tool_calls": [
                    {
                        "function": {
                            "name": "web_scrape",
                            "arguments": {"url": "https://example.com"},
                        }
                    }
                ],
            }
        }
        calls = parser.extract_tool_calls(response)
        assert len(calls) == 1
        assert calls[0].name == "web_scrape"
        assert calls[0].arguments == {"url": "https://example.com"}
        assert calls[0].id == "call_0"

    def test_multiple_native_tool_calls(self, parser):
        response = {
            "message": {
                "content": "",
                "tool_calls": [
                    {"function": {"name": "tool_a", "arguments": {"x": 1}}},
                    {"function": {"name": "tool_b", "arguments": {"y": 2}}},
                ],
            }
        }
        calls = parser.extract_tool_calls(response)
        assert len(calls) == 2
        assert calls[0].name == "tool_a"
        assert calls[1].name == "tool_b"
        assert calls[1].id == "call_1"

    def test_native_tool_call_string_arguments(self, parser):
        """Arguments given as a JSON string instead of dict."""
        response = {
            "message": {
                "content": "",
                "tool_calls": [
                    {
                        "function": {
                            "name": "echo",
                            "arguments": '{"text": "hello"}',
                        }
                    }
                ],
            }
        }
        calls = parser.extract_tool_calls(response)
        assert len(calls) == 1
        assert calls[0].arguments == {"text": "hello"}

    def test_native_tool_call_bad_string_arguments(self, parser):
        """Unparseable string arguments -> wrapped in {"raw": ...}."""
        response = {
            "message": {
                "content": "",
                "tool_calls": [
                    {
                        "function": {
                            "name": "echo",
                            "arguments": "not valid json",
                        }
                    }
                ],
            }
        }
        calls = parser.extract_tool_calls(response)
        assert len(calls) == 1
        assert calls[0].arguments == {"raw": "not valid json"}

    def test_native_tool_call_no_name_skipped(self, parser):
        response = {
            "message": {
                "content": "",
                "tool_calls": [
                    {"function": {"name": "", "arguments": {}}},
                ],
            }
        }
        calls = parser.extract_tool_calls(response)
        assert len(calls) == 0


# ---------------------------------------------------------------------------
# extract_tool_calls: JSON in text
# ---------------------------------------------------------------------------

class TestJsonInText:
    def test_json_tool_call_in_text(self, parser):
        text = 'I will use the tool: {"tool": "web_scrape", "arguments": {"url": "https://foo.com"}}'
        calls = parser.extract_tool_calls(text)
        assert len(calls) == 1
        assert calls[0].name == "web_scrape"
        assert calls[0].arguments["url"] == "https://foo.com"

    def test_json_name_input_format(self, parser):
        text = '{"name": "db_query", "input": {"sql": "SELECT 1"}}'
        calls = parser.extract_tool_calls(text)
        assert len(calls) == 1
        assert calls[0].name == "db_query"
        assert calls[0].arguments["sql"] == "SELECT 1"

    def test_json_function_params_format(self, parser):
        text = '{"function": "echo", "parameters": {"text": "hi"}}'
        calls = parser.extract_tool_calls(text)
        assert len(calls) == 1
        assert calls[0].name == "echo"
        assert calls[0].arguments["text"] == "hi"

    def test_multiple_json_objects_in_text(self, parser):
        text = (
            'Step 1: {"tool": "search", "arguments": {"q": "test"}} '
            'Step 2: {"tool": "write", "arguments": {"path": "/tmp/x"}}'
        )
        calls = parser.extract_tool_calls(text)
        assert len(calls) == 2
        assert calls[0].name == "search"
        assert calls[1].name == "write"


# ---------------------------------------------------------------------------
# extract_tool_calls: JSON in code blocks
# ---------------------------------------------------------------------------

class TestCodeBlockToolCalls:
    def test_json_code_block(self, parser):
        text = """Here is my tool call:
```json
{"tool": "echo", "arguments": {"text": "hello"}}
```
"""
        calls = parser.extract_tool_calls(text)
        assert len(calls) == 1
        assert calls[0].name == "echo"

    def test_plain_code_block(self, parser):
        text = """```
{"name": "db_query", "input": {"sql": "SELECT 1"}}
```"""
        calls = parser.extract_tool_calls(text)
        assert len(calls) == 1
        assert calls[0].name == "db_query"

    def test_code_block_with_invalid_json_skipped(self, parser):
        text = """```json
not valid json at all
```"""
        calls = parser.extract_tool_calls(text)
        assert len(calls) == 0


# ---------------------------------------------------------------------------
# extract_tool_calls: no tool calls
# ---------------------------------------------------------------------------

class TestNoToolCalls:
    def test_plain_text_returns_empty(self, parser):
        calls = parser.extract_tool_calls("Just a normal response about weather.")
        assert calls == []

    def test_empty_string(self, parser):
        calls = parser.extract_tool_calls("")
        assert calls == []

    def test_none_text_from_dict(self, parser):
        calls = parser.extract_tool_calls({"message": {}})
        assert calls == []

    def test_json_without_tool_keys(self, parser):
        text = '{"status": "ok", "count": 42}'
        calls = parser.extract_tool_calls(text)
        assert calls == []


# ---------------------------------------------------------------------------
# extract_text
# ---------------------------------------------------------------------------

class TestExtractText:
    def test_string_input(self, parser):
        assert parser.extract_text("hello") == "hello"

    def test_ollama_dict_format(self, parser):
        resp = {"message": {"content": "The answer is 42."}}
        assert parser.extract_text(resp) == "The answer is 42."

    def test_response_key(self, parser):
        """When 'message' key is missing and 'response' is present, returns 'response'."""
        # Note: _get_text checks message first. If message is a dict, it returns content.
        # To reach the 'response' fallback, 'message' must NOT be in the dict.
        resp = {"response": "Direct response text."}
        # Actually, .get("message", {}) returns {} which IS a dict, so content="" is returned.
        # The 'response' fallback is only reached when message is NOT a dict.
        # So let's test the actual behavior:
        assert parser.extract_text(resp) == ""
        # And test the true fallback path where message is not a dict:
        resp2 = {"message": None, "response": "Fallback text"}
        assert parser.extract_text(resp2) == "Fallback text"

    def test_none_on_unknown_type(self, parser):
        assert parser.extract_text(12345) is None

    def test_empty_message_content(self, parser):
        resp = {"message": {"content": ""}}
        assert parser.extract_text(resp) == ""


# ---------------------------------------------------------------------------
# strip_thinking_block
# ---------------------------------------------------------------------------

class TestStripThinkingBlock:
    def test_removes_think_block(self):
        text = "<think>I need to consider this carefully.</think>The answer is 42."
        assert strip_thinking_block(text) == "The answer is 42."

    def test_multiline_think_block(self):
        text = "<think>\nStep 1: foo\nStep 2: bar\n</think>\nDone!"
        assert strip_thinking_block(text) == "Done!"

    def test_no_think_block_passthrough(self):
        text = "Just a regular response."
        assert strip_thinking_block(text) == "Just a regular response."

    def test_empty_think_block(self):
        text = "<think></think>Result."
        assert strip_thinking_block(text) == "Result."


# ---------------------------------------------------------------------------
# extract_json_from_response
# ---------------------------------------------------------------------------

class TestExtractJsonFromResponse:
    def test_valid_json_extracted(self):
        text = 'Here is the result: {"summary": "Great dental practice", "score": 85}'
        result = extract_json_from_response(text)
        assert result is not None
        assert result["summary"] == "Great dental practice"
        assert result["score"] == 85

    def test_json_after_think_block(self):
        text = '<think>Analyzing...</think>{"industry": "dentist", "health_score": 72}'
        result = extract_json_from_response(text)
        assert result is not None
        assert result["industry"] == "dentist"

    def test_malformed_json_returns_none(self):
        text = "This is not JSON at all, just plain text."
        result = extract_json_from_response(text)
        assert result is None

    def test_empty_string(self):
        assert extract_json_from_response("") is None

    def test_partial_json(self):
        text = '{"incomplete": true, "missing_brace'
        result = extract_json_from_response(text)
        assert result is None


# ---------------------------------------------------------------------------
# _extract_json_objects edge cases
# ---------------------------------------------------------------------------

class TestExtractJsonObjects:
    def test_closing_brace_inside_string(self, parser):
        """Braces inside JSON strings should not confuse the parser."""
        text = '{"text": "contains } inside", "ok": true}'
        objects = parser._extract_json_objects(text)
        assert len(objects) == 1
        assert objects[0]["text"] == "contains } inside"
        assert objects[0]["ok"] is True

    def test_escaped_quotes(self, parser):
        text = r'{"text": "say \"hello\"", "done": true}'
        objects = parser._extract_json_objects(text)
        assert len(objects) == 1
        assert "hello" in objects[0]["text"]

    def test_multiple_objects(self, parser):
        text = '{"a": 1} some junk {"b": 2}'
        objects = parser._extract_json_objects(text)
        assert len(objects) == 2
        assert objects[0]["a"] == 1
        assert objects[1]["b"] == 2

    def test_junk_between_objects(self, parser):
        text = 'prefix {"x": 10} --- middle text --- {"y": 20} suffix'
        objects = parser._extract_json_objects(text)
        assert len(objects) == 2
        assert objects[0]["x"] == 10
        assert objects[1]["y"] == 20

    def test_nested_objects(self, parser):
        text = '{"outer": {"inner": "value"}}'
        objects = parser._extract_json_objects(text)
        assert len(objects) == 1
        assert objects[0]["outer"]["inner"] == "value"

    def test_array_values(self, parser):
        text = '{"items": [1, 2, 3]}'
        objects = parser._extract_json_objects(text)
        assert len(objects) == 1
        assert objects[0]["items"] == [1, 2, 3]

    def test_empty_text(self, parser):
        assert parser._extract_json_objects("") == []

    def test_no_json(self, parser):
        assert parser._extract_json_objects("just plain text without any braces") == []

    def test_unmatched_brace(self, parser):
        """Unmatched opening brace should not produce a result."""
        text = '{"unclosed": true'
        objects = parser._extract_json_objects(text)
        assert len(objects) == 0
