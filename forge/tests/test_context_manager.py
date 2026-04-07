"""Tests for forge.core.context_manager — conversation state and auto-compaction."""

from forge.core.context_manager import ContextManager, Message

# ---------------------------------------------------------------------------
# Message dataclass
# ---------------------------------------------------------------------------


class TestMessage:
    def test_to_dict_basic(self):
        m = Message(role="user", content="Hello")
        d = m.to_dict()
        assert d == {"role": "user", "content": "Hello"}

    def test_to_dict_with_tool_call_id(self):
        m = Message(role="tool", content="result", tool_call_id="call_0")
        d = m.to_dict()
        assert d["tool_call_id"] == "call_0"

    def test_estimated_tokens(self):
        m = Message(role="user", content="a" * 400)
        assert m.estimated_tokens() == 100  # 400 chars / 4

    def test_estimated_tokens_minimum(self):
        m = Message(role="user", content="")
        assert m.estimated_tokens() >= 1


# ---------------------------------------------------------------------------
# set_system_prompt
# ---------------------------------------------------------------------------


class TestSetSystemPrompt:
    def test_stores_prompt(self):
        ctx = ContextManager()
        ctx.set_system_prompt("You are a helpful assistant.")
        assert ctx._system_prompt == "You are a helpful assistant."

    def test_system_prompt_in_messages(self):
        ctx = ContextManager()
        ctx.set_system_prompt("System prompt here.")
        msgs = ctx.get_messages()
        assert msgs[0]["role"] == "system"
        assert msgs[0]["content"] == "System prompt here."


# ---------------------------------------------------------------------------
# add_user_message / add_assistant_message
# ---------------------------------------------------------------------------


class TestAddMessages:
    def test_add_user_message(self):
        ctx = ContextManager()
        ctx.add_user_message("Hello")
        assert ctx.message_count() == 1
        msgs = ctx.get_messages()
        assert msgs[0]["role"] == "user"
        assert msgs[0]["content"] == "Hello"

    def test_add_assistant_message_string(self):
        ctx = ContextManager()
        ctx.add_assistant_message("I can help with that.")
        assert ctx.message_count() == 1
        msgs = ctx.get_messages()
        assert msgs[0]["role"] == "assistant"

    def test_add_assistant_message_dict(self):
        """Ollama-format dict with message.content."""
        ctx = ContextManager()
        ctx.add_assistant_message({"message": {"content": "Response from model."}})
        assert ctx.message_count() == 1
        msgs = ctx.get_messages()
        assert msgs[0]["content"] == "Response from model."

    def test_add_assistant_message_dict_fallback(self):
        """Dict without message key falls back to str()."""
        ctx = ContextManager()
        ctx.add_assistant_message({"something": "else"})
        assert ctx.message_count() == 1

    def test_message_count_increments(self):
        ctx = ContextManager()
        ctx.add_user_message("One")
        ctx.add_assistant_message("Two")
        ctx.add_user_message("Three")
        assert ctx.message_count() == 3


# ---------------------------------------------------------------------------
# add_tool_result
# ---------------------------------------------------------------------------


class TestAddToolResult:
    def test_stores_tool_result(self):
        ctx = ContextManager()
        ctx.add_tool_result("echo", "call_0", '{"echoed": "hello"}')
        assert ctx.message_count() == 1
        msgs = ctx.get_messages()
        assert msgs[0]["role"] == "tool"
        assert msgs[0]["tool_call_id"] == "call_0"

    def test_tool_result_content(self):
        ctx = ContextManager()
        ctx.add_tool_result("db_query", "call_1", "42 rows returned")
        msg = ctx._messages[0]
        assert msg.tool_name == "db_query"
        assert msg.content == "42 rows returned"


# ---------------------------------------------------------------------------
# get_messages
# ---------------------------------------------------------------------------


class TestGetMessages:
    def test_returns_ordered_list(self):
        ctx = ContextManager()
        ctx.set_system_prompt("System")
        ctx.add_user_message("User 1")
        ctx.add_assistant_message("Assistant 1")
        ctx.add_user_message("User 2")

        msgs = ctx.get_messages()
        assert len(msgs) == 4
        assert msgs[0]["role"] == "system"
        assert msgs[1]["role"] == "user"
        assert msgs[2]["role"] == "assistant"
        assert msgs[3]["role"] == "user"

    def test_no_system_prompt(self):
        ctx = ContextManager()
        ctx.add_user_message("Hello")
        msgs = ctx.get_messages()
        assert len(msgs) == 1
        assert msgs[0]["role"] == "user"


# ---------------------------------------------------------------------------
# estimated_tokens
# ---------------------------------------------------------------------------


class TestEstimatedTokens:
    def test_increases_with_messages(self):
        ctx = ContextManager()
        t0 = ctx.estimated_tokens()
        ctx.add_user_message("a" * 100)
        t1 = ctx.estimated_tokens()
        ctx.add_assistant_message("b" * 200)
        t2 = ctx.estimated_tokens()
        assert t2 > t1 > t0

    def test_includes_system_prompt(self):
        ctx = ContextManager()
        ctx.set_system_prompt("x" * 400)
        assert ctx.estimated_tokens() >= 100

    def test_zero_when_empty(self):
        ctx = ContextManager()
        assert ctx.estimated_tokens() == 0


# ---------------------------------------------------------------------------
# needs_compaction
# ---------------------------------------------------------------------------


class TestNeedsCompaction:
    def test_returns_false_when_empty(self):
        ctx = ContextManager(max_tokens=1000)
        assert ctx.needs_compaction() is False

    def test_returns_true_when_threshold_exceeded(self):
        ctx = ContextManager(max_tokens=100, compact_threshold=0.5)
        # 50 tokens threshold; add > 200 chars -> > 50 tokens
        ctx.add_user_message("x" * 300)
        assert ctx.needs_compaction() is True

    def test_returns_false_below_threshold(self):
        ctx = ContextManager(max_tokens=10000, compact_threshold=0.75)
        ctx.add_user_message("short message")
        assert ctx.needs_compaction() is False


# ---------------------------------------------------------------------------
# compact
# ---------------------------------------------------------------------------


class TestCompact:
    def test_compact_reduces_message_count(self):
        ctx = ContextManager(max_tokens=500, preserve_recent=2)
        for i in range(10):
            ctx.add_user_message(f"Message {i}")
        count_before = ctx.message_count()

        ctx.compact()  # No model adapter -> simple summary

        count_after = ctx.message_count()
        assert count_after < count_before

    def test_compact_preserves_recent_messages(self):
        ctx = ContextManager(preserve_recent=3)
        for i in range(10):
            ctx.add_user_message(f"Message {i}")

        ctx.compact()

        msgs = ctx.get_messages()
        # Should have: 1 summary + 3 preserved = 4
        assert ctx.message_count() == 4
        # Last message should be the most recent one
        assert msgs[-1]["content"] == "Message 9"

    def test_compact_creates_summary_message(self):
        ctx = ContextManager(preserve_recent=2)
        for i in range(10):
            ctx.add_user_message(f"Message {i}")

        ctx.compact()

        msgs = ctx.get_messages()
        assert "[CONTEXT SUMMARY" in msgs[0]["content"]

    def test_compact_increments_compaction_count(self):
        ctx = ContextManager(preserve_recent=2)
        for i in range(10):
            ctx.add_user_message(f"Message {i}")

        ctx.compact()
        assert ctx._compaction_count == 1

        # Add more messages and compact again
        for i in range(10):
            ctx.add_user_message(f"New {i}")
        ctx.compact()
        assert ctx._compaction_count == 2

    def test_compact_skips_when_too_few_messages(self):
        ctx = ContextManager(preserve_recent=6)
        ctx.add_user_message("Only one")
        count_before = ctx.message_count()

        ctx.compact()

        assert ctx.message_count() == count_before

    def test_compact_with_model_adapter(self):
        """When a model adapter is provided, uses model summarization."""

        class FakeModel:
            def generate(self, messages, model=None, timeout=None):
                return {"message": {"content": "Summary: stuff happened."}}

        ctx = ContextManager(preserve_recent=2)
        for i in range(10):
            ctx.add_user_message(f"Message {i}")

        ctx.compact(model_adapter=FakeModel())

        msgs = ctx.get_messages()
        assert "Summary: stuff happened." in msgs[0]["content"]

    def test_compact_model_failure_falls_back(self):
        """If model summary fails, falls back to simple summary."""

        class FailModel:
            def generate(self, messages, model=None, timeout=None):
                raise ConnectionError("model down")

        ctx = ContextManager(preserve_recent=2)
        for i in range(10):
            ctx.add_user_message(f"Message {i}")

        ctx.compact(model_adapter=FailModel())

        msgs = ctx.get_messages()
        # Should still have a summary, just the simple one
        assert "[CONTEXT SUMMARY" in msgs[0]["content"]


# ---------------------------------------------------------------------------
# clear
# ---------------------------------------------------------------------------


class TestClear:
    def test_clear_resets_messages(self):
        ctx = ContextManager()
        ctx.set_system_prompt("System")
        ctx.add_user_message("Hello")
        ctx.add_assistant_message("Hi")

        ctx.clear()

        assert ctx.message_count() == 0
        assert ctx._compaction_count == 0

    def test_clear_preserves_system_prompt(self):
        ctx = ContextManager()
        ctx.set_system_prompt("Keep me")
        ctx.add_user_message("Hello")

        ctx.clear()

        msgs = ctx.get_messages()
        assert len(msgs) == 1
        assert msgs[0]["role"] == "system"
        assert msgs[0]["content"] == "Keep me"
