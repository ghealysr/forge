"""Tests for forge.core.agent_loop — the core execution engine."""

from unittest.mock import MagicMock, patch

from forge.core.agent_loop import AgentConfig, AgentLoop, AgentResult
from forge.core.tool_registry import SimpleTool, ToolRegistry

# ---------------------------------------------------------------------------
# Mock adapter
# ---------------------------------------------------------------------------


class MockAdapter:
    """Minimal adapter that yields pre-canned responses."""

    def __init__(self, responses):
        self._responses = iter(responses)

    def generate(self, messages, model=None, tools=None, timeout=None, temperature=0.3):
        return next(self._responses)

    def generate_simple(self, prompt, think=False):
        return next(self._responses)

    def is_healthy(self):
        return True

    def list_models(self):
        return ["mock-model"]

    def close(self):
        pass


class FailAdapter:
    """Adapter that always raises."""

    def generate(self, **kwargs):
        raise ConnectionError("model offline")

    def generate_simple(self, prompt, think=False):
        raise ConnectionError("model offline")

    def is_healthy(self):
        return False

    def list_models(self):
        return []

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_registry(*tools):
    reg = ToolRegistry()
    for t in tools:
        reg.register(t)
    return reg


def _echo_tool():
    return SimpleTool(
        name="echo",
        description="Echo input back.",
        parameters={"type": "object", "properties": {"text": {"type": "string"}}},
        func=lambda args: {"echoed": args.get("text", "")},
    )


def _failing_tool():
    return SimpleTool(
        name="fail_always",
        description="Always raises.",
        parameters={"type": "object", "properties": {}},
        func=lambda args: (_ for _ in ()).throw(RuntimeError("boom")),
    )


# ---------------------------------------------------------------------------
# Tests: construction
# ---------------------------------------------------------------------------


class TestAgentConfig:
    def test_defaults(self):
        cfg = AgentConfig()
        assert cfg.model == "gemma4"
        assert cfg.max_turns == 200
        assert cfg.max_consecutive_errors == 5
        assert "TASK_COMPLETE" in cfg.stop_sequences

    def test_custom_values(self):
        cfg = AgentConfig(model="llama3", max_turns=10, context_window=4096)
        assert cfg.model == "llama3"
        assert cfg.max_turns == 10
        assert cfg.context_window == 4096


class TestAgentLoopConstruction:
    def test_basic_construction(self):
        adapter = MockAdapter([])
        registry = ToolRegistry()
        cfg = AgentConfig()
        loop = AgentLoop(adapter, registry, cfg)
        assert loop._running is False
        assert loop._turn_count == 0


# ---------------------------------------------------------------------------
# Tests: run() — text only, no tool calls
# ---------------------------------------------------------------------------


class TestRunTextOnly:
    def test_simple_text_response_completes(self):
        """Model returns plain text (no tool call) -> loop stops after 1 turn."""
        adapter = MockAdapter(["Hello! Here is your answer."])
        registry = _make_registry()
        cfg = AgentConfig(max_turns=5)
        loop = AgentLoop(adapter, registry, cfg)

        result = loop.run("Say hello")

        assert isinstance(result, AgentResult)
        assert result.turns_used == 1
        assert result.tool_calls_made == 0
        assert result.final_output == "Hello! Here is your answer."
        assert result.total_time > 0

    def test_task_complete_in_text(self):
        """Model says TASK_COMPLETE -> loop stops, status is 'completed'."""
        adapter = MockAdapter(["Done. TASK_COMPLETE"])
        registry = _make_registry()
        cfg = AgentConfig(max_turns=10)
        loop = AgentLoop(adapter, registry, cfg)

        result = loop.run("Do the thing")

        assert result.status == "completed"
        assert "TASK_COMPLETE" in result.final_output

    def test_task_failed_in_text(self):
        MockAdapter(["Cannot proceed. TASK_FAILED"])
        loop = AgentLoop(
            MockAdapter(["Cannot proceed. TASK_FAILED"]), _make_registry(), AgentConfig()
        )
        result = loop.run("Impossible task")
        assert result.status == "failed"

    def test_need_human_in_text(self):
        adapter = MockAdapter(["I need help. NEED_HUMAN"])
        loop = AgentLoop(adapter, _make_registry(), AgentConfig())
        result = loop.run("Do something ambiguous")
        assert result.status == "needs_human"


# ---------------------------------------------------------------------------
# Tests: run() — with tool calls
# ---------------------------------------------------------------------------


class TestRunWithTools:
    def test_tool_call_dispatched(self):
        """Model returns a tool call -> tool is executed -> result fed back."""
        tool_call_response = {
            "message": {
                "content": "",
                "tool_calls": [
                    {
                        "function": {
                            "name": "echo",
                            "arguments": {"text": "hello"},
                        }
                    }
                ],
            }
        }
        # First response has a tool call; second is final text.
        adapter = MockAdapter([tool_call_response, "All done. TASK_COMPLETE"])
        registry = _make_registry(_echo_tool())
        cfg = AgentConfig(max_turns=10)
        loop = AgentLoop(adapter, registry, cfg)

        result = loop.run("Echo hello")

        assert result.tool_calls_made >= 1
        assert result.status == "completed"

    def test_unknown_tool_returns_error(self):
        """Tool call for a non-existent tool returns an error result."""
        tool_call_response = {
            "message": {
                "content": "",
                "tool_calls": [
                    {"function": {"name": "nonexistent", "arguments": {}}},
                ],
            }
        }
        adapter = MockAdapter([tool_call_response, "TASK_COMPLETE"])
        registry = _make_registry()
        cfg = AgentConfig(max_turns=5)
        loop = AgentLoop(adapter, registry, cfg)

        result = loop.run("Call something")

        assert result.tool_calls_made >= 1
        # The loop should still proceed despite the unknown tool error.
        assert result.status == "completed"

    def test_tool_error_retries(self):
        """A tool that raises gets retried up to max_retries_per_tool times."""
        tool_call_response = {
            "message": {
                "content": "",
                "tool_calls": [
                    {"function": {"name": "fail_always", "arguments": {}}},
                ],
            }
        }
        adapter = MockAdapter([tool_call_response, "TASK_COMPLETE"])
        registry = _make_registry(_failing_tool())
        cfg = AgentConfig(max_turns=5, max_retries_per_tool=2)
        loop = AgentLoop(adapter, registry, cfg)

        result = loop.run("Try the failing tool")
        # Tool should have been attempted (1 initial + 2 retries = 3 attempts)
        assert len(result.errors) >= 1


# ---------------------------------------------------------------------------
# Tests: stop()
# ---------------------------------------------------------------------------


class TestStop:
    def test_stop_sets_running_false(self):
        adapter = MockAdapter([])
        loop = AgentLoop(adapter, _make_registry(), AgentConfig())
        loop._running = True
        loop.stop()
        assert loop._running is False


# ---------------------------------------------------------------------------
# Tests: _should_stop
# ---------------------------------------------------------------------------


class TestShouldStop:
    def test_detects_task_complete(self):
        loop = AgentLoop(MockAdapter([]), _make_registry(), AgentConfig())
        assert loop._should_stop("The job is done TASK_COMPLETE.", []) is True

    def test_detects_task_failed(self):
        loop = AgentLoop(MockAdapter([]), _make_registry(), AgentConfig())
        assert loop._should_stop("TASK_FAILED: out of memory", []) is True

    def test_detects_need_human(self):
        loop = AgentLoop(MockAdapter([]), _make_registry(), AgentConfig())
        assert loop._should_stop("I'm stuck. NEED_HUMAN please.", []) is True

    def test_no_stop_on_regular_text(self):
        loop = AgentLoop(MockAdapter([]), _make_registry(), AgentConfig())
        assert loop._should_stop("Just a normal reply.", []) is False

    def test_none_text(self):
        loop = AgentLoop(MockAdapter([]), _make_registry(), AgentConfig())
        assert loop._should_stop(None, []) is False


# ---------------------------------------------------------------------------
# Tests: _determine_status
# ---------------------------------------------------------------------------


class TestDetermineStatus:
    def _loop(self, **overrides):
        loop = AgentLoop(MockAdapter([]), _make_registry(), AgentConfig(**overrides))
        return loop

    def test_stopped_status(self):
        loop = self._loop(max_turns=100)
        loop._running = False
        loop._turn_count = 5
        assert loop._determine_status("something") == "stopped"

    def test_max_turns_status(self):
        loop = self._loop(max_turns=10)
        loop._running = True
        loop._turn_count = 10
        assert loop._determine_status("something") == "max_turns"

    def test_error_circuit_breaker_status(self):
        loop = self._loop(max_consecutive_errors=3)
        loop._running = True
        loop._turn_count = 5
        loop._consecutive_errors = 3
        assert loop._determine_status(None) == "error_circuit_breaker"

    def test_completed_from_task_complete(self):
        loop = self._loop()
        loop._running = True
        loop._turn_count = 3
        assert loop._determine_status("TASK_COMPLETE") == "completed"

    def test_failed_from_task_failed(self):
        loop = self._loop()
        loop._running = True
        loop._turn_count = 3
        assert loop._determine_status("TASK_FAILED") == "failed"

    def test_needs_human_from_need_human(self):
        loop = self._loop()
        loop._running = True
        loop._turn_count = 3
        assert loop._determine_status("NEED_HUMAN") == "needs_human"

    def test_no_stop_signal(self):
        loop = self._loop()
        loop._running = True
        loop._turn_count = 3
        assert loop._determine_status("Some output without a signal") == "no_stop_signal"

    def test_none_output_completed(self):
        loop = self._loop()
        loop._running = True
        loop._turn_count = 3
        assert loop._determine_status(None) == "completed"


# ---------------------------------------------------------------------------
# Tests: max turns limit
# ---------------------------------------------------------------------------


class TestMaxTurns:
    @patch("time.sleep")  # Don't actually sleep
    def test_max_turns_respected(self, mock_sleep):
        """Agent stops when max_turns is reached."""
        # Return empty content each turn to keep loop going without stopping
        responses = ["" for _ in range(50)]
        adapter = MockAdapter(responses)
        cfg = AgentConfig(max_turns=3)
        loop = AgentLoop(adapter, _make_registry(), cfg)

        result = loop.run("Keep going forever")

        assert result.turns_used == 3
        assert result.status == "max_turns"


# ---------------------------------------------------------------------------
# Tests: consecutive error circuit breaker
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    @patch("time.sleep")  # Don't actually sleep during backoff
    def test_circuit_breaker_triggers(self, mock_sleep):
        """After max_consecutive_errors model failures, the loop stops."""
        adapter = FailAdapter()
        cfg = AgentConfig(max_turns=100, max_consecutive_errors=3)
        loop = AgentLoop(adapter, _make_registry(), cfg)

        result = loop.run("This will fail repeatedly")

        assert result.status == "error_circuit_breaker"
        assert len(result.errors) >= 3


# ---------------------------------------------------------------------------
# Tests: context compaction triggers
# ---------------------------------------------------------------------------


class TestContextCompaction:
    def test_compaction_triggers_when_needed(self):
        """When context exceeds compact_threshold, compact() is called."""
        # Use a tiny context window so compaction triggers fast
        cfg = AgentConfig(context_window=100, compact_threshold=0.1, max_turns=3)
        adapter = MockAdapter(["Short reply TASK_COMPLETE"])
        loop = AgentLoop(adapter, _make_registry(), cfg)

        # Manually add a large message to trigger compaction
        loop._context.add_user_message("x" * 500)
        assert loop._context.needs_compaction() is True

        result = loop.run("Trigger compaction")
        # The run should complete without error even with compaction
        assert result.turns_used >= 1


# ---------------------------------------------------------------------------
# Tests: on_turn_complete callback
# ---------------------------------------------------------------------------


class TestCallback:
    def test_callback_invoked(self):
        callback = MagicMock()
        adapter = MockAdapter(["Done."])
        cfg = AgentConfig(max_turns=5)
        # Text-only reply won't trigger callback (callback is only for tool-call turns).
        # Use a tool-call response to trigger it.
        tool_resp = {
            "message": {
                "content": "",
                "tool_calls": [{"function": {"name": "echo", "arguments": {"text": "hi"}}}],
            }
        }
        adapter = MockAdapter([tool_resp, "Done."])
        loop = AgentLoop(adapter, _make_registry(_echo_tool()), cfg, on_turn_complete=callback)
        loop.run("Test callback")
        callback.assert_called()

    def test_callback_error_does_not_kill_loop(self):
        def bad_callback(**kwargs):
            raise ValueError("callback crash")

        tool_resp = {
            "message": {
                "content": "",
                "tool_calls": [{"function": {"name": "echo", "arguments": {"text": "hi"}}}],
            }
        }
        adapter = MockAdapter([tool_resp, "TASK_COMPLETE"])
        cfg = AgentConfig(max_turns=5)
        loop = AgentLoop(adapter, _make_registry(_echo_tool()), cfg, on_turn_complete=bad_callback)
        result = loop.run("Test bad callback")
        assert result.status == "completed"
