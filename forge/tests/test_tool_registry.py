"""Tests for forge.core.tool_registry — tool definitions and dispatch."""

import pytest

from forge.core.tool_registry import SimpleTool, Tool, ToolDefinition, ToolRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class DummyTool(Tool):
    """Concrete Tool subclass for testing."""

    @property
    def name(self) -> str:
        return "dummy"

    @property
    def description(self) -> str:
        return "A dummy tool for testing."

    @property
    def parameters(self) -> dict:
        return {
            "type": "object",
            "properties": {"x": {"type": "integer"}},
        }

    def execute(self, arguments: dict):
        return {"result": arguments.get("x", 0) * 2}


def _upper_func(args):
    return {"upper": args.get("text", "").upper()}


# ---------------------------------------------------------------------------
# Tests: register / get_tool
# ---------------------------------------------------------------------------

class TestRegister:
    def test_register_adds_tool(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        assert reg.count() == 1

    def test_get_tool_retrieves_by_name(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        tool = reg.get_tool("dummy")
        assert tool is not None
        assert tool.name == "dummy"

    def test_get_tool_returns_none_for_unknown(self):
        reg = ToolRegistry()
        assert reg.get_tool("nonexistent") is None

    def test_register_overwrites_existing(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        reg.register(DummyTool())
        assert reg.count() == 1

    def test_multiple_different_tools(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        reg.register(SimpleTool("echo", "Echo", {}, lambda a: a))
        assert reg.count() == 2


# ---------------------------------------------------------------------------
# Tests: get_tool_definitions
# ---------------------------------------------------------------------------

class TestGetToolDefinitions:
    def test_returns_list_of_dicts(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        defs = reg.get_tool_definitions()
        assert isinstance(defs, list)
        assert len(defs) == 1

    def test_definition_structure(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        d = reg.get_tool_definitions()[0]
        assert d["type"] == "function"
        assert d["function"]["name"] == "dummy"
        assert d["function"]["description"] == "A dummy tool for testing."
        assert "properties" in d["function"]["parameters"]

    def test_empty_registry_returns_empty_list(self):
        reg = ToolRegistry()
        assert reg.get_tool_definitions() == []


# ---------------------------------------------------------------------------
# Tests: list_tools
# ---------------------------------------------------------------------------

class TestListTools:
    def test_returns_names(self):
        reg = ToolRegistry()
        reg.register(DummyTool())
        reg.register(SimpleTool("echo", "Echo", {}, lambda a: a))
        names = reg.list_tools()
        assert "dummy" in names
        assert "echo" in names

    def test_empty_registry(self):
        reg = ToolRegistry()
        assert reg.list_tools() == []


# ---------------------------------------------------------------------------
# Tests: count
# ---------------------------------------------------------------------------

class TestCount:
    def test_count_zero(self):
        reg = ToolRegistry()
        assert reg.count() == 0

    def test_count_after_adds(self):
        reg = ToolRegistry()
        for i in range(5):
            reg.register(SimpleTool(f"tool_{i}", f"Tool {i}", {}, lambda a: a))
        assert reg.count() == 5


# ---------------------------------------------------------------------------
# Tests: register_function
# ---------------------------------------------------------------------------

class TestRegisterFunction:
    def test_creates_simple_tool(self):
        reg = ToolRegistry()
        reg.register_function(
            name="uppercase",
            description="Convert text to uppercase.",
            parameters={"type": "object", "properties": {"text": {"type": "string"}}},
            func=_upper_func,
        )
        assert reg.count() == 1
        tool = reg.get_tool("uppercase")
        assert tool is not None
        assert isinstance(tool, SimpleTool)

    def test_registered_function_is_callable(self):
        reg = ToolRegistry()
        reg.register_function(
            name="uppercase",
            description="Convert text to uppercase.",
            parameters={},
            func=_upper_func,
        )
        tool = reg.get_tool("uppercase")
        result = tool.execute({"text": "hello"})
        assert result == {"upper": "HELLO"}


# ---------------------------------------------------------------------------
# Tests: SimpleTool
# ---------------------------------------------------------------------------

class TestSimpleTool:
    def test_execute_calls_function(self):
        tool = SimpleTool("upper", "Uppercase", {}, _upper_func)
        result = tool.execute({"text": "world"})
        assert result == {"upper": "WORLD"}

    def test_properties(self):
        tool = SimpleTool("myname", "My description", {"type": "object"}, lambda a: None)
        assert tool.name == "myname"
        assert tool.description == "My description"
        assert tool.parameters == {"type": "object"}

    def test_get_definition(self):
        tool = SimpleTool("test", "Test tool", {"type": "object"}, lambda a: None)
        defn = tool.get_definition()
        assert isinstance(defn, ToolDefinition)
        assert defn.name == "test"
        assert defn.description == "Test tool"

    def test_execute_passes_arguments(self):
        received = {}

        def capture(args):
            received.update(args)
            return "ok"

        tool = SimpleTool("cap", "Capture", {}, capture)
        tool.execute({"a": 1, "b": 2})
        assert received == {"a": 1, "b": 2}

    def test_execute_raises_propagates(self):
        def bad(args):
            raise ValueError("nope")

        tool = SimpleTool("bad", "Bad tool", {}, bad)
        with pytest.raises(ValueError, match="nope"):
            tool.execute({})


# ---------------------------------------------------------------------------
# Tests: Tool base class (DummyTool)
# ---------------------------------------------------------------------------

class TestToolBase:
    def test_execute(self):
        tool = DummyTool()
        result = tool.execute({"x": 5})
        assert result == {"result": 10}

    def test_get_definition(self):
        tool = DummyTool()
        defn = tool.get_definition()
        assert defn.name == "dummy"
        assert defn.description == "A dummy tool for testing."
