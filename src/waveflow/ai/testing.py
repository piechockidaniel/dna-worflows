"""A scripted MAF chat client for deterministic, offline tests of AI workflows."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from agent_framework import BaseChatClient, ChatResponse, Content, FunctionInvocationLayer, Message


class ToolCall:
    """Script step: the model calls a tool."""

    def __init__(self, name: str, arguments: dict[str, Any] | None = None, call_id: str | None = None) -> None:
        self.name = name
        self.arguments = arguments or {}
        self.call_id = call_id or f"call-{name}"


class ScriptedChatClient(FunctionInvocationLayer, BaseChatClient):
    """Replies from a script. Steps: ``str`` / ``dict`` (JSON reply), :class:`ToolCall`, an exception
    instance (raised), or a callable ``(messages) -> step``. Received conversations are recorded."""

    def __init__(self, *script: Any, usage: dict[str, int] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.script = list(script)
        self.usage = usage
        self.conversations: list[list[tuple[str, str]]] = []

    def _inner_get_response(self, *, messages: Any, stream: bool, options: Any, **kwargs: Any) -> Any:
        async def respond() -> ChatResponse:
            self.conversations.append([(str(m.role), m.text) for m in messages])
            if not self.script:
                raise AssertionError("ScriptedChatClient ran out of script steps")
            step = self.script.pop(0)
            if isinstance(step, Callable) and not isinstance(step, (ToolCall, type)):  # type: ignore[arg-type]
                step = step(messages)
            if isinstance(step, BaseException):
                raise step
            if isinstance(step, ToolCall):
                content = Content.from_function_call(call_id=step.call_id, name=step.name, arguments=step.arguments)
                return ChatResponse(messages=Message("assistant", [content]))
            text = step if isinstance(step, str) else json.dumps(step)
            return ChatResponse(messages=Message("assistant", [text]), usage_details=self.usage)
        return respond()
