"""AI activities on Microsoft Agent Framework (plan item P5.2).

All three are provider-neutral (they take a *named* client) and declare the dependency
``ai:<client>``, so a model outage parks work behind a hold exactly like a broker outage would.

* ``AgentPrompt``        – free text in, text out.
* ``ExtractStructured``  – text in, JSON validated against a JSON Schema out (with repair attempts).
* ``Classify``           – text in, one of N labels out; **the label is the outcome**, so it routes
  the flowchart directly (e.g. ``complaint`` → CRM, ``invoice`` → ERP, ``Unknown`` → human).
"""

from __future__ import annotations

import json
from typing import Any

import jsonschema
from agent_framework import Message

from DnA Workflows.ai.clients import ChatClientRegistry
from DnA Workflows.ai.errors import AiOutputError
from DnA Workflows.ai.jsonout import extract_json
from DnA Workflows.runtime.activity import Activity, ActivityContext, ActivityInputs, ActivityResult, Done
from DnA Workflows.runtime.models import NodeSpec


def _text(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, default=str, ensure_ascii=False)


def usage_of(response: Any) -> dict[str, Any]:
    usage = getattr(response, "usage_details", None)
    if not usage:
        return {}
    try:
        return {k: v for k, v in dict(usage).items() if isinstance(v, (int, float, str)) or v is None}
    except (TypeError, ValueError):  # pragma: no cover - exotic usage objects
        return {}


class _AiActivity(Activity):
    category = "AI"

    def __init__(self, clients: ChatClientRegistry) -> None:
        self.clients = clients

    def dependency(self, inputs: Any, node: NodeSpec) -> str | None:
        return f"ai:{self.clients.resolve_name(inputs.client)}"

    async def _ask(self, context: ActivityContext, system: str | None, messages: list[Message],
                   options: dict[str, Any] | None = None) -> Any:
        client = self.clients.get(context.inputs.client)
        chat = ([Message("system", [system])] if system else []) + messages
        kwargs = {"options": options} if options else {}
        response = await client.get_response(chat, **kwargs)
        usage = usage_of(response)
        if usage:
            context.log("ai usage", "debug", usage=usage, client=self.clients.resolve_name(context.inputs.client))
        return response


class AgentPrompt(_AiActivity):
    """Send a prompt to a chat model and return its text answer."""

    type_name = "AgentPrompt"
    output_schema = {"type": "object", "properties": {"text": {"type": "string"}, "usage": {"type": "object"}}}

    class Inputs(ActivityInputs):
        prompt: Any
        instructions: str | None = None
        client: str | None = None
        temperature: float | None = None
        max_tokens: int | None = None

    async def execute(self, context: ActivityContext) -> ActivityResult:
        i = context.inputs
        options = {k: v for k, v in (("temperature", i.temperature), ("max_tokens", i.max_tokens)) if v is not None}
        response = await self._ask(context, i.instructions, [Message("user", [_text(i.prompt)])], options)
        return Done(output={"text": response.text, "usage": usage_of(response)})


class ExtractStructured(_AiActivity):
    """Extract structured data matching a JSON Schema from unstructured input."""

    type_name = "ExtractStructured"

    class Inputs(ActivityInputs):
        input: Any
        json_schema: dict[str, Any]
        instructions: str | None = None
        client: str | None = None
        max_repairs: int = 1

    async def execute(self, context: ActivityContext) -> ActivityResult:
        i = context.inputs
        validator = jsonschema.Draft202012Validator(i.json_schema)
        system = ((i.instructions + "\n\n") if i.instructions else "") + (
            "Extract the requested data. Answer with ONLY a JSON value that validates against this JSON Schema "
            f"(no prose, no code fences):\n{json.dumps(i.json_schema)}")
        messages = [Message("user", [_text(i.input)])]
        problem = ""
        for _ in range(i.max_repairs + 1):
            response = await self._ask(context, system, messages)
            try:
                data = extract_json(response.text)
                errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))
                if not errors:
                    return Done(output=data)
                problem = "; ".join(f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in errors[:5])
            except ValueError as exc:
                problem = str(exc)
            messages += [Message("assistant", [response.text or ""]),
                         Message("user", [f"That answer was invalid: {problem}. Return corrected JSON only."])]
        raise AiOutputError(f"model output did not match the schema after {i.max_repairs + 1} attempt(s): {problem}")


class Classify(_AiActivity):
    """Classify input into one of ``labels``. The chosen label is the outcome (``Unknown`` otherwise)."""

    type_name = "Classify"
    output_schema = {"type": "object", "properties": {"label": {"type": "string"}, "confidence": {"type": "number"}, "reason": {"type": "string"}}}
    outcomes = ("*",)

    class Inputs(ActivityInputs):
        input: Any
        labels: list[str]
        instructions: str | None = None
        client: str | None = None
        #: Below this confidence the outcome is ``Unknown`` (route to a human).
        min_confidence: float = 0.0

    async def execute(self, context: ActivityContext) -> ActivityResult:
        i = context.inputs
        system = ((i.instructions + "\n\n") if i.instructions else "") + (
            f"Classify the user's input into exactly one of these labels: {json.dumps(i.labels)}. "
            'Answer with ONLY JSON: {"label": "<one of the labels or Unknown>", "confidence": <0..1>, '
            '"reason": "<short>"}')
        response = await self._ask(context, system, [Message("user", [_text(i.input)])])
        try:
            answer = extract_json(response.text)
        except ValueError as exc:
            raise AiOutputError(f"classification answer was not JSON: {response.text[:200]!r}") from exc
        label = str(answer.get("label", "Unknown")) if isinstance(answer, dict) else "Unknown"
        confidence = answer.get("confidence") if isinstance(answer, dict) else None
        try:
            confidence = float(confidence) if confidence is not None else None
        except (TypeError, ValueError):
            confidence = None
        if label not in i.labels or (confidence is not None and confidence < i.min_confidence):
            label = "Unknown"
        return Done(outcome=label, output={"label": label, "confidence": confidence,
                                           "reason": answer.get("reason") if isinstance(answer, dict) else None})
