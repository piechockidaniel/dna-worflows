"""Broker-neutral messaging activities (plan item P2.5).

Request/reply needs no special activity: ``SendMessage`` then ``MessageReceived`` with
``correlate: true`` – replies carrying the execution's correlation id resume exactly that execution.
"""

from __future__ import annotations

from typing import Any

from DnA Workflows.messaging.bus import SEND_INTENT, MessageBus, stimulus_keys
from DnA Workflows.runtime.activity import Activity, ActivityContext, ActivityInputs, ActivityResult, Done, Suspend
from DnA Workflows.runtime.bindings import is_static
from DnA Workflows.runtime.models import NodeSpec


class SendMessage(Activity):
    """Send a message to a logical endpoint. Delivered via the outbox after the checkpoint commits,
    so it is sent if and only if the workflow step is durably recorded."""

    type_name = "SendMessage"
    output_schema = {"type": "object", "properties": {"message_id": {"type": "string"}, "endpoint": {"type": "string"}, "type": {"type": "string"}}}
    category = "Messaging"

    class Inputs(ActivityInputs):
        endpoint: str
        type: str
        data: Any = None
        subject: str | None = None
        correlation_id: str | None = None     # defaults to the execution's correlation id
        headers: dict[str, str] = {}

    def __init__(self, bus: MessageBus) -> None:
        self.bus = bus

    def dependency(self, inputs: Any, node: NodeSpec) -> str | None:
        return self.bus.dependency_key(inputs.endpoint)

    async def execute(self, context: ActivityContext) -> ActivityResult:
        i = context.inputs
        envelope = self.bus.envelope(
            i.type, i.data, subject=i.subject, correlation_id=i.correlation_id or context.correlation_id,
            causation_id=context.work_item.id, headers=i.headers, trace=context.trace,
            source=f"DnA Workflows://{context.definition.id}/{context.node.id}")
        context.emit(SEND_INTENT, {"endpoint": i.endpoint, "envelope": envelope.model_dump(mode="json")},
                     dependency=context.dependency)
        return Done(output={"message_id": envelope.id, "endpoint": i.endpoint, "type": i.type})


class MessageReceived(Activity):
    """Trigger / wait on messages from a logical endpoint (optionally a specific message type)."""

    type_name = "MessageReceived"
    output_schema = {"type": "object", "properties": {"id": {"type": "string"}, "type": {"type": "string"}, "source": {"type": "string"}, "subject": {"type": "string"}, "data": {"type": "object"}, "correlation_id": {"type": "string"}, "headers": {"type": "object"}, "endpoint": {"type": "string"}}}
    category = "Messaging"
    is_trigger = True

    class Inputs(ActivityInputs):
        endpoint: str
        type: str | None = None
        #: Mid-flow only: accept just messages carrying this execution's correlation id.
        correlate: bool = False

    async def execute(self, context: ActivityContext) -> ActivityResult:
        if "stimulus" in context.payload:
            return Done(output=context.payload["stimulus"])
        i = context.inputs
        key = stimulus_keys(i.endpoint, i.type)[0]
        return Suspend("message", key, correlation_id=context.correlation_id if i.correlate else None,
                       payload={"endpoint": i.endpoint, "type": i.type})

    @classmethod
    def trigger_keys(cls, node: NodeSpec) -> list[tuple[str, str]]:
        endpoint = node.inputs.get("endpoint")
        message_type = node.inputs.get("type")
        if not isinstance(endpoint, str) or not is_static(message_type):
            return []
        return [("message", stimulus_keys(endpoint, message_type)[0])]
