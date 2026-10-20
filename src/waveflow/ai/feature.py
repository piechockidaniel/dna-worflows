"""AI feature (plan phase P5) on Microsoft Agent Framework.

Example::

    ai:
      default_client: local
      clients:
        local: {provider: ollama, model: llama3.1, endpoint: "http://localhost:11434"}
      triage: {client: local, min_confidence: 0.8}

    resilience:
      release:
        rules:
          - {pattern: "http:*", strategy: ai-recommend, options: {interval: 300, min_confidence: 0.8}}
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from DnA Workflows.ai.activities import AgentPrompt, Classify, ExtractStructured
from DnA Workflows.ai.clients import (BUILTIN_PROVIDERS, ChatClientConfig, ChatClientProvider, ChatClientRef,
                                 ChatClientRegistry)
from DnA Workflows.ai.errors import AiErrorClassifier
from DnA Workflows.ai.operations import AiRecommendRelease, OperationsTools, TriageService
from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.persistence.contracts import ExecutionStore, JournalStore, KeyValueStore
from DnA Workflows.resilience.holds import HoldService
from DnA Workflows.resilience.proposals import ProposalService
from DnA Workflows.resilience.release import ProbeRegistry, ReleaseStrategy
from DnA Workflows.runtime.contracts import FaultClassifier
from DnA Workflows.runtime.feature import add_activity
from DnA Workflows.runtime.registry import WorkflowRegistry


class TriageOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client: str | None = None
    min_confidence: float = 0.7


class AiOptions(FeatureOptions):
    clients: dict[str, ChatClientConfig] = Field(default_factory=dict)
    default_client: str | None = None
    triage: TriageOptions = Field(default_factory=TriageOptions)
    #: Let agents file proposals through the propose_* tools (they can never act directly).
    allow_proposals: bool = True


class AiFeature(Feature):
    """AI activities, operations tools, triage agent and the ``ai-recommend`` release strategy."""

    name = "ai"
    depends_on = ("runtime", "resilience")
    Options = AiOptions

    def configure(self, services: ServiceCollection) -> None:
        o: AiOptions = self.options  # type: ignore[assignment]
        for provider in BUILTIN_PROVIDERS:
            services.add_contributor(ChatClientProvider, provider, key=provider.name)
        services.add(ChatClientRegistry, lambda p: ChatClientRegistry(
            dict(o.clients), p.get_all(ChatClientProvider), p.get_all(ChatClientRef), o.default_client))
        services.add_contributor(FaultClassifier, AiErrorClassifier, key="ai")
        add_activity(services, AgentPrompt, ExtractStructured, Classify)
        services.add(OperationsTools, lambda p: OperationsTools(
            p.get(HoldService), p.get(ExecutionStore), p.get(JournalStore), p.get(ProposalService),
            p.get(ProbeRegistry), p.get(WorkflowRegistry), allow_proposals=o.allow_proposals))
        services.add(TriageService, lambda p: TriageService(
            p.get(HoldService), p.get(OperationsTools), p.get(ProposalService), p.get(ChatClientRegistry),
            p.get(KeyValueStore), p.get(Clock), client=o.triage.client, min_confidence=o.triage.min_confidence))
        services.add_contributor(ReleaseStrategy, lambda p: AiRecommendRelease(p), key="ai-recommend")


def add_chat_client(services: ServiceCollection, name: str, client: object) -> None:
    """Register a ready-made MAF chat client from code."""
    services.add_contributor(ChatClientRef, ChatClientRef(name=name, client=client), key=f"chat-client:{name}")
