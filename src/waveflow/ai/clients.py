"""Named chat clients on Microsoft Agent Framework (plan item P5.1).

The plugin contract for models is MAF's own ``SupportsChatGetResponse`` (any ``BaseChatClient``):
DnA Workflows adds naming, configuration and lazy provider factories, nothing else. Adding a provider is
contributing a :class:`ChatClientProvider`; registering a ready-made client object is
contributing a :class:`ChatClientRef`.

Example::

    ai:
      default_client: local
      clients:
        gpt:   {provider: openai, model: gpt-4o-mini, api_key: "${OPENAI_API_KEY}"}
        azure: {provider: azure_openai, model: my-deployment, endpoint: "https://x.openai.azure.com",
                api_version: "2024-10-21"}          # no api_key → Azure AD (DefaultAzureCredential)
        local: {provider: ollama, model: llama3.1, endpoint: "http://localhost:11434"}
        vllm:  {provider: openai_compatible, model: qwen2.5, endpoint: "http://vllm:8000/v1", api_key: none}
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field


class ChatClientConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str
    model: str | None = None
    endpoint: str | None = None
    api_key: str | None = None
    api_version: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


@runtime_checkable
class ChatClientProvider(Protocol):
    """Contributor (Extend axis): builds MAF chat clients for one provider name."""

    name: str

    def create(self, config: ChatClientConfig) -> Any: ...


class ChatClientRef(BaseModel):
    """Contributor: a ready-made client registered from code (tests, custom middleware stacks)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: str
    client: Any


class OpenAIProvider:
    name = "openai"

    def create(self, config: ChatClientConfig) -> Any:
        from agent_framework.openai import OpenAIChatClient
        return OpenAIChatClient(config.model, api_key=config.api_key, base_url=config.endpoint, **config.options)


class AzureOpenAIProvider:
    name = "azure_openai"

    def create(self, config: ChatClientConfig) -> Any:
        from agent_framework.openai import OpenAIChatClient
        kwargs: dict[str, Any] = dict(config.options)
        if config.api_key:
            kwargs["api_key"] = config.api_key
        else:  # managed identity / workload identity / az login
            from azure.identity.aio import DefaultAzureCredential
            kwargs["credential"] = DefaultAzureCredential()
        return OpenAIChatClient(config.model, azure_endpoint=config.endpoint, api_version=config.api_version,
                                **kwargs)


class OpenAICompatibleProvider:
    """Any OpenAI-compatible Chat Completions endpoint (vLLM, LM Studio, LiteLLM, Ollama /v1, ...)."""

    name = "openai_compatible"

    def create(self, config: ChatClientConfig) -> Any:
        from agent_framework.openai import OpenAIChatCompletionClient
        return OpenAIChatCompletionClient(config.model, api_key=config.api_key or "none", base_url=config.endpoint,
                                          **config.options)


class OllamaProvider:
    """Local models – for clients whose data must not leave the premises."""

    name = "ollama"

    def create(self, config: ChatClientConfig) -> Any:
        from agent_framework.ollama import OllamaChatClient
        return OllamaChatClient(host=config.endpoint, model=config.model, **config.options)


BUILTIN_PROVIDERS = [OpenAIProvider, AzureOpenAIProvider, OpenAICompatibleProvider, OllamaProvider]


class UnknownChatClient(LookupError):
    pass


class ChatClientRegistry:
    def __init__(self, configs: dict[str, ChatClientConfig], providers: list[ChatClientProvider],
                 refs: list[ChatClientRef], default: str | None = None) -> None:
        self.providers = {p.name: p for p in providers}
        for name, cfg in configs.items():
            if cfg.provider not in self.providers:
                raise ValueError(f"AI client '{name}': unknown provider '{cfg.provider}'. "
                                 f"Known: {sorted(self.providers)}")
        self.configs = configs
        self._clients: dict[str, Any] = {r.name: r.client for r in refs}
        names = [*self._clients, *configs]
        self.default = default or (names[0] if names else None)

    def names(self) -> list[str]:
        return sorted({*self._clients, *self.configs})

    def resolve_name(self, name: str | None) -> str:
        chosen = name or self.default
        if chosen is None or (chosen not in self._clients and chosen not in self.configs):
            raise UnknownChatClient(f"No AI client named '{chosen}'. Known: {self.names()}")
        return chosen

    def get(self, name: str | None = None) -> Any:
        chosen = self.resolve_name(name)
        if chosen not in self._clients:   # built lazily: unused providers need no packages/credentials
            cfg = self.configs[chosen]
            self._clients[chosen] = self.providers[cfg.provider].create(cfg)
        return self._clients[chosen]

    def describe(self) -> list[dict[str, Any]]:
        return [{"name": n, "provider": self.configs[n].provider if n in self.configs else "code",
                 "model": self.configs[n].model if n in self.configs else None, "default": n == self.default}
                for n in self.names()]
