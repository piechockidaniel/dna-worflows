"""API feature (plan item P6.2): serves the management API with uvicorn inside the shell."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from DnA Workflows.kernel.container import ServiceCollection, ServiceProvider
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.hosting import HostedService

log = logging.getLogger(__name__)


class ApiOptions(FeatureOptions):
    host: str = "0.0.0.0"
    port: int = 8080
    api_key: str | None = None
    title: str = "DnA Workflows"
    #: Directory with the built Studio (``studio/dist``); served under ``/studio``.
    studio_dir: str | None = None
    #: Runtime configuration handed to the Studio (``/studio/config.json``), e.g.
    #: ``{"keycloak": {"url": "https://sso.example.com", "realm": "DnA Workflows", "clientId": "DnA Workflows-studio"}}``
    studio_config: dict[str, Any] = {}
    #: Allowed browser origins (the Vite dev server during development).
    cors_origins: list[str] = []
    #: Start the HTTP server with the shell (switch off to mount the app in your own ASGI server).
    serve: bool = True


class ApiApp:
    def __init__(self, app: Any) -> None:
        self.app = app


class ApiServer:
    def __init__(self, app: Any, host: str, port: int) -> None:
        import uvicorn
        self.server = uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level="warning", lifespan="off"))
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self.server.serve(), name="DnA Workflows-api")
        log.info("API listening on %s:%s", self.server.config.host, self.server.config.port)

    async def stop(self) -> None:
        if self._task is not None:
            self.server.should_exit = True
            await self._task
            self._task = None


class ApiFeature(Feature):
    """REST management API (FastAPI). ``pip install DnA Workflows[api]``."""

    name = "api"
    depends_on = ("runtime",)
    Options = ApiOptions

    def configure(self, services: ServiceCollection) -> None:
        o: ApiOptions = self.options  # type: ignore[assignment]

        def app(p: ServiceProvider) -> ApiApp:
            from DnA Workflows.api.app import create_app
            app = create_app(p, api_key=o.api_key, title=o.title, studio_dir=o.studio_dir,
                             studio_config=dict(o.studio_config))
            if o.cors_origins:
                from fastapi.middleware.cors import CORSMiddleware
                app.add_middleware(CORSMiddleware, allow_origins=o.cors_origins, allow_credentials=True,
                                   allow_methods=["*"], allow_headers=["*"])
            return ApiApp(app)
        services.add(ApiApp, app)
        if o.serve:
            services.add_contributor(HostedService, lambda p: ApiServer(p.get(ApiApp).app, o.host, o.port), key="api")
