"""Security with Keycloak / OIDC (plan phase P10).

DnA Workflows is an OIDC *resource server*: it validates bearer tokens (RS256/ES256 JWTs) issued by
Keycloak – for people (Studio, authorization code + PKCE) and for machines (MCP clients and other
services, **client credentials**). Keycloak realm roles and client roles map to DnA Workflows permissions.

Roles are capabilities, not people (G8): several people hold each role, so no single person is a
bottleneck, and every sensitive hand-over (publish, approve, release) is an explicit permission.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Literal

import httpx
import jwt
from pydantic import BaseModel, Field

from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions

log = logging.getLogger(__name__)

# -- permissions -----------------------------------------------------------------------------------------

PERMISSIONS = (
    "workflows.read", "workflows.design", "workflows.review", "workflows.publish",
    "executions.read", "executions.start", "executions.operate",
    "holds.read", "holds.manage", "proposals.read", "proposals.propose", "proposals.approve",
    "journal.read", "forecasts.read", "messages.send", "ai.triage", "admin",
)

READ = {"workflows.read", "executions.read", "holds.read", "proposals.read", "journal.read", "forecasts.read"}

#: Default Keycloak role → permissions. Override with ``security.roles`` in the shell configuration.
DEFAULT_ROLES: dict[str, set[str]] = {
    "DnA Workflows-viewer": set(READ),
    # the target operator: a domain analyst designs, tests, runs and watches workflows
    "DnA Workflows-analyst": READ | {"workflows.design", "executions.start", "messages.send", "proposals.propose"},
    "DnA Workflows-reviewer": READ | {"workflows.review"},
    "DnA Workflows-publisher": READ | {"workflows.publish"},
    "DnA Workflows-operator": READ | {"executions.operate", "holds.manage", "proposals.propose", "ai.triage"},
    "DnA Workflows-approver": READ | {"proposals.approve"},
    # machine clients (MCP agents, services): read + propose + start, never approve
    "DnA Workflows-agent": READ | {"proposals.propose", "executions.start"},
    "DnA Workflows-admin": set(PERMISSIONS),
}


class Principal(BaseModel):
    subject: str
    name: str
    kind: Literal["user", "service", "api-key", "anonymous"]
    roles: list[str] = Field(default_factory=list)
    permissions: list[str] = Field(default_factory=list)
    client_id: str | None = None

    def can(self, permission: str) -> bool:
        return "admin" in self.permissions or permission in self.permissions


class AuthError(Exception):
    def __init__(self, message: str, status: int = 401) -> None:
        super().__init__(message)
        self.status = status


# -- token validation --------------------------------------------------------------------------------------

class JwksCache:
    """OIDC discovery + JWKS with caching and refresh on unknown ``kid`` (key rotation)."""

    def __init__(self, issuer: str, *, jwks_url: str | None = None, ttl: float = 3600.0,
                 transport: httpx.AsyncBaseTransport | None = None, min_refresh_interval: float = 10.0) -> None:
        self.issuer = issuer.rstrip("/")
        self.jwks_url = jwks_url
        self.ttl = ttl
        self.min_refresh_interval = min_refresh_interval
        self._transport = transport
        self._keys: dict[str, Any] = {}
        self._fetched = 0.0
        self._lock = asyncio.Lock()

    async def key(self, kid: str | None) -> Any:
        if self._stale() or (kid not in self._keys and time.monotonic() - self._fetched > self.min_refresh_interval):
            await self.refresh()
        if kid is None and len(self._keys) == 1:
            return next(iter(self._keys.values()))
        if kid not in self._keys:
            raise AuthError(f"unknown signing key '{kid}'")
        return self._keys[kid]

    def _stale(self) -> bool:
        return not self._keys or time.monotonic() - self._fetched > self.ttl

    async def refresh(self) -> None:
        async with self._lock:
            async with httpx.AsyncClient(transport=self._transport, timeout=10) as client:
                url = self.jwks_url
                if url is None:
                    discovery = (await client.get(f"{self.issuer}/.well-known/openid-configuration")).json()
                    url = discovery["jwks_uri"]
                document = (await client.get(url)).json()
            keys = {}
            for jwk in document.get("keys", []):
                if jwk.get("use", "sig") != "sig":
                    continue
                try:
                    keys[jwk.get("kid")] = jwt.PyJWK(jwk).key
                except jwt.PyJWKError as exc:  # unsupported key types are skipped
                    log.debug("Skipping JWK %s: %s", jwk.get("kid"), exc)
            self._keys = keys
            self._fetched = time.monotonic()


class TokenValidator:
    def __init__(self, jwks: JwksCache, *, audience: str | list[str] | None, client_id: str | None,
                 algorithms: list[str], leeway: float, roles: dict[str, set[str]]) -> None:
        self.jwks = jwks
        self.audience = audience
        self.client_id = client_id
        self.algorithms = algorithms
        self.leeway = leeway
        self.roles = roles

    async def validate(self, token: str) -> Principal:
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as exc:
            raise AuthError(f"malformed token: {exc}") from exc
        key = await self.jwks.key(header.get("kid"))
        try:
            claims = jwt.decode(token, key, algorithms=self.algorithms, audience=self.audience,
                                issuer=self.jwks.issuer, leeway=self.leeway,
                                options={"verify_aud": self.audience is not None, "require": ["exp", "iss", "sub"]})
        except jwt.ExpiredSignatureError as exc:
            raise AuthError("token expired") from exc
        except jwt.PyJWTError as exc:
            raise AuthError(f"invalid token: {exc}") from exc
        return self.principal(claims)

    def principal(self, claims: dict[str, Any]) -> Principal:
        roles = set(claims.get("realm_access", {}).get("roles", []))
        for client, access in (claims.get("resource_access") or {}).items():
            if self.client_id is None or client == self.client_id:
                roles.update(access.get("roles", []))
        permissions = sorted({p for r in roles for p in self.roles.get(r, set())})
        username = claims.get("preferred_username") or claims.get("client_id") or claims["sub"]
        service = username.startswith("service-account-") or "client_id" in claims
        return Principal(subject=claims["sub"], name=username, kind="service" if service else "user",
                         roles=sorted(roles), permissions=permissions, client_id=claims.get("azp"))


class Authenticator:
    """Resolves the caller. Modes: OIDC bearer tokens, a static API key (dev/automation), anonymous."""

    def __init__(self, validator: TokenValidator | None, *, api_keys: dict[str, list[str]] | None = None,
                 anonymous_permissions: list[str] | None = None) -> None:
        self.validator = validator
        self.api_keys = api_keys or {}
        self.anonymous_permissions = anonymous_permissions

    async def authenticate(self, authorization: str | None, api_key: str | None = None,
                           actor: str | None = None) -> Principal:
        if authorization:
            scheme, _, token = authorization.partition(" ")
            if scheme.lower() != "bearer" or not token:
                raise AuthError("expected 'Authorization: Bearer <token>'")
            if self.validator is None:
                raise AuthError("bearer tokens are not enabled")
            return await self.validator.validate(token.strip())
        if api_key:
            if api_key not in self.api_keys:
                raise AuthError("invalid API key")
            return Principal(subject=f"api-key:{api_key[:4]}…", name=actor or "api", kind="api-key",
                             permissions=sorted(self.api_keys[api_key]))
        if self.anonymous_permissions is not None:
            return Principal(subject="anonymous", name=actor or "anonymous", kind="anonymous",
                             permissions=sorted(self.anonymous_permissions))
        raise AuthError("authentication required")


# -- feature ---------------------------------------------------------------------------------------------

class SecurityOptions(FeatureOptions):
    #: Keycloak realm URL, e.g. https://keycloak.example.com/realms/DnA Workflows
    issuer: str | None = None
    audience: str | list[str] | None = "DnA Workflows-api"
    #: Client whose client roles are read from ``resource_access`` (realm roles are always read).
    client_id: str | None = "DnA Workflows-api"
    jwks_url: str | None = None
    algorithms: list[str] = Field(default_factory=lambda: ["RS256", "ES256"])
    leeway: float = 30.0
    #: Role → permissions; merged over the defaults (set a role to [] to disable it).
    roles: dict[str, list[str]] = Field(default_factory=dict)
    #: Static API keys (automation, development) → permissions.
    api_keys: dict[str, list[str]] = Field(default_factory=dict)
    #: Permissions for unauthenticated callers (None = authentication required). Development only.
    anonymous_permissions: list[str] | None = None


class SecurityFeature(Feature):
    """OIDC (Keycloak) bearer tokens, roles → permissions, API keys."""

    name = "security"
    Options = SecurityOptions

    #: Test seam: an httpx transport for discovery/JWKS.
    transport: httpx.AsyncBaseTransport | None = None

    def configure(self, services: ServiceCollection) -> None:
        o: SecurityOptions = self.options  # type: ignore[assignment]
        roles = {**DEFAULT_ROLES, **{k: set(v) for k, v in o.roles.items()}}
        unknown = {p for perms in roles.values() for p in perms} - set(PERMISSIONS)
        if unknown:
            raise ValueError(f"Unknown permissions in security.roles: {sorted(unknown)}")
        validator = None
        if o.issuer:
            jwks = JwksCache(o.issuer, jwks_url=o.jwks_url, transport=type(self).transport)
            validator = TokenValidator(jwks, audience=o.audience, client_id=o.client_id,
                                       algorithms=list(o.algorithms), leeway=o.leeway, roles=roles)
        services.add_instance(Authenticator, Authenticator(
            validator, api_keys={k: list(v) for k, v in o.api_keys.items()},
            anonymous_permissions=o.anonymous_permissions))


def development_authenticator(api_key: str | None) -> Authenticator:
    """Used when the security feature is not composed: full access, optionally behind one API key."""
    if api_key:
        return Authenticator(None, api_keys={api_key: list(PERMISSIONS)})
    return Authenticator(None, anonymous_permissions=list(PERMISSIONS))
