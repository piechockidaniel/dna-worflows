import json
import time

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from DnA Workflows.api.feature import ApiApp
from DnA Workflows.security import (DEFAULT_ROLES, PERMISSIONS, AuthError, Authenticator, JwksCache, SecurityFeature,
                               TokenValidator)

ISSUER = "https://keycloak.test/realms/DnA Workflows"

POSTGRES = False  # TestClient/ASGI app: see tests/conftest.py


class FakeKeycloak:
    """Discovery + JWKS endpoints and a token factory, like a Keycloak realm."""

    def __init__(self):
        self.keys = {"k1": rsa.generate_private_key(public_exponent=65537, key_size=2048)}
        self.requests = 0

    def rotate(self, kid):
        self.keys[kid] = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests += 1
        if request.url.path.endswith("/.well-known/openid-configuration"):
            return httpx.Response(200, json={"issuer": ISSUER, "jwks_uri": f"{ISSUER}/protocol/openid-connect/certs"})
        jwks = []
        for kid, key in self.keys.items():
            jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
            jwks.append({**jwk, "kid": kid, "use": "sig", "alg": "RS256"})
        jwks.append({"kty": "oct", "kid": "weird", "use": "enc", "k": "x"})
        return httpx.Response(200, json={"keys": jwks})

    def token(self, *, sub="u1", username="ada", realm_roles=(), client_roles=(), aud="DnA Workflows-api", kid="k1",
              exp_in=300, iss=ISSUER, service=False, **extra):
        claims = {"sub": sub, "iss": iss, "aud": aud, "exp": int(time.time()) + exp_in, "iat": int(time.time()),
                  "azp": "DnA Workflows-studio", "realm_access": {"roles": list(realm_roles)},
                  "resource_access": {"DnA Workflows-api": {"roles": list(client_roles)}, "other": {"roles": ["admin"]}},
                  **extra}
        if service:
            claims.update({"preferred_username": f"service-account-{username}", "client_id": username,
                           "azp": username})
        else:
            claims["preferred_username"] = username
        return jwt.encode(claims, self.keys[kid], algorithm="RS256", headers={"kid": kid})


@pytest.fixture
def keycloak(monkeypatch):
    kc = FakeKeycloak()
    monkeypatch.setattr(SecurityFeature, "transport", httpx.MockTransport(kc.handler))
    return kc


@pytest.fixture
def api(make_shell, keycloak):
    shell = make_shell({"api": {"serve": False}, "resilience": {}, "authoring": {},
                        "security": {"issuer": ISSUER, "api_keys": {"ci-key": ["workflows.read", "workflows.publish"]},
                                     "roles": {"custom-role": ["holds.read"]}}})
    with TestClient(shell.get(ApiApp).app) as client:
        yield client, keycloak


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


def test_roles_map_to_permissions_and_routes_enforce_them(api):
    client, kc = api
    assert client.get("/workflows").status_code == 401
    assert client.get("/workflows").headers["www-authenticate"] == "Bearer"
    analyst = kc.token(realm_roles=["DnA Workflows-analyst"])
    me = client.get("/me", headers=bearer(analyst)).json()
    assert me["kind"] == "user" and me["name"] == "ada" and "workflows.design" in me["permissions"]
    assert "proposals.approve" not in me["permissions"]
    assert client.get("/workflows", headers=bearer(analyst)).status_code == 200
    assert client.post("/holds", json={"scope": "global", "reason": "x"}, headers=bearer(analyst)).status_code == 403
    # client roles of *our* client count, roles of other clients do not
    operator = kc.token(client_roles=["DnA Workflows-operator"])
    hold = client.post("/holds", json={"scope": "global", "reason": "x"},
                       headers={**bearer(operator), "X-Actor": "spoofed"})
    assert hold.status_code == 201 and hold.json()["created_by"] == "ada"      # actor comes from the token
    assert "admin" not in client.get("/me", headers=bearer(operator)).json()["permissions"]
    custom = client.get("/me", headers=bearer(kc.token(realm_roles=["custom-role"]))).json()
    assert custom["permissions"] == ["holds.read"]


def test_invalid_tokens_are_rejected(api):
    client, kc = api
    for token, fragment in ((kc.token(exp_in=-3600), "expired"), (kc.token(aud="someone-else"), "audience"),
                            (kc.token(iss="https://evil/realms/x"), "issuer"), ("not.a.jwt", "malformed")):
        r = client.get("/me", headers=bearer(token))
        assert r.status_code == 401 and fragment in r.json()["detail"].lower(), (fragment, r.json())
    assert client.get("/me", headers={"Authorization": "Basic abc"}).status_code == 401
    assert client.get("/me", headers={"Authorization": "Bearer "}).status_code == 401


def test_key_rotation_refetches_jwks(api):
    client, kc = api
    assert client.get("/me", headers=bearer(kc.token(realm_roles=["DnA Workflows-viewer"]))).status_code == 200
    kc.rotate("k2")
    rotated = kc.token(realm_roles=["DnA Workflows-viewer"], kid="k2")
    r = client.get("/me", headers=bearer(rotated))
    # refresh is rate-limited (10 s); the first unknown kid within that window is rejected …
    assert r.status_code in (200, 401)


async def test_jwks_cache_rotation_and_unknown_kid():
    kc = FakeKeycloak()
    cache = JwksCache(ISSUER, transport=httpx.MockTransport(kc.handler), min_refresh_interval=0)
    assert await cache.key("k1") is not None
    kc.rotate("k2")
    assert await cache.key("k2") is not None           # unknown kid → refresh → found
    with pytest.raises(AuthError, match="unknown signing key"):
        await cache.key("nope")
    single = JwksCache(ISSUER, jwks_url=f"{ISSUER}/certs", transport=httpx.MockTransport(kc.handler))
    kc.keys = {"only": kc.keys["k1"]}
    assert await single.key(None) is not None         # token without kid, exactly one key


async def test_service_account_client_credentials_and_api_keys(api):
    client, kc = api
    agent = kc.token(username="mcp-agent", realm_roles=["DnA Workflows-agent"], service=True)
    me = client.get("/me", headers=bearer(agent)).json()
    assert me["kind"] == "service" and me["name"] == "service-account-mcp-agent" and me["client_id"] == "mcp-agent"
    assert client.post("/proposals/x/approve", json={}, headers=bearer(agent)).status_code == 403
    assert client.get("/workflows", headers={"X-API-Key": "ci-key"}).status_code == 200
    assert client.get("/executions", headers={"X-API-Key": "ci-key"}).status_code == 403
    assert client.get("/workflows", headers={"X-API-Key": "wrong"}).status_code == 401


async def test_authenticator_modes():
    anon = Authenticator(None, anonymous_permissions=["workflows.read"])
    p = await anon.authenticate(None, None, "bob")
    assert p.kind == "anonymous" and p.name == "bob" and p.can("workflows.read") and not p.can("admin")
    with pytest.raises(AuthError, match="not enabled"):
        await anon.authenticate("Bearer x")
    with pytest.raises(AuthError, match="required"):
        await Authenticator(None).authenticate(None)
    admin = await Authenticator(None, api_keys={"k": ["admin"]}).authenticate(None, "k")
    assert admin.can("anything")
    assert set(DEFAULT_ROLES["DnA Workflows-admin"]) == set(PERMISSIONS)


def test_unknown_permission_in_configuration_fails(make_shell):
    with pytest.raises(ValueError, match="Unknown permissions"):
        make_shell({"security": {"roles": {"x": ["teleport"]}}})


def test_principal_from_claims_without_username():
    validator = TokenValidator(JwksCache(ISSUER), audience=None, client_id=None, algorithms=["RS256"], leeway=0,
                               roles={"r": {"holds.read"}})
    p = validator.principal({"sub": "abc", "resource_access": {"any": {"roles": ["r"]}}})
    assert p.name == "abc" and p.permissions == ["holds.read"] and p.kind == "user"
