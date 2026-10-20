import logging
import os
import tempfile
import uuid

import pytest

from DnA Workflows.testing import build_test_shell

# plugins whose optional modules are not built yet / not installed log noisy warnings at discovery
logging.getLogger("DnA Workflows.kernel.features").setLevel(logging.ERROR)


_RUNTIME = os.path.join(tempfile.gettempdir(), f"DnA Workflows-runtime-{os.getuid()}")
os.makedirs(_RUNTIME, mode=0o700, exist_ok=True)
os.environ.setdefault("XDG_RUNTIME_DIR", _RUNTIME)   # pgserver/platformdirs warning otherwise

BACKENDS = ["memory", "sqlite"]
try:  # a real PostgreSQL 16 from pip (pgserver); skipped when unavailable
    import pgserver  # noqa: F401
    import psycopg  # noqa: F401
    BACKENDS.append("postgres")
except ImportError:  # pragma: no cover
    pass


@pytest.fixture(scope="session")
def postgres_uri(tmp_path_factory):
    import pgserver
    server = pgserver.get_server(str(tmp_path_factory.mktemp("pgdata")), cleanup_mode="stop")
    yield server.get_uri()
    server.cleanup()


@pytest.fixture(params=BACKENDS)
def backend(request, tmp_path):
    if request.param == "memory":
        return {}
    if request.param == "sqlite":
        return {"persistence.sqlite": {"path": str(tmp_path / "DnA Workflows.db")}}
    if not getattr(request.module, "POSTGRES", True):
        pytest.skip("module drives the app through TestClient (own event loop); async pool is loop-bound")
    uri = request.getfixturevalue("postgres_uri")
    schema = f"t_{uuid.uuid4().hex[:12]}"    # every test gets its own schema (like its own database)
    return {"persistence.postgres": {"dsn": uri, "schema_name": schema, "max_pool_size": 4}}


@pytest.fixture
async def make_shell(backend):
    """Build a deterministic test shell on every persistence backend."""
    shells = []

    def factory(features=None, **kwargs):
        merged = {**backend, **(features or {})}
        shell = build_test_shell(merged, **kwargs)
        shells.append(shell)
        return shell

    yield factory
    for shell in shells:
        if "persistence.sqlite" in backend:
            from DnA Workflows.persistence.sqlite import SqliteDatabase
            db = shell.provider.get_optional(SqliteDatabase)
            if db is not None:
                db.close()
        if "persistence.postgres" in backend:
            from DnA Workflows.persistence.postgres import PostgresDatabase
            pg = shell.provider.get_optional(PostgresDatabase)
            if pg is not None:
                await pg.close()
