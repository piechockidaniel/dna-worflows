"""Conformance suite every persistence provider must pass (P1.11)."""

from datetime import UTC, datetime, timedelta

import pytest

from DnA Workflows.persistence.contracts import (ConcurrencyConflict, DefinitionStore, DuplicateDelivery,
                                            ExecutionStore, JournalStore, KeyValueStore, OutboxStore)
from DnA Workflows.runtime.models import (Bookmark, Checkpoint, ExecutionStatus, IntentStatus, JournalEvent,
                                     OutboxIntent, WorkflowDefinition, WorkflowExecution, WorkItem,
                                     WorkState)

T0 = datetime(2030, 1, 1, tzinfo=UTC)


@pytest.fixture
def stores(make_shell):
    shell = make_shell()
    return {c: shell.get(c) for c in (ExecutionStore, OutboxStore, JournalStore, DefinitionStore, KeyValueStore)}


def execution(ex_id="e1", **kw) -> WorkflowExecution:
    return WorkflowExecution(id=ex_id, definition_id=kw.pop("definition_id", "d"), definition_version=1,
                             created_at=kw.pop("created_at", T0), **kw)


def event(kind, **kw) -> JournalEvent:
    return JournalEvent(id=f"ev-{kind}-{kw.get('execution_id')}", kind=kind, timestamp=T0, **kw)


async def test_commit_is_fenced_and_atomic(stores):
    s: ExecutionStore = stores[ExecutionStore]
    ex = execution()
    assert await s.commit(Checkpoint(execution=ex, expected_revision=0)) == 1
    assert ex.revision == 1
    stale = execution()
    with pytest.raises(ConcurrencyConflict):
        await s.commit(Checkpoint(execution=stale, expected_revision=0,
                                  intents=[OutboxIntent(id="i-x", kind="k")], journal=[event("x")]))
    # nothing of the failed checkpoint leaked
    assert await stores[OutboxStore].get("i-x") is None
    assert await stores[JournalStore].query(kinds=["x"]) == []

    loaded = await s.get("e1")
    loaded.variables["a"] = 1
    assert await s.commit(Checkpoint(execution=loaded, expected_revision=1)) == 2
    assert (await s.get("e1")).variables == {"a": 1}
    assert await s.get("missing") is None


async def test_inbox_keys_deduplicate_atomically(stores):
    s: ExecutionStore = stores[ExecutionStore]
    await s.commit(Checkpoint(execution=execution("a"), expected_revision=0, inbox_keys=["m1"]))
    with pytest.raises(DuplicateDelivery):
        await s.commit(Checkpoint(execution=execution("b"), expected_revision=0, inbox_keys=["m1"],
                                  journal=[event("dup", execution_id="b")]))
    assert await s.get("b") is None
    assert await stores[JournalStore].query(execution_id="b") == []


async def test_listing_due_bookmarks_parked_and_counts(stores):
    s: ExecutionStore = stores[ExecutionStore]
    waiting = execution("w", status=ExecutionStatus.WAITING, correlation_id="c1", bookmarks=[
        Bookmark(id="b1", node_id="n", stimulus_type="message", key="orders|*"),
        Bookmark(id="b2", node_id="n", stimulus_type="message", key="orders|paid", correlation_id="c1")])
    due = execution("d", created_at=T0 + timedelta(seconds=1), work=[
        WorkItem(id="w1", node_id="n", created_at=T0)])
    later = execution("l", created_at=T0 + timedelta(seconds=2), work=[
        WorkItem(id="w2", node_id="n", state=WorkState.RETRYING, not_before=T0 + timedelta(minutes=5))])
    parked = execution("p", status=ExecutionStatus.PARKED, definition_id="other", work=[
        WorkItem(id="w3", node_id="n", state=WorkState.PARKED, dependency="http:x", hold_id="h1"),
        WorkItem(id="w4", node_id="n", state=WorkState.PARKED, dependency="http:y")])
    done = execution("c", status=ExecutionStatus.COMPLETED, bookmarks=[
        Bookmark(id="b3", node_id="n", stimulus_type="message", key="orders|*")])
    for ex in (waiting, due, later, parked, done):
        await s.commit(Checkpoint(execution=ex, expected_revision=0))

    assert await s.due(T0) == ["d"]
    assert await s.due(T0 + timedelta(minutes=10)) == ["d", "l"]

    found = await s.find_bookmarks("message", ["orders|paid", "orders|*"], "c1")
    assert sorted(b.id for _, b in found) == ["b1", "b2"]
    found = await s.find_bookmarks("message", ["orders|paid", "orders|*"], "other")
    assert [b.id for _, b in found] == ["b1"]
    assert await s.find_bookmarks("message", [], None) == []
    assert await s.find_bookmarks("event", ["orders|*"], None) == []

    assert sorted(await s.find_parked()) == [("p", "w3"), ("p", "w4")]
    assert await s.find_parked(dependency="http:x") == [("p", "w3")]
    assert await s.find_parked(hold_id="h1", dependency="http:x") == [("p", "w3")]

    assert [x.id for x in await s.list()] == ["c", "p", "w", "d", "l"]
    assert [x.id for x in await s.list(status=ExecutionStatus.PARKED)] == ["p"]
    assert [x.id for x in await s.list(definition_id="other")] == ["p"]
    assert [x.id for x in await s.list(correlation_id="c1")] == ["w"]
    assert len(await s.list(limit=2)) == 2 and len(await s.list(limit=10, offset=4)) == 1
    counts = await s.count_by_status()
    assert counts["running"] == 2 and counts["parked"] == 1 and counts["completed"] == 1


async def test_outbox_store(stores):
    s: ExecutionStore = stores[ExecutionStore]
    o: OutboxStore = stores[OutboxStore]
    intents = [
        OutboxIntent(id="i1", kind="k", created_at=T0, next_attempt_at=T0, dependency="rabbitmq:a"),
        OutboxIntent(id="i2", kind="k", created_at=T0, next_attempt_at=T0 + timedelta(minutes=1)),
    ]
    await s.commit(Checkpoint(execution=execution(), expected_revision=0, intents=intents))
    assert [i.id for i in await o.due(T0)] == ["i1"]
    assert [i.id for i in await o.due(T0 + timedelta(minutes=2))] == ["i1", "i2"]
    i1 = await o.get("i1")
    i1.status = IntentStatus.HELD
    i1.hold_id = "h"
    await o.update(i1)
    assert [i.id for i in await o.due(T0 + timedelta(minutes=2))] == ["i2"]
    assert [i.id for i in await o.list(status=IntentStatus.HELD, hold_id="h", dependency="rabbitmq:a")] == ["i1"]
    assert len(await o.list()) == 2
    assert await o.count_by_status() == {"held": 1, "pending": 1}
    assert await o.get("nope") is None


async def test_journal_is_ordered_and_queryable(stores):
    j: JournalStore = stores[JournalStore]
    await j.append([event("a", execution_id="e1", correlation_id="c", trace_id="t1"),
                    event("b", execution_id="e2", correlation_id="c", trace_id="t2")])
    await j.append([event("c", execution_id="e1")])
    all_events = await j.read(0)
    assert [e.kind for e in all_events] == ["a", "b", "c"]
    seqs = [e.seq for e in all_events]
    assert seqs == sorted(seqs) and len(set(seqs)) == 3
    assert [e.kind for e in await j.read(seqs[0])] == ["b", "c"]
    assert [e.kind for e in await j.read(0, limit=1)] == ["a"]
    assert [e.kind for e in await j.query(execution_id="e1")] == ["a", "c"]
    assert [e.kind for e in await j.query(correlation_id="c")] == ["a", "b"]
    assert [e.kind for e in await j.query(trace_id="t2")] == ["b"]
    assert [e.kind for e in await j.query(kinds=["a", "c"], limit=1)] == ["a"]


async def test_definitions_and_key_value(stores):
    d: DefinitionStore = stores[DefinitionStore]
    await d.save(WorkflowDefinition(id="x", version=1, name="one"))
    await d.save(WorkflowDefinition(id="x", version=2, name="two"))
    await d.save(WorkflowDefinition(id="x", version=2, name="two-b"))
    assert (await d.get("x")).name == "two-b"
    assert (await d.get("x", 1)).name == "one"
    assert await d.get("y") is None and await d.get("x", 9) is None
    assert [(x.id, x.version) for x in await d.list()] == [("x", 1), ("x", 2)]

    kv: KeyValueStore = stores[KeyValueStore]
    await kv.put("holds", "b", {"id": "b"})
    await kv.put("holds", "a", {"id": "a", "when": T0})
    await kv.put("holds", "a", {"id": "a", "v": 2})
    await kv.put("other", "z", {"id": "z"})
    assert await kv.get("holds", "a") == {"id": "a", "v": 2}
    assert [x["id"] for x in await kv.list("holds")] == ["a", "b"]
    await kv.delete("holds", "a")
    assert await kv.get("holds", "a") is None and len(await kv.list("holds")) == 1
