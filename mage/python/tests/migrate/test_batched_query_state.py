"""Unit tests for the _BatchedQueryState helper in migrate.py."""

import pytest

HASH = "abc123"


class _Closer:
    """A callable closer that records each call and can be told to raise."""

    def __init__(self, raises=None):
        self.calls = []
        self.raises = raises

    def __call__(self, entry):
        self.calls.append(entry)
        if self.raises is not None:
            raise self.raises


def _make(migrate_module, **kwargs):
    closer = _Closer(**kwargs)
    state = migrate_module._BatchedQueryState("test", closer)
    return state, closer


def test_start_records_entry(migrate_module):
    state, _ = _make(migrate_module)
    entry = {"x": 1}
    state.start(HASH, entry)
    assert state.get(HASH) is entry


def test_start_raises_if_duplicate_hash(migrate_module):
    state, _ = _make(migrate_module)
    state.start(HASH, {"x": 1})
    with pytest.raises(RuntimeError, match="already running"):
        state.start(HASH, {"x": 2})


def test_fetch_returns_result_and_keeps_entry(migrate_module):
    state, closer = _make(migrate_module)
    state.start(HASH, {"rows": [1, 2, 3]})
    result = state.fetch(HASH, lambda e: [10, 20])
    assert result == [10, 20]
    assert state.get(HASH)["rows"] == [1, 2, 3]
    assert closer.calls == []


def test_fetch_evicts_on_empty_result(migrate_module):
    state, closer = _make(migrate_module)
    entry = {"x": 1}
    state.start(HASH, entry)
    result = state.fetch(HASH, lambda e: [])
    assert result == []
    assert closer.calls == [entry]
    with pytest.raises(KeyError):
        state.get(HASH)


def test_fetch_evicts_on_exception_and_reraises(migrate_module):
    """The direct bug-#2 fix: an exception during fetch must evict the entry."""
    state, closer = _make(migrate_module)
    entry = {"x": 1}
    state.start(HASH, entry)

    class BoomError(Exception):
        pass

    def _fetcher(_entry):
        raise BoomError("convert failed")

    with pytest.raises(BoomError, match="convert failed"):
        state.fetch(HASH, _fetcher)

    assert closer.calls == [entry]
    with pytest.raises(KeyError):
        state.get(HASH)


def test_closer_exception_is_swallowed_during_fetch_exception(migrate_module):
    """If the closer itself raises while tearing down, the original fetcher
    exception is what propagates — closer errors are silent."""
    state, closer = _make(migrate_module, raises=RuntimeError("close failed"))
    state.start(HASH, {"x": 1})

    class BoomError(Exception):
        pass

    with pytest.raises(BoomError):
        state.fetch(HASH, lambda _e: (_ for _ in ()).throw(BoomError()))

    # Closer was called once even though it raised.
    assert len(closer.calls) == 1


def test_start_after_eviction_due_to_exception_succeeds(migrate_module):
    """The user-facing bug: after a fetch exception the same hash was
    permanently blocked. A subsequent start() must succeed."""
    state, _ = _make(migrate_module)
    state.start(HASH, {"x": 1})

    class BoomError(Exception):
        pass

    with pytest.raises(BoomError):
        state.fetch(HASH, lambda _e: (_ for _ in ()).throw(BoomError()))

    # This used to raise "already running" before the fix.
    state.start(HASH, {"x": 2})
    assert state.get(HASH)["x"] == 2


def test_start_after_eviction_on_empty_succeeds(migrate_module):
    state, _ = _make(migrate_module)
    state.start(HASH, {"x": 1})
    assert state.fetch(HASH, lambda _e: []) == []
    state.start(HASH, {"x": 2})
    assert state.get(HASH)["x"] == 2


def test_different_hashes_are_independent(migrate_module):
    state, closer = _make(migrate_module)
    state.start("h1", {"x": 1})
    state.start("h2", {"x": 2})

    class BoomError(Exception):
        pass

    with pytest.raises(BoomError):
        state.fetch("h1", lambda _e: (_ for _ in ()).throw(BoomError()))

    # h1 evicted, h2 untouched.
    with pytest.raises(KeyError):
        state.get("h1")
    assert state.get("h2")["x"] == 2
    assert len(closer.calls) == 1
