"""Unit tests for the materialization registry and runner (no Spark, no project)."""

import logging
import threading
from typing import Any

import pytest

from kelp.tables.materialization.runner import ModelRegistry, ModelSpec, Runner


def make_spec(name: str, depends_on: list[str] | None = None, fn: Any = None) -> ModelSpec:
    """Build a spec with a trivial callable returning the model name."""

    def default_fn(full_refresh: bool = False, spark: Any = None) -> str:
        return name

    return ModelSpec(name=name, fn=fn or default_fn, depends_on=depends_on or [])


def recording_registry(
    graph: dict[str, list[str]], calls: list[str] | None = None
) -> tuple[ModelRegistry, list[str]]:
    """Build a registry whose model functions append their name to a call list."""
    calls = [] if calls is None else calls
    lock = threading.Lock()
    registry = ModelRegistry()

    def make_fn(name: str) -> Any:
        def fn(full_refresh: bool = False, spark: Any = None) -> str:
            with lock:
                calls.append(name)
            return name

        return fn

    for name, deps in graph.items():
        registry.register(ModelSpec(name=name, fn=make_fn(name), depends_on=list(deps)))

    return registry, calls


# -------------------------
# registry
# -------------------------


def test_register_and_lookup() -> None:
    registry = ModelRegistry()
    spec = make_spec("a")
    registry.register(spec)

    assert registry.get("a") is spec
    assert "a" in registry
    assert len(registry) == 1
    assert registry.names() == ["a"]


def test_names_keeps_registration_order() -> None:
    registry = ModelRegistry()
    for name in ["c", "a", "b"]:
        registry.register(make_spec(name))

    assert registry.names() == ["c", "a", "b"]


def test_clear_removes_all_models() -> None:
    registry = ModelRegistry()
    registry.register(make_spec("a"))
    registry.clear()

    assert len(registry) == 0
    assert registry.names() == []


def test_duplicate_registration_warns_and_replaces(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = ModelRegistry()
    registry.register(make_spec("a"))
    replacement = make_spec("a", depends_on=["b"])

    with caplog.at_level(logging.WARNING, logger="kelp.tables.materialization.runner"):
        registry.register(replacement)

    assert "already registered" in caplog.text
    assert "'a'" in caplog.text
    assert registry.get("a") is replacement
    assert len(registry) == 1


def test_get_unknown_model_lists_known_models() -> None:
    registry = ModelRegistry()
    registry.register(make_spec("a"))

    with pytest.raises(KeyError) as excinfo:
        registry.get("missing")

    assert "Unknown model 'missing'" in str(excinfo.value)
    assert "Known models: a" in str(excinfo.value)


def test_registries_are_isolated() -> None:
    first, _ = recording_registry({"a": []})
    second, _ = recording_registry({"b": []})

    assert "a" in first
    assert "a" not in second
    assert "b" in second


# -------------------------
# toposort / levels
# -------------------------


def test_toposort_orders_dependencies_first() -> None:
    registry, _ = recording_registry({"c": ["b"], "a": [], "b": ["a"]})

    order = registry.toposort()

    assert order == ["a", "b", "c"]


def test_toposort_subset_includes_transitive_dependencies() -> None:
    registry, _ = recording_registry({"a": [], "b": ["a"], "c": ["b"], "d": []})

    assert registry.toposort(["c"]) == ["a", "b", "c"]


def test_toposort_detects_cycle_and_names_path() -> None:
    registry, _ = recording_registry({"a": ["b"], "b": ["a"]})

    with pytest.raises(ValueError, match="Cyclic dependency detected: a -> b -> a"):
        registry.toposort(["a"])


def test_toposort_unknown_dependency_names_requiring_model() -> None:
    registry, _ = recording_registry({"a": ["ghost"]})

    with pytest.raises(KeyError) as excinfo:
        registry.toposort(["a"])

    assert "Unknown model 'ghost' required by 'a'" in str(excinfo.value)


def test_levels_group_independent_models() -> None:
    registry, _ = recording_registry({"a": [], "b": [], "c": ["a", "b"], "d": ["c"], "e": ["a"]})

    levels = registry.levels()

    assert [sorted(level) for level in levels] == [["a", "b"], ["c", "e"], ["d"]]


def test_levels_of_empty_registry() -> None:
    assert ModelRegistry().levels() == []


def test_levels_subset_only_covers_closure() -> None:
    registry, _ = recording_registry({"a": [], "b": ["a"], "unrelated": []})

    assert registry.levels(["b"]) == [["a"], ["b"]]


# -------------------------
# planning
# -------------------------


def test_plan_one_includes_upstreams() -> None:
    registry, _ = recording_registry({"a": [], "b": ["a"], "c": ["b"]})
    runner = Runner(registry=registry)

    assert runner.plan_one("c") == ["a", "b", "c"]


def test_plan_one_unknown_model_raises() -> None:
    runner = Runner(registry=ModelRegistry())

    with pytest.raises(KeyError, match="Unknown model 'nope'"):
        runner.plan_one("nope")


def test_plan_all_returns_full_order() -> None:
    registry, _ = recording_registry({"b": ["a"], "a": []})
    runner = Runner(registry=registry)

    assert runner.plan_all() == ["a", "b"]


# -------------------------
# running
# -------------------------


def test_run_one_runs_only_that_model() -> None:
    registry, calls = recording_registry({"a": [], "b": ["a"]})
    runner = Runner(registry=registry)

    result = runner.run_one("b")

    assert result == "b"
    assert calls == ["b"]
    assert [entry.model for entry in runner.runlog.entries] == ["b"]
    assert runner.runlog.entries[0].status == "success"


def test_run_one_unknown_model_raises() -> None:
    runner = Runner(registry=ModelRegistry())

    with pytest.raises(KeyError, match="Unknown model 'nope'"):
        runner.run_one("nope")


def test_run_executes_in_dependency_order() -> None:
    registry, calls = recording_registry({"c": ["b"], "b": ["a"], "a": []})
    runner = Runner(registry=registry)

    runner.run()

    assert calls == ["a", "b", "c"]
    assert [entry.model for entry in runner.runlog.entries] == ["a", "b", "c"]
    assert len(runner.runlog.successes()) == 3


def test_run_subset_runs_upstreams() -> None:
    registry, calls = recording_registry({"a": [], "b": ["a"], "unrelated": []})
    runner = Runner(registry=registry)

    runner.run(["b"])

    assert calls == ["a", "b"]


def test_run_all_forwards_full_refresh() -> None:
    seen: list[bool] = []
    registry = ModelRegistry()
    registry.register(
        ModelSpec(name="a", fn=lambda full_refresh=False, spark=None: seen.append(full_refresh))
    )

    Runner(registry=registry).run_all(full_refresh=True)

    assert seen == [True]


def test_run_parallel_runs_every_model_once_and_logs() -> None:
    registry, calls = recording_registry(
        {"a": [], "b": [], "c": ["a", "b"], "d": ["c"], "e": ["a"]}
    )
    runner = Runner(registry=registry)

    runner.run(parallel=True, max_workers=3)

    assert sorted(calls) == ["a", "b", "c", "d", "e"]
    assert sorted(entry.model for entry in runner.runlog.entries) == [
        "a",
        "b",
        "c",
        "d",
        "e",
    ]
    assert all(entry.status == "success" for entry in runner.runlog.entries)
    assert calls.index("c") > calls.index("a")
    assert calls.index("c") > calls.index("b")
    assert calls.index("d") > calls.index("c")


def test_run_parallel_actually_overlaps_a_level() -> None:
    barrier = threading.Barrier(2, timeout=5)
    registry = ModelRegistry()
    for name in ["a", "b"]:
        registry.register(
            ModelSpec(name=name, fn=lambda full_refresh=False, spark=None: barrier.wait())
        )

    Runner(registry=registry).run(parallel=True, max_workers=2)


def _session_recording_registry(names: list[str]) -> tuple[ModelRegistry, list[Any]]:
    """Build a registry whose models record the session they were handed."""
    seen: list[Any] = []
    lock = threading.Lock()
    registry = ModelRegistry()

    def make_fn() -> Any:
        def fn(full_refresh: bool = False, spark: Any = None) -> None:
            with lock:
                seen.append(spark)

        return fn

    for name in names:
        registry.register(ModelSpec(name=name, fn=make_fn()))
    return registry, seen


def test_run_hands_the_session_to_every_model() -> None:
    """The session is resolved once by the runner and passed down, not rediscovered."""
    registry, seen = _session_recording_registry(["a", "b"])
    session = object()

    Runner(registry=registry, spark=session).run_all()

    assert seen == [session, session]


def test_run_parallel_hands_the_session_to_worker_threads() -> None:
    """PySpark's active session is thread-local, so workers must be handed it."""
    registry, seen = _session_recording_registry(["a", "b", "c"])
    session = object()

    Runner(registry=registry, spark=session).run_all(parallel=True, max_workers=3)

    assert len(seen) == 3
    assert all(handed is session for handed in seen)


def test_failing_model_propagates_and_is_logged() -> None:
    def boom(full_refresh: bool = False, spark: Any = None) -> None:
        raise RuntimeError("kaboom")

    registry, calls = recording_registry({"a": [], "c": ["b"]})
    registry.register(ModelSpec(name="b", fn=boom, depends_on=["a"]))
    runner = Runner(registry=registry)

    with pytest.raises(RuntimeError, match="kaboom"):
        runner.run()

    assert calls == ["a"]
    failures = runner.runlog.failures()
    assert [entry.model for entry in failures] == ["b"]
    assert failures[0].error == "kaboom"
    assert runner.runlog.last() is failures[0]


def test_failing_model_propagates_in_parallel() -> None:
    def boom(full_refresh: bool = False, spark: Any = None) -> None:
        raise RuntimeError("kaboom")

    registry, calls = recording_registry({"a": []})
    registry.register(ModelSpec(name="b", fn=boom))
    runner = Runner(registry=registry)

    with pytest.raises(RuntimeError, match="kaboom"):
        runner.run(parallel=True)

    assert calls == ["a"]
    assert [entry.model for entry in runner.runlog.failures()] == ["b"]
