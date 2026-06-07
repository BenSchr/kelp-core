from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

# -------------------------
# registry
# -------------------------


@dataclass
class ModelSpec:
    name: str
    fn: Callable[..., Any]
    depends_on: list[str] = field(default_factory=list)


_REGISTRY: dict[str, ModelSpec] = {}


# -------------------------
# planning
# -------------------------


def _toposort(names: list[str] | None = None) -> list[str]:
    visited: set[str] = set()
    visiting: set[str] = set()
    order: list[str] = []

    def visit(name: str, path: list[str]) -> None:
        if name in visited:
            return

        if name in visiting:
            cycle = " -> ".join([*path, name])
            raise ValueError(f"Cyclic dependency detected: {cycle}")

        spec = _REGISTRY.get(name)
        if spec is None:
            raise KeyError(f"Unknown model: {name}")

        visiting.add(name)
        for dep in spec.depends_on:
            visit(dep, [*path, name])
        visiting.remove(name)

        visited.add(name)
        order.append(name)

    for name in names or list(_REGISTRY.keys()):
        visit(name, [])

    return order


# -------------------------
# run log
# -------------------------


@dataclass
class RunLogEntry:
    model: str
    status: str  # success | failed
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    error: str | None = None


@dataclass
class RunLog:
    entries: list[RunLogEntry] = field(default_factory=list)

    def add(self, entry: RunLogEntry) -> None:
        self.entries.append(entry)

    def last(self) -> RunLogEntry | None:
        return self.entries[-1] if self.entries else None

    def successes(self) -> list[RunLogEntry]:
        return [e for e in self.entries if e.status == "success"]

    def failures(self) -> list[RunLogEntry]:
        return [e for e in self.entries if e.status == "failed"]


# -------------------------
# runner
# -------------------------


class Runner:
    def __init__(self):
        self.runlog = RunLog()

    def plan_one(self, name: str) -> list[str]:
        if name not in _REGISTRY:
            raise KeyError(f"Unknown model: {name}")
        return [name]

    def plan_all(self) -> list[str]:
        return _toposort()

    def run_one(self, name: str):
        if name not in _REGISTRY:
            raise KeyError(f"Unknown model: {name}")
        return self._run_model(name)

    def run_all(self, full_refresh: bool = False) -> None:
        for name in self.plan_all():
            self._run_model(name, full_refresh=full_refresh)

    def _run_model(self, name: str, full_refresh: bool = False) -> Any:
        spec = _REGISTRY[name]

        started_at = datetime.now(UTC)
        started = perf_counter()

        try:
            result = spec.fn(full_refresh=full_refresh)

            finished_at = datetime.now(UTC)
            duration = perf_counter() - started

            self.runlog.add(
                RunLogEntry(
                    model=name,
                    status="success",
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=duration,
                )
            )
            return result

        except Exception as exc:
            finished_at = datetime.now(UTC)
            duration = perf_counter() - started

            self.runlog.add(
                RunLogEntry(
                    model=name,
                    status="failed",
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=duration,
                    error=str(exc),
                )
            )
            raise
