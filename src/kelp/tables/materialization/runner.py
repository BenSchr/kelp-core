"""Registry, planning and execution of materialized models."""

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, Literal

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# -------------------------
# registry
# -------------------------


@dataclass
class ModelSpec:
    """A registered materialized model.

    Attributes:
        name: Unique model name.
        fn: Callable executing the materialization, invoked with ``full_refresh``
            and ``spark``. The session is handed over rather than discovered by the
            callable, because PySpark's active session is thread-local and would be
            invisible to a model running on a worker thread.
        depends_on: Names of models that must run before this one.
    """

    name: str
    fn: Callable[..., Any]
    depends_on: list[str] = field(default_factory=list)


class ModelRegistry:
    """Registered materialized models and their dependency graph."""

    def __init__(self) -> None:
        """Create an empty registry."""
        self._specs: dict[str, ModelSpec] = {}

    def register(self, spec: ModelSpec) -> None:
        """Register a model, logging a warning when it replaces an existing name.

        Args:
            spec: Model specification to register.
        """
        if spec.name in self._specs:
            logger.warning(
                "Model '%s' is already registered; replacing the previous definition",
                spec.name,
            )
        self._specs[spec.name] = spec

    def get(self, name: str) -> ModelSpec:
        """Return the spec registered under ``name``.

        Args:
            name: Model name.

        Returns:
            The registered model specification.

        Raises:
            KeyError: If no model is registered under ``name``.
        """
        return self._require(name)

    def names(self) -> list[str]:
        """Return all registered model names in registration order."""
        return list(self._specs)

    def clear(self) -> None:
        """Remove all registered models."""
        self._specs.clear()

    def __contains__(self, name: str) -> bool:
        """Return whether ``name`` is registered."""
        return name in self._specs

    def __len__(self) -> int:
        """Return the number of registered models."""
        return len(self._specs)

    def toposort(self, names: list[str] | None = None) -> list[str]:
        """Dependency-ordered model names; all of them when names is None.

        Transitive dependencies of the requested models are always included.

        Args:
            names: Models to order, or None for every registered model.

        Returns:
            Model names ordered so that every dependency precedes its consumers.

        Raises:
            KeyError: If a requested model or one of its dependencies is unknown.
            ValueError: If the dependency graph contains a cycle.
        """
        visited: set[str] = set()
        visiting: set[str] = set()
        order: list[str] = []

        def visit(name: str, path: list[str], required_by: str | None) -> None:
            if name in visited:
                return

            if name in visiting:
                cycle = " -> ".join([*path, name])
                raise ValueError(f"Cyclic dependency detected: {cycle}")

            spec = self._require(name, required_by=required_by)

            visiting.add(name)
            for dep in spec.depends_on:
                visit(dep, [*path, name], required_by=name)
            visiting.remove(name)

            visited.add(name)
            order.append(name)

        for name in self.names() if names is None else names:
            visit(name, [], None)

        return order

    def levels(self, names: list[str] | None = None) -> list[list[str]]:
        """Dependency levels: every model in a level may run concurrently.

        Args:
            names: Models to group, or None for every registered model.

        Returns:
            Lists of model names, each level depending only on earlier levels.

        Raises:
            KeyError: If a requested model or one of its dependencies is unknown.
            ValueError: If the dependency graph contains a cycle.
        """
        order = self.toposort(names)
        selected = set(order)

        depth: dict[str, int] = {}
        for name in order:
            deps = [d for d in self._specs[name].depends_on if d in selected]
            depth[name] = 1 + max((depth[d] for d in deps), default=-1)

        levels: list[list[str]] = [[] for _ in range(max(depth.values(), default=-1) + 1)]
        for name in order:
            levels[depth[name]].append(name)

        return levels

    def _require(self, name: str, required_by: str | None = None) -> ModelSpec:
        """Return the spec for ``name`` or raise a helpful KeyError.

        Args:
            name: Model name to look up.
            required_by: Model that declared ``name`` as a dependency, if any.

        Returns:
            The registered model specification.

        Raises:
            KeyError: If no model is registered under ``name``.
        """
        spec = self._specs.get(name)
        if spec is not None:
            return spec

        known = ", ".join(self.names()) or "<none>"
        origin = f" required by '{required_by}'" if required_by else ""
        raise KeyError(f"Unknown model '{name}'{origin}. Known models: {known}")


#: Default registry the ``@materialized`` decorator registers into.
REGISTRY = ModelRegistry()


# -------------------------
# run log
# -------------------------


@dataclass
class RunLogEntry:
    """Outcome of a single model run."""

    model: str
    status: Literal["success", "failed"]
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    error: str | None = None


@dataclass
class RunLog:
    """Collected run outcomes of a runner."""

    entries: list[RunLogEntry] = field(default_factory=list)

    def add(self, entry: RunLogEntry) -> None:
        """Append an entry to the log."""
        self.entries.append(entry)

    def last(self) -> RunLogEntry | None:
        """Return the most recent entry, or None when the log is empty."""
        return self.entries[-1] if self.entries else None

    def successes(self) -> list[RunLogEntry]:
        """Return all successful entries."""
        return [e for e in self.entries if e.status == "success"]

    def failures(self) -> list[RunLogEntry]:
        """Return all failed entries."""
        return [e for e in self.entries if e.status == "failed"]


# -------------------------
# runner
# -------------------------


class Runner:
    """Executes registered materialized models in dependency order."""

    def __init__(
        self,
        registry: ModelRegistry | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        """Create a runner.

        Args:
            registry: Registry to run models from; defaults to the module-level
                ``REGISTRY``.
            spark: SparkSession handed to every model. Defaults to the session
                active when a run starts.
        """
        self.registry = registry if registry is not None else REGISTRY
        self.runlog = RunLog()
        self._spark = spark
        self._runlog_lock = threading.Lock()

    def _resolve_spark(self) -> SparkSession | None:
        """Resolve the SparkSession to hand to models, on the calling thread.

        PySpark tracks the active session per thread, so a model running in a
        worker thread cannot discover it. Resolving here — before any model runs —
        means every model receives the session the run was started from.

        Returns:
            The injected or active session, or ``None`` when there is none.
        """
        if self._spark is None:
            self._spark = SparkSession.getActiveSession()
        return self._spark

    def plan_one(self, name: str) -> list[str]:
        """Dependency-ordered names needed to build ``name``, including ``name``.

        Args:
            name: Model to plan.

        Returns:
            Model names in the order they must run.
        """
        return self.registry.toposort([name])

    def plan_all(self) -> list[str]:
        """Dependency-ordered names of every registered model."""
        return self.registry.toposort()

    def run_one(self, name: str, full_refresh: bool = False) -> Any:
        """Run exactly this one model and return its result.

        Upstreams are NOT run - use ``run(plan_one(name))`` for that.

        Args:
            name: Model to run.
            full_refresh: Whether to rebuild the target from scratch.

        Returns:
            Whatever the model function returns.
        """
        self.registry.get(name)
        return self._run_model(name, full_refresh=full_refresh, spark=self._resolve_spark())

    def run(
        self,
        names: list[str] | None = None,
        full_refresh: bool = False,
        parallel: bool = False,
        max_workers: int = 4,
    ) -> None:
        """Run models in dependency order, optionally running each level concurrently.

        With ``parallel=True`` the models of a dependency level are submitted from
        several threads to the same SparkSession, which Spark supports. The session
        is resolved once here and handed to each model, since worker threads cannot
        see the active session themselves.

        Args:
            names: Models to run, or None for every registered model.
            full_refresh: Whether to rebuild the targets from scratch.
            parallel: Whether to run independent models of a level concurrently.
            max_workers: Maximum number of threads used when ``parallel`` is True.
        """
        spark = self._resolve_spark()

        if not parallel:
            for name in self.registry.toposort(names):
                self._run_model(name, full_refresh=full_refresh, spark=spark)
            return

        for level in self.registry.levels(names):
            if len(level) == 1:
                self._run_model(level[0], full_refresh=full_refresh, spark=spark)
                continue

            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [
                    pool.submit(self._run_model, name, full_refresh=full_refresh, spark=spark)
                    for name in level
                ]
                errors = [future.exception() for future in futures]

            for error in errors:
                if error is not None:
                    raise error

    def run_all(
        self,
        full_refresh: bool = False,
        parallel: bool = False,
        max_workers: int = 4,
    ) -> None:
        """Run every registered model in dependency order.

        Args:
            full_refresh: Whether to rebuild the targets from scratch.
            parallel: Whether to run independent models of a level concurrently.
            max_workers: Maximum number of threads used when ``parallel`` is True.
        """
        self.run(None, full_refresh=full_refresh, parallel=parallel, max_workers=max_workers)

    def _run_model(
        self,
        name: str,
        full_refresh: bool = False,
        spark: SparkSession | None = None,
    ) -> Any:
        """Execute a single model function and record its outcome.

        Args:
            name: Model to run.
            full_refresh: Whether to rebuild the target from scratch.
            spark: Session handed to the model, resolved by the caller.

        Returns:
            Whatever the model function returns.

        Raises:
            Exception: Any error raised by the model function, after logging it.
        """
        spec = self.registry.get(name)

        started_at = datetime.now(UTC)
        started = perf_counter()

        try:
            result = spec.fn(full_refresh=full_refresh, spark=spark)
        except Exception as exc:
            self._record(
                RunLogEntry(
                    model=name,
                    status="failed",
                    started_at=started_at,
                    finished_at=datetime.now(UTC),
                    duration_seconds=perf_counter() - started,
                    error=str(exc),
                )
            )
            raise

        self._record(
            RunLogEntry(
                model=name,
                status="success",
                started_at=started_at,
                finished_at=datetime.now(UTC),
                duration_seconds=perf_counter() - started,
            )
        )
        return result

    def _record(self, entry: RunLogEntry) -> None:
        """Append an entry to the run log in a thread-safe way."""
        with self._runlog_lock:
            self.runlog.add(entry)
