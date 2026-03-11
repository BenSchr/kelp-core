"""Runtime orchestration for framework-agnostic metadata backends."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from kelp.meta.catalog import build_catalog
from kelp.meta.context import MetaContextStore, MetaRuntimeContext
from kelp.meta.loader import collect_yaml_file_paths, load_yaml_files_with_jinja_parallel
from kelp.meta.project import load_framework_settings, resolve_project_file_path
from kelp.meta.settings import resolve_setting
from kelp.meta.spec import MetaProjectSpec


def _collect_all_metadata_paths(
    spec: MetaProjectSpec,
    *,
    project_root: Path,
    framework_settings: Any,
) -> list[Path]:
    file_paths: list[Path] = []
    for object_spec in spec.object_specs:
        setting_value = getattr(framework_settings, object_spec.path_attr, None)
        if not setting_value:
            continue

        paths = [setting_value] if isinstance(setting_value, str) else list(setting_value)

        for relative_path in paths:
            absolute_path = project_root / relative_path
            file_paths.extend(collect_yaml_file_paths(absolute_path))

    deduped = dict.fromkeys(sorted(file_paths))
    return list(deduped.keys())


def build_runtime_context(
    spec: MetaProjectSpec,
    *,
    project_file_path: str,
    target: str | None = None,
    init_vars: dict[str, Any] | None = None,
) -> MetaRuntimeContext:
    """Build runtime context for one framework without global storage.

    Args:
        spec: Framework project specification.
        project_file_path: Project file path.
        target: Selected target.
        init_vars: Optional runtime override vars.

    Returns:
        Built framework runtime context.

    """
    framework_settings, runtime_vars, _ = load_framework_settings(
        spec,
        project_file_path=project_file_path,
        target=target,
        init_vars=init_vars,
    )

    project_root = Path(project_file_path).parent

    all_metadata_files = _collect_all_metadata_paths(
        spec,
        project_root=project_root,
        framework_settings=framework_settings,
    )

    merged_raw_payload = load_yaml_files_with_jinja_parallel(
        all_metadata_files,
        jinja_context=runtime_vars,
    )

    raw_objects = {
        object_spec.root_key: merged_raw_payload.get(object_spec.root_key, [])
        for object_spec in spec.object_specs
    }

    catalog = build_catalog(
        raw_objects=raw_objects,
        project_settings=framework_settings,
        object_specs=spec.object_specs,
        project_root=str(project_root),
    )

    return MetaRuntimeContext(
        framework_id=spec.framework_id,
        project_root=str(project_root),
        project_file_path=project_file_path,
        target=target,
        runtime_vars=runtime_vars,
        project_settings=framework_settings,
        catalog=catalog,
    )


def init_runtime(
    spec: MetaProjectSpec,
    *,
    project_file_path: str | None = None,
    target: str | None = None,
    init_vars: dict[str, Any] | None = None,
    refresh: bool = False,
    store_in_global: bool = True,
) -> MetaRuntimeContext:
    """Initialize or retrieve runtime context for the framework.

    Args:
        spec: Framework project specification.
        project_file_path: Optional explicit project file path.
        target: Optional target name.
        init_vars: Optional runtime var overrides.
        refresh: Whether to recreate existing stored context.
        store_in_global: Whether to persist context in global store.

    Returns:
        Framework runtime context.

    """
    if spec.resolve_runtime_settings:
        target = resolve_setting(
            key=spec.target_setting_key,
            default=target,
            init_settings={spec.target_setting_key: target} if target is not None else None,
            env_prefix=spec.settings_env_prefix,
            spark_prefix=spec.settings_spark_prefix,
        )
        project_file_path = resolve_setting(
            key=spec.project_file_setting_key,
            default=project_file_path,
            init_settings={spec.project_file_setting_key: project_file_path}
            if project_file_path is not None
            else None,
            env_prefix=spec.settings_env_prefix,
            spark_prefix=spec.settings_spark_prefix,
        )

    resolved_project_file_path = resolve_project_file_path(
        spec.project_filename,
        explicit_project_file_path=project_file_path,
    )

    return MetaContextStore.get_or_create(
        spec.framework_id,
        factory=lambda: build_runtime_context(
            spec,
            project_file_path=resolved_project_file_path,
            target=target,
            init_vars=init_vars,
        ),
        refresh=refresh,
        store_in_global=store_in_global,
    )


def get_context(framework_id: str) -> MetaRuntimeContext | None:
    """Get stored runtime context for the given framework.

    Args:
        framework_id: Framework identifier.

    Returns:
        Stored context, if available.

    """
    return MetaContextStore.get(framework_id)
