"""Generic manifest serialization and deserialization for meta runtimes.

A manifest is a JSON snapshot of a fully built MetaRuntimeContext. It allows
frameworks to skip project discovery, variable resolution, and metadata loading
on subsequent initializations by loading the pre-built context directly.

This module is framework-agnostic and must not contain any Kelp-specific logic.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from kelp.meta.context import MetaRuntimeContext
from kelp.meta.spec import MetaObjectSpec, MetaProjectSpec

logger = logging.getLogger(__name__)

MANIFEST_VERSION = 1


class ManifestSpecSignature(BaseModel):
    """Compact signature of a MetaProjectSpec for compatibility validation.

    Attributes:
        framework_id: Framework identifier.
        project_header: Top-level YAML key for framework settings.
        object_catalog_attrs: Sorted list of catalog attribute names from object specs.
    """

    framework_id: str = Field(description="Framework identifier")
    project_header: str = Field(description="Project YAML header key")
    object_catalog_attrs: list[str] = Field(
        description="Sorted catalog attribute names from object specs",
    )


class ManifestPayload(BaseModel):
    """Top-level manifest JSON structure.

    Attributes:
        manifest_version: Schema version of the manifest format.
        spec_signature: Compact spec metadata for compatibility checks.
        project_root: Project root directory at time of export.
        project_file_path: Resolved project file path at time of export.
        target: Selected target at time of export, if any.
        runtime_vars: Resolved runtime variables.
        project_settings: Serialized framework settings payload.
        catalog: Serialized catalog keyed by catalog_attr.
    """

    manifest_version: int = Field(description="Manifest format version")
    spec_signature: ManifestSpecSignature = Field(description="Spec compatibility signature")
    project_root: str = Field(description="Project root path at export time")
    project_file_path: str = Field(description="Project file path at export time")
    target: str | None = Field(default=None, description="Target at export time")
    runtime_vars: dict[str, Any] = Field(description="Resolved runtime variables")
    project_settings: dict[str, Any] = Field(description="Serialized framework settings")
    catalog: dict[str, list[dict[str, Any]]] = Field(description="Serialized catalog payload")


def _build_spec_signature(spec: MetaProjectSpec) -> ManifestSpecSignature:
    """Build a compact signature from a MetaProjectSpec.

    Args:
        spec: The framework project specification.

    Returns:
        A ManifestSpecSignature for embedding in manifests.
    """
    return ManifestSpecSignature(
        framework_id=spec.framework_id,
        project_header=spec.project_header,
        object_catalog_attrs=sorted(s.catalog_attr for s in spec.object_specs),
    )


def _validate_manifest_compatibility(
    manifest: ManifestPayload,
    spec: MetaProjectSpec,
) -> None:
    """Validate that a manifest is compatible with the active spec.

    Args:
        manifest: The loaded manifest payload.
        spec: The active framework project specification.

    Raises:
        ValueError: If the manifest is incompatible with the spec.
    """
    if manifest.manifest_version != MANIFEST_VERSION:
        raise ValueError(
            f"Manifest version mismatch: expected {MANIFEST_VERSION}, "
            f"got {manifest.manifest_version}. Re-export the manifest.",
        )

    expected_sig = _build_spec_signature(spec)

    if manifest.spec_signature.framework_id != expected_sig.framework_id:
        raise ValueError(
            f"Manifest framework mismatch: expected '{expected_sig.framework_id}', "
            f"got '{manifest.spec_signature.framework_id}'.",
        )

    if manifest.spec_signature.project_header != expected_sig.project_header:
        raise ValueError(
            f"Manifest project header mismatch: expected '{expected_sig.project_header}', "
            f"got '{manifest.spec_signature.project_header}'.",
        )

    if manifest.spec_signature.object_catalog_attrs != expected_sig.object_catalog_attrs:
        raise ValueError(
            f"Manifest object specs mismatch: expected {expected_sig.object_catalog_attrs}, "
            f"got {manifest.spec_signature.object_catalog_attrs}.",
        )


def _rebuild_catalog_from_manifest(
    manifest: ManifestPayload,
    object_specs: tuple[MetaObjectSpec, ...],
) -> dict[str, list[Any]]:
    """Reconstruct typed catalog objects from serialized manifest data.

    Args:
        manifest: The loaded manifest payload.
        object_specs: Object type specifications from the active spec.

    Returns:
        Catalog dict keyed by catalog_attr with validated Pydantic model instances.

    Raises:
        ValueError: If catalog items fail validation.
    """
    catalog: dict[str, list[Any]] = {}
    spec_lookup = {s.catalog_attr: s for s in object_specs}

    for catalog_attr, items in manifest.catalog.items():
        obj_spec = spec_lookup.get(catalog_attr)
        if obj_spec is None:
            logger.warning(
                "Manifest contains unknown catalog key '%s', skipping.",
                catalog_attr,
            )
            continue

        parsed_items: list[Any] = []
        for item_data in items:
            try:
                parsed_items.append(obj_spec.model_class(**item_data))
            except Exception as error:
                raise ValueError(
                    f"Failed to validate {obj_spec.model_label} from manifest: {error}",
                ) from error
        catalog[catalog_attr] = parsed_items

    # Ensure empty lists for specs not present in manifest
    for spec in object_specs:
        if spec.catalog_attr not in catalog:
            catalog[spec.catalog_attr] = []

    return catalog


def load_manifest(
    manifest_file_path: str,
    spec: MetaProjectSpec,
    *,
    expected_target: str | None = None,
    expected_project_file_path: str | None = None,
) -> MetaRuntimeContext:
    """Load a MetaRuntimeContext from a manifest JSON file.

    Performs compatibility validation against the active spec and optional
    consistency checks against expected target/project file.

    Args:
        manifest_file_path: Path to the manifest JSON file.
        spec: Active framework project specification.
        expected_target: If provided, validate manifest target matches.
        expected_project_file_path: If provided, validate manifest project file matches.

    Returns:
        A fully reconstructed MetaRuntimeContext.

    Raises:
        FileNotFoundError: If manifest file does not exist.
        ValueError: If manifest is incompatible or fails validation.
    """
    path = Path(manifest_file_path)
    if not path.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_file_path}")

    logger.debug("Loading manifest from: %s", manifest_file_path)

    raw_data = path.read_text(encoding="utf-8")
    try:
        manifest = ManifestPayload.model_validate_json(raw_data)
    except Exception as error:
        raise ValueError(
            f"Failed to parse manifest file '{manifest_file_path}': {error}"
        ) from error

    _validate_manifest_compatibility(manifest, spec)

    if expected_target is not None and manifest.target != expected_target:
        raise ValueError(
            f"Manifest target mismatch: manifest has target='{manifest.target}', "
            f"but expected target='{expected_target}'.",
        )

    if (
        expected_project_file_path is not None
        and manifest.project_file_path != expected_project_file_path
    ):
        raise ValueError(
            f"Manifest project file mismatch: manifest has "
            f"project_file_path='{manifest.project_file_path}', "
            f"but expected '{expected_project_file_path}'.",
        )

    # Reconstruct typed project settings
    project_settings = spec.project_settings_model(**manifest.project_settings)

    # Reconstruct typed catalog objects
    catalog = _rebuild_catalog_from_manifest(manifest, spec.object_specs)

    return MetaRuntimeContext(
        framework_id=manifest.spec_signature.framework_id,
        project_root=manifest.project_root,
        project_file_path=manifest.project_file_path,
        target=manifest.target,
        runtime_vars=manifest.runtime_vars,
        project_settings=project_settings,
        catalog=catalog,
        generated_from_manifest=True,
        manifest_file_path=manifest_file_path,
    )


def export_manifest(
    ctx: MetaRuntimeContext,
    spec: MetaProjectSpec,
    output_path: str,
) -> str:
    """Export a MetaRuntimeContext to a manifest JSON file.

    Args:
        ctx: The runtime context to serialize.
        spec: The framework project specification.
        output_path: Path where the manifest JSON file will be written.

    Returns:
        The absolute path of the written manifest file.

    Raises:
        TypeError: If runtime_vars contain non-JSON-serializable values.
    """
    # Validate runtime_vars are JSON-serializable
    try:
        json.dumps(ctx.runtime_vars)
    except (TypeError, ValueError) as error:
        raise TypeError(
            f"runtime_vars contain non-JSON-serializable values: {error}. "
            "Ensure all runtime variables are JSON-safe before exporting.",
        ) from error

    # Serialize project settings
    if isinstance(ctx.project_settings, BaseModel):
        project_settings_data = ctx.project_settings.model_dump(mode="python")
    else:
        project_settings_data = dict(ctx.project_settings)

    # Serialize catalog items
    catalog_data: dict[str, list[dict[str, Any]]] = {}
    for catalog_attr, items in ctx.catalog.items():
        serialized_items: list[dict[str, Any]] = []
        for item in items:
            if isinstance(item, BaseModel):
                serialized_items.append(item.model_dump(mode="python"))
            elif isinstance(item, dict):
                serialized_items.append(item)
            else:
                raise TypeError(
                    f"Catalog item in '{catalog_attr}' is not serializable: {type(item)}",
                )
        catalog_data[catalog_attr] = serialized_items

    manifest = ManifestPayload(
        manifest_version=MANIFEST_VERSION,
        spec_signature=_build_spec_signature(spec),
        project_root=ctx.project_root,
        project_file_path=ctx.project_file_path,
        target=ctx.target,
        runtime_vars=ctx.runtime_vars,
        project_settings=project_settings_data,
        catalog=catalog_data,
    )

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )

    logger.debug("Manifest exported to: %s", path.absolute())
    return str(path.absolute())
