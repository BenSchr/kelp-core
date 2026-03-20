from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from pathlib import Path

import yaml

from kelp.config import get_context
from kelp.meta.context import MetaRuntimeContext
from kelp.models.metric_view import MetricView
from kelp.models.model import Column, ForeignKeyConstraint, Model, PrimaryKeyConstraint
from kelp.models.project_config import ProjectConfig, RemoteCatalogConfig
from kelp.utils.dict_parser import apply_cfg_hierarchy_to_dict_recursive

logger = logging.getLogger(__name__)


def _get_project_config(ctx: MetaRuntimeContext | None) -> ProjectConfig:
    if ctx is None:
        raise RuntimeError("RuntimeContext required for ServicePathConfig.from_context()")
    return ctx.project_settings


@dataclass
class ServicePathConfig:
    """Central configuration for service file path resolution.

    Encapsulates all path context needed by services (YamlManager, PipelineManager, etc)
    so they don't need to call get_context() internally. This centralizes configuration
    management and makes services more testable and reusable.

    Args:
        project_root: Absolute path to project root.
        service_root: Service-specific base path (e.g., "kelp_metadata/models").
                     Relative to project_root.
        hierarchy_config: Hierarchy configuration dict from project config (e.g., models config).
                         Used for resolving defaults and path mappings.

    Example:
        config = ServicePathConfig.from_context()
        report = YamlManager.patch_model_yaml(table, path_config=config)
    """

    project_root: Path
    service_root: Path
    hierarchy_config: dict | None = None

    @staticmethod
    def from_context(
        service_root_key: str = "models_path",
        hierarchy_config_key: str = "models",
    ) -> ServicePathConfig:
        """Create ServicePathConfig from current runtime context.

        Args:
            service_root_key: Key in project_config for the service root path.
                            Default: "models_path" for YamlManager.

        Returns:
            ServicePathConfig with paths from context.

        Raises:
            RuntimeError: If context is not initialized or required config is missing.
        """
        ctx = get_context()
        project_config = _get_project_config(ctx)
        if not ctx or not project_config:
            raise RuntimeError("RuntimeContext required for ServicePathConfig.from_context()")

        project_root = Path(ctx.project_root)
        service_root = Path(getattr(project_config, service_root_key, ""))

        if not service_root:
            raise RuntimeError(
                f"Project config missing '{service_root_key}' for service path resolution",
            )

        hierarchy_config = getattr(project_config, hierarchy_config_key, None)

        return ServicePathConfig(
            project_root=project_root,
            service_root=service_root,
            hierarchy_config=hierarchy_config,
        )

    @property
    def service_root_absolute(self) -> Path:
        """Get absolute path to service root."""
        return self.project_root / self.service_root


@dataclass
class YamlUpdateReport:
    """Report summarizing the changes made during a YAML patch operation."""

    model_name: str
    file_path: Path
    result_model: dict
    changes_made: bool
    added_fields: list[str]
    updated_fields: list[str]
    removed_fields: list[str]


class YamlManager:
    """Service for writing and patching model YAML files."""

    @classmethod
    def patch_metric_view_yaml(
        cls,
        source_metric_view: MetricView,
        *,
        path_config: ServicePathConfig | None = None,
        relative_file_path: str | Path | None = None,
        dry_run: bool = False,
    ) -> YamlUpdateReport:
        """Patch or create a metric view YAML file with metadata from a source metric view.

        Args:
            source_metric_view: Metric view sourced from Databricks metadata.
            path_config: Service path configuration (project root, service root, hierarchy config).
                If not provided, auto-discovers from runtime context (requires context to be initialized).
            relative_file_path: Optional explicit path override. If provided, uses this path directly.

        Returns:
            YamlUpdateReport with details about changes made.
        """
        if path_config is None:
            path_config = ServicePathConfig.from_context(
                service_root_key="metrics_path",
                hierarchy_config_key="metric_views",
            )

        resolved_file_path = cls._resolve_or_determine_path(
            name=source_metric_view.name,
            origin_file_path=source_metric_view.origin_file_path,
            schema=source_metric_view.schema_,
            catalog=source_metric_view.catalog,
            path_config=path_config,
            explicit_path=relative_file_path,
            kind="metric view",
        )
        if relative_file_path is None:
            logger.debug(
                "Determined file path for metric view %s: %s",
                source_metric_view.name,
                resolved_file_path,
            )

        full_file_path = path_config.service_root_absolute / resolved_file_path
        document = cls._load_yaml_document(full_file_path)
        models = document.get("kelp_metric_views")
        if not isinstance(models, list):
            models = []

        model_name = source_metric_view.name
        model_index = cls._find_model_index(models, model_name)
        if model_index is None:
            model = {"name": model_name}
            models.append(model)
            original_model = None
        else:
            model = models[model_index]
            original_model = copy.deepcopy(model)

        defaults = cls._get_hierarchy_defaults(resolved_file_path, path_config)
        cls._patch_metric_view_dict(model, source_metric_view, defaults)

        changes_made = original_model is None or model != original_model
        added_fields, updated_fields, removed_fields = cls._detect_changes(original_model, model)

        if changes_made and not dry_run:
            document["kelp_metric_views"] = models
            logger.debug(
                "Writing updated YAML for metric view %s to %s",
                source_metric_view.name,
                full_file_path,
            )
            cls._write_yaml_document(full_file_path, document)

        return YamlUpdateReport(
            model_name=source_metric_view.name,
            file_path=full_file_path,
            result_model=model,
            changes_made=changes_made,
            added_fields=added_fields,
            updated_fields=updated_fields,
            removed_fields=removed_fields,
        )

    @classmethod
    def patch_model_yaml(
        cls,
        source_model: Model,
        *,
        path_config: ServicePathConfig | None = None,
        relative_file_path: str | Path | None = None,
        dry_run: bool = False,
        remote_catalog_config: RemoteCatalogConfig | None = None,
    ) -> YamlUpdateReport:
        """Patch or create a model YAML file with metadata from a source table.

        This is the main entry point for patching table models. It handles:
        - Loading existing YAML or creating new document
        - Finding or creating model entry for the table
        - Applying model patching (description, tags, constraints, columns)
        - Detecting and writing changed files

        Args:
            source_model: Model sourced from Databricks metadata.
            path_config: Service path configuration (project root, service root, hierarchy config).
                        If not provided, auto-discovers from runtime context (requires context to be initialized).
            relative_file_path: Optional explicit path override. If provided, uses this path directly.

        Returns:
            YamlUpdateReport with details about changes made.

        Example:
            # With explicit config
            config = ServicePathConfig.from_context()
            report = YamlManager.patch_model_yaml(table, path_config=config)

            # With auto-discovered config (requires context)
            report = YamlManager.patch_model_yaml(table)
        """
        if path_config is None:
            path_config = ServicePathConfig.from_context()

        resolved_file_path = cls._resolve_or_determine_path(
            name=source_model.name,
            origin_file_path=source_model.origin_file_path,
            schema=source_model.schema_,
            catalog=source_model.catalog,
            path_config=path_config,
            explicit_path=relative_file_path,
            kind="table",
        )
        if relative_file_path is None:
            logger.debug(
                "Determined file path for table %s: %s",
                source_model.name,
                resolved_file_path,
            )

        full_file_path = path_config.service_root_absolute / resolved_file_path
        document = cls._load_yaml_document(full_file_path)
        models = document.get("kelp_models")
        if not isinstance(models, list):
            models = []

        model_name = source_model.name
        model_index = cls._find_model_index(models, model_name)
        if model_index is None:
            model = {"name": model_name}
            models.append(model)
            original_model = None
        else:
            model = models[model_index]
            # Keep a deep copy of original state for change detection
            original_model = copy.deepcopy(model)

        defaults = cls._get_hierarchy_defaults(resolved_file_path, path_config)
        cls._patch_model_dict(model, source_model, defaults, remote_catalog_config)

        # Detect changes
        changes_made = original_model is None or model != original_model
        added_fields, updated_fields, removed_fields = cls._detect_changes(original_model, model)

        # Only write if something changed and not in dry_run mode
        if changes_made and not dry_run:
            document["kelp_models"] = models
            logger.debug(
                "Writing updated YAML for table %s to %s",
                source_model.name,
                full_file_path,
            )
            cls._write_yaml_document(full_file_path, document)

        return YamlUpdateReport(
            model_name=source_model.name,
            file_path=full_file_path,
            result_model=model,
            changes_made=changes_made,
            added_fields=added_fields,
            updated_fields=updated_fields,
            removed_fields=removed_fields,
        )

    @classmethod
    def model_to_dict(
        cls,
        source_model: Model,
        include_hierarchy_defaults: bool = True,
        remote_catalog_config: RemoteCatalogConfig | None = None,
    ) -> dict:
        """Convert a Model object to a YAML model dict.

        Serializes a Model into a model dictionary suitable for YAML output in kelp_models.
        Used by all model serialization use cases: CLI generation, file patching, etc.
        Provides unified handling of model fields (description, tags, constraints, columns).

        Args:
            source_model: Model to convert to model dict.
            include_hierarchy_defaults: If True, applies project config hierarchy defaults
                to reduce redundant field values. Set to False for standalone display use
            if changes_made and not dry_run:

        Returns:
            Model dictionary with name and patchable fields. Empty values are excluded.

        Example:
            # For CLI generation (no context needed)
            model = YamlManager.model_to_dict(table, include_hierarchy_defaults=False)
            content = {"kelp_models": [model]}

            # For file patching (with context)
            model = YamlManager.model_to_dict(table, include_hierarchy_defaults=True)
        """
        model = {"name": source_model.name}

        # Get hierarchy defaults if requested and available
        defaults = {}
        if include_hierarchy_defaults and source_model.origin_file_path:
            defaults = cls._get_hierarchy_defaults(
                Path(source_model.origin_file_path),
                ServicePathConfig.from_context(),
            )

        # Patch using standard logic (handles all patchable fields)
        cls._patch_model_dict(model, source_model, defaults, remote_catalog_config)

        return model

    @classmethod
    def _patch_model_dict(
        cls,
        model: dict,
        source_model: Model,
        defaults: dict,
        remote_catalog_config: RemoteCatalogConfig | None = None,
    ) -> None:
        """Patch a single model dict in-place.

        Always updates fields from source. When a field has a default value in hierarchy config,
        only writes it to YAML if it differs from the default (to avoid redundancy).

        Args:
            model: Existing YAML model dict to patch in-place.
            source_model: Source model with new values.
            defaults: Hierarchy defaults for the model path.
            remote_catalog_config: When provided, filters table_properties based on
                table_property_mode (append: only update existing local keys;
                managed: only sync keys in managed_table_properties list).
        """
        # Only write description if it differs from default
        default_description = defaults.get("description")
        if source_model.description != default_description:
            cls._set_or_remove(model, "description", source_model.description)
        elif "description" in model:
            # Remove if matches default (no need to write it)
            model.pop("description", None)

        # Filter tags to exclude those matching defaults
        filtered_tags = cls._filter_tags(source_model.tags, defaults.get("tags"))
        cls._set_or_remove(model, "tags", filtered_tags)

        # Only write constraints if no default or differs from default
        default_constraints = defaults.get("constraints")
        serialized_constraints = cls._serialize_constraints(source_model.constraints)
        serialized_constraints = cls._normalize_foreign_key_references(serialized_constraints)
        serialized_constraints = cls._preserve_local_foreign_key_variables(
            serialized_constraints,
            model.get("constraints"),
        )
        if serialized_constraints != default_constraints:
            cls._set_or_remove(model, "constraints", serialized_constraints)
        elif "constraints" in model:
            model.pop("constraints", None)

        # Table properties: deserialize JSON-encoded values back to complex types
        # and filter out defaults from hierarchy config
        deserialized_props = Model.deserialize_property_values(source_model.table_properties)
        default_props = defaults.get("table_properties")
        if isinstance(default_props, dict) and default_props:
            filtered_props = {k: v for k, v in deserialized_props.items() if k not in default_props}
        else:
            filtered_props = deserialized_props

        # When syncing from remote, scope properties based on table_property_mode
        if remote_catalog_config is not None:
            filtered_props = cls._filter_properties_by_mode(
                filtered_props, model, remote_catalog_config
            )

        cls._set_or_remove(model, "table_properties", filtered_props)

        existing_columns = model.get("columns")
        if not isinstance(existing_columns, list):
            existing_columns = []

        model["columns"] = cls._patch_columns(existing_columns, source_model.columns)

    @classmethod
    def metric_view_to_model_dict(
        cls,
        source_metric_view: MetricView,
        include_hierarchy_defaults: bool = True,
    ) -> dict:
        """Convert a MetricView object to a YAML model dict.

        Args:
            source_metric_view: Metric view to convert to model dict.
            include_hierarchy_defaults: If True, applies project config hierarchy defaults
                to reduce redundant field values.

        Returns:
            Model dictionary with name and patchable fields. Empty values are excluded.
        """
        model = {"name": source_metric_view.name}

        defaults = {}
        if include_hierarchy_defaults and source_metric_view.origin_file_path:
            path_config = ServicePathConfig.from_context(
                service_root_key="metrics_path",
                hierarchy_config_key="metric_views",
            )

            defaults = cls._get_hierarchy_defaults(
                Path(source_metric_view.origin_file_path),
                path_config,
            )

        cls._patch_metric_view_dict(model, source_metric_view, defaults)
        return model

    @classmethod
    def _patch_metric_view_dict(
        cls,
        model: dict,
        source_metric_view: MetricView,
        defaults: dict,
    ) -> None:
        """Patch a single metric view dict in-place.

        Always updates fields from source. When a field has a default value in hierarchy config,
        only writes it to YAML if it differs from the default (to avoid redundancy).
        """
        # Only write catalog if it differs from default
        default_catalog = defaults.get("catalog")
        if source_metric_view.catalog != default_catalog:
            cls._set_or_remove(model, "catalog", source_metric_view.catalog)
        elif "catalog" in model:
            model.pop("catalog", None)

        # Only write schema if it differs from default
        default_schema = defaults.get("schema")
        if source_metric_view.schema_ != default_schema:
            cls._set_or_remove(model, "schema", source_metric_view.schema_)
        elif "schema" in model:
            model.pop("schema", None)

        # Only write description if it differs from default
        default_description = defaults.get("description")
        if source_metric_view.description != default_description:
            cls._set_or_remove(model, "description", source_metric_view.description)
        elif "description" in model:
            model.pop("description", None)

        # Filter tags to exclude those matching defaults
        filtered_tags = cls._filter_tags(source_metric_view.tags, defaults.get("tags"))
        cls._set_or_remove(model, "tags", filtered_tags)

        # Inject description as comment into definition if description exists
        definition = source_metric_view.definition.copy() if source_metric_view.definition else {}
        if source_metric_view.description and definition:
            definition = {"comment": source_metric_view.description, **definition}

        # Preserve existing source field from local model if present
        # (local source may contain variables like ${catalog}.${schema}.table)
        existing_definition = model.get("definition")
        if isinstance(existing_definition, dict) and "source" in existing_definition:
            # Keep the local source field, don't overwrite with remote rendered value
            definition["source"] = existing_definition["source"]

        # Definition is always written (no default handling)
        cls._set_or_remove(model, "definition", definition)

    @classmethod
    def _patch_columns(
        cls,
        existing_columns: list[dict],
        source_columns: list[Column],
    ) -> list[dict]:
        """Patch columns to match the source list and update allowed fields."""
        existing_by_name = {
            col.get("name"): col for col in existing_columns if isinstance(col, dict)
        }
        patched_columns: list[dict] = []
        for source_col in source_columns:
            col_dict = existing_by_name.get(source_col.name, {"name": source_col.name})
            cls._set_or_remove(col_dict, "description", source_col.description)
            cls._set_or_remove(col_dict, "data_type", source_col.data_type)
            cls._set_or_remove(col_dict, "tags", source_col.tags)
            patched_columns.append(col_dict)
        return patched_columns

    @classmethod
    def _filter_tags(cls, tags: dict[str, str], default_tags: dict | None) -> dict[str, str]:
        """Filter tags to exclude defaults from hierarchy config."""
        if not tags:
            return {}
        if not isinstance(default_tags, dict) or not default_tags:
            return tags
        return {key: value for key, value in tags.items() if key not in default_tags}

    @classmethod
    def _filter_properties_by_mode(
        cls,
        source_props: dict,
        existing_model: dict,
        config: RemoteCatalogConfig,
    ) -> dict:
        """Filter table properties based on the remote catalog config mode.

        Args:
            source_props: Deserialized properties from the source model.
            existing_model: Current YAML model dict (contains existing local properties).
            config: Remote catalog configuration with table_property_mode.

        Returns:
            Filtered properties dict.

            - append: Only keep properties whose keys already exist in the local model.
            - managed: Only keep properties whose keys are in managed_table_properties.
        """
        mode = config.table_property_mode
        existing_props = existing_model.get("table_properties")
        existing_keys = set(existing_props.keys()) if isinstance(existing_props, dict) else set()

        if mode == "append":
            return {k: v for k, v in source_props.items() if k in existing_keys}

        if mode == "managed":
            managed_keys = set(config.managed_table_properties)
            return {k: v for k, v in source_props.items() if k in managed_keys}

        return source_props

    @classmethod
    def _serialize_constraints(
        cls,
        constraints: list[PrimaryKeyConstraint | ForeignKeyConstraint],
    ) -> list[dict]:
        """Serialize constraints to YAML-friendly dicts."""
        result: list[dict] = []
        for constraint in constraints:
            if isinstance(constraint, PrimaryKeyConstraint):
                result.append(
                    constraint.model_dump(),
                    # {
                    #     "name": constraint.name,
                    #     "type": "primary_key",
                    #             "columns": list(constraint.columns),
                    # }
                )
            elif isinstance(constraint, ForeignKeyConstraint):
                result.append(
                    constraint.model_dump(),
                    #     {
                    #         "name": constraint.name,
                    #         "type": "foreign_key",
                    #                 "columns": list(constraint.columns),
                    #                 "reference_table": constraint.reference_table,
                    #                 "reference_columns": list(constraint.reference_columns),
                    #     }
                )
        return result

    @classmethod
    def _normalize_foreign_key_references(cls, constraints: list[dict]) -> list[dict]:
        """Extract unqualified FK reference names for cleaner YAML storage.

        When a referenced table FQN exists in local metadata, stores only the
        unqualified table name in YAML. The Model loader will resolve to FQN
        when creating the Model object, so consumers see resolved references
        without needing resolution logic.

        When a referenced table is NOT in local metadata, keeps the remote FQN value.
        """
        normalized: list[dict] = []
        for constraint in constraints:
            if not isinstance(constraint, dict):
                normalized.append(constraint)
                continue

            updated = copy.deepcopy(constraint)
            if updated.get("type") == "foreign_key":
                reference_table = updated.get("reference_table")
                if isinstance(reference_table, str):
                    updated["reference_table"] = cls._extract_unqualified_reference_name(
                        reference_table
                    )
            normalized.append(updated)
        return normalized

    @classmethod
    def _preserve_local_foreign_key_variables(
        cls,
        new_constraints: list[dict],
        existing_constraints: list | None,
    ) -> list[dict]:
        """Preserve local templated FK reference_table values when patching.

        Mirrors metric view preservation logic: if local YAML already has a
        templated reference (for example ``${ catalog }.schema.table``), keep it
        instead of overwriting with rendered remote values.
        """
        if not isinstance(existing_constraints, list):
            return new_constraints

        existing_fk_by_name: dict[str, str] = {}
        for constraint in existing_constraints:
            if not isinstance(constraint, dict):
                continue
            if constraint.get("type") != "foreign_key":
                continue
            name = constraint.get("name")
            reference_table = constraint.get("reference_table")
            if isinstance(name, str) and isinstance(reference_table, str):
                existing_fk_by_name[name] = reference_table

        patched_constraints: list[dict] = []
        for constraint in new_constraints:
            if not isinstance(constraint, dict):
                patched_constraints.append(constraint)
                continue

            updated = copy.deepcopy(constraint)
            if updated.get("type") == "foreign_key":
                name = updated.get("name")
                if isinstance(name, str):
                    local_reference = existing_fk_by_name.get(name)
                    if isinstance(local_reference, str) and "${" in local_reference:
                        updated["reference_table"] = local_reference

            patched_constraints.append(updated)

        return patched_constraints

    @classmethod
    def _extract_unqualified_reference_name(cls, reference_table: str) -> str:
        """Extract unqualified table name if table exists in local catalog.

        Args:
            reference_table: Referenced table name, optionally qualified.

        Returns:
            Unqualified table name if table exists in local metadata,
            otherwise the original FQN value (for external tables).
        """
        if "." not in reference_table:
            return reference_table

        reference_name = reference_table.split(".")[-1]
        try:
            ctx = get_context(init=False)
        except Exception:  # noqa: BLE001
            return reference_table

        try:
            ctx.catalog_index.get("models", reference_name)
            # Table found in local catalog, store unqualified name in YAML
            return reference_name
        except Exception:  # noqa: BLE001
            # Not in local catalog, keep the external FQN
            return reference_table

    @classmethod
    def _detect_changes(
        cls,
        original: dict | None,
        updated: dict,
    ) -> tuple[list[str], list[str], list[str]]:
        """Detect added, updated, and removed fields between original and updated models.

        Returns:
            Tuple of (added_fields, updated_fields, removed_fields)
        """
        if original is None:
            # All fields in updated are new
            return list(updated.keys()), [], []

        added = []
        updated_list = []
        removed = []

        # Find added and updated fields
        for key, value in updated.items():
            if key not in original:
                added.append(key)
            elif original[key] != value:
                updated_list.append(key)

        # Find removed fields
        removed = [key for key in original if key not in updated]

        return added, updated_list, removed

    @classmethod
    def _set_or_remove(cls, target: dict, key: str, value) -> None:
        """Set a key if value is meaningful, otherwise remove it."""
        if value in (None, ""):
            target.pop(key, None)
            return
        if isinstance(value, (list, dict)) and not value:
            target.pop(key, None)
            return
        target[key] = value

    @classmethod
    def _find_model_index(cls, models: list[dict], name: str) -> int | None:
        """Find a model index by name."""
        for index, model in enumerate(models):
            if isinstance(model, dict) and model.get("name") == name:
                return index
        return None

    @classmethod
    def _resolve_or_determine_path(
        cls,
        *,
        name: str,
        origin_file_path: str | None,
        schema: str | None,
        catalog: str | None,
        path_config: ServicePathConfig,
        explicit_path: str | Path | None,
        kind: str,
    ) -> Path:
        """Return the file path, using provided path, origin file, or hierarchy defaults.

        Args:
            name: Object name (table or metric view).
            origin_file_path: Origin file path if available.
            schema: Schema of the object.
            catalog: Catalog of the object.
            path_config: Service path configuration with hierarchy settings.
            explicit_path: Optional explicit path override.
            kind: Human-readable object kind for logging.

        Returns:
            Relative path to file within service_root.
        """
        if explicit_path:
            return Path(explicit_path)

        if origin_file_path:
            # Strip service_root prefix from origin_file_path to avoid duplication
            # e.g., "models/silver/table.yml" -> "silver/table.yml"
            # or "kelp_metadata/models/silver/table.yml" -> "silver/table.yml"
            path_obj = Path(origin_file_path)

            # If absolute and under service_root_absolute, normalize to service-root-relative path.
            if path_obj.is_absolute():
                try:
                    return path_obj.relative_to(path_config.service_root_absolute)
                except ValueError:
                    return path_obj

            # For relative origin paths, support both:
            # - full service root prefix: kelp_metadata/models/...
            # - service root basename prefix: models/...
            prefixes: list[Path] = [path_config.service_root]
            if path_config.service_root.name:
                prefixes.append(Path(path_config.service_root.name))

            for prefix in prefixes:
                try:
                    return path_obj.relative_to(prefix)
                except ValueError:
                    continue

            # origin_file_path doesn't start with known prefixes, use as-is
            return path_obj

        return cls._determine_new_file_path_common(
            name=name,
            schema=schema,
            catalog=catalog,
            path_config=path_config,
            kind=kind,
        )

    @classmethod
    def _determine_new_file_path_common(
        cls,
        *,
        name: str,
        schema: str | None,
        catalog: str | None,
        path_config: ServicePathConfig,
        kind: str,
    ) -> Path:
        """Determine the correct folder path for a new object based on hierarchy config.

        Args:
            name: Object name (table or metric view).
            schema: Schema of the object.
            catalog: Catalog of the object.
            path_config: Service path configuration with hierarchy settings.
            kind: Human-readable object kind for logging.

        Returns:
            Relative path to file within service_root.
        """
        folder_key = cls._find_hierarchy_folder_for_schema(
            schema,
            catalog,
            path_config.hierarchy_config or {},
        )

        if not folder_key:
            logger.debug(
                "Could not determine folder for %s %s (schema=%s, catalog=%s). "
                "Writing to root service path.",
                kind,
                name,
                schema,
                catalog,
            )
            folder_key = ""

        file_path = Path(folder_key) / f"{name}.yml" if folder_key else Path(f"{name}.yml")

        return file_path

    @classmethod
    def _find_hierarchy_folder_for_schema(
        cls,
        schema: str | None,
        catalog: str | None,
        models_cfg: dict,
    ) -> str | None:
        """Find the folder path in models_cfg that applies this schema/catalog pair.

        Returns the full path (e.g., 'bronze', 'bronze/sub_bronze') for nested hierarchies.
        Recursively searches through nested folder structures.
        Takes into account top-level +schema and +catalog defaults.
        """
        if not models_cfg:
            return None
        # Extract top-level defaults
        top_level_schema = models_cfg.get("+schema")
        top_level_catalog = models_cfg.get("+catalog")
        return cls._search_hierarchy_recursive(
            schema,
            catalog,
            models_cfg,
            "",
            top_level_schema,
            top_level_catalog,
        )

    @classmethod
    def _search_hierarchy_recursive(
        cls,
        schema: str | None,
        catalog: str | None,
        config: dict,
        path_prefix: str,
        inherited_schema: str | None = None,
        inherited_catalog: str | None = None,
    ) -> str | None:
        """Recursively search through folder hierarchy for matching schema/catalog.

        Args:
            schema: Target schema from table
            catalog: Target catalog from table
            config: Current config dict to search
            path_prefix: Current folder path being built
            inherited_schema: Schema inherited from parent/top-level
            inherited_catalog: Catalog inherited from parent/top-level
        """
        if not config or not isinstance(config, dict):
            return None

        for key, value in config.items():
            # Skip top-level keys like +catalog, +tags, +schema
            if isinstance(key, str) and key.startswith("+"):
                continue

            # Only process dict values (folder definitions)
            if not isinstance(value, dict):
                continue

            # Build the current path
            current_path = f"{path_prefix}/{key}" if path_prefix else key

            # Get folder-level schema/catalog (may be None, inheriting from parent)
            folder_schema = value.get("+schema")
            folder_catalog = value.get("+catalog")

            # Use inherited values if folder doesn't override
            effective_schema = folder_schema if folder_schema is not None else inherited_schema
            effective_catalog = folder_catalog if folder_catalog is not None else inherited_catalog

            # Check if this folder matches
            schema_matches = effective_schema == schema
            catalog_matches = effective_catalog == catalog or effective_catalog is None

            if schema_matches and catalog_matches:
                return current_path

            # Recursively search nested folders, passing down effective defaults
            nested_path = cls._search_hierarchy_recursive(
                schema,
                catalog,
                value,
                current_path,
                effective_schema,
                effective_catalog,
            )
            if nested_path:
                return nested_path

        return None

    @classmethod
    def _get_hierarchy_defaults(cls, file_path: Path, path_config: ServicePathConfig) -> dict:
        """Get defaults applied via project config hierarchy for a model path.

        Args:
            file_path: File path (relative to service root).
            path_config: Service path configuration with hierarchy config.

        Returns:
            Dictionary of hierarchy defaults for the file path.
        """
        if not path_config.hierarchy_config:
            return {}

        # Use the provided file_path (relative to service root) for hierarchy lookup
        # This ensures correct hierarchy defaults based on folder structure
        defaults: dict = {}
        return apply_cfg_hierarchy_to_dict_recursive(
            defaults,
            path_config.hierarchy_config,
            tpl_path=str(file_path),
        )

    @classmethod
    def _load_yaml_document(cls, file_path: Path) -> dict:
        """Load a YAML file if it exists, otherwise return a new document."""
        abs_path = cls._to_absolute_path(file_path)
        if not abs_path.exists():
            return {}
        text = abs_path.read_text(encoding="utf-8")
        return yaml.safe_load(text) or {}

    @classmethod
    def _write_yaml_document(cls, file_path: Path, document: dict) -> None:
        """Write the YAML document to disk."""
        abs_path = cls._to_absolute_path(file_path)
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        yaml_text = yaml.safe_dump(
            document, sort_keys=False, default_flow_style=False, allow_unicode=True
        )
        abs_path.write_text(yaml_text, encoding="utf-8")

    @classmethod
    def _to_absolute_path(cls, file_path: Path) -> Path:
        """Convert a path to absolute, using project root if relative."""
        if file_path.is_absolute():
            return file_path

        ctx = get_context()
        if ctx and ctx.project_root:
            return Path(ctx.project_root).joinpath(file_path).resolve()

        return file_path.resolve()
