from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
import yaml

from kelp.config import get_context
from kelp.models.table import Column, ForeignKeyConstraint, PrimaryKeyConstraint, Table
from kelp.utils.dict_parser import apply_cfg_hierarchy_to_dict_recursive
import logging

logger = logging.getLogger(__name__)


@dataclass
class YamlUpdateReport:
    """Report summarizing the changes made during a YAML patch operation."""

    table_name: str
    file_path: Path
    result_model: dict
    changes_made: bool
    added_fields: list[str]
    updated_fields: list[str]
    removed_fields: list[str]


class YamlManager:
    """Service for writing and patching table YAML files."""

    @classmethod
    def patch_table_yaml(
        cls,
        source_table: Table,
        *,
        file_path: str | Path | None = None,
    ) -> YamlUpdateReport:
        """Patch or create a model YAML file with metadata from a source table.

        If file_path is provided, uses that.
        If table has origin_file_path, uses that (without resolving to absolute).
        Otherwise, determines the correct folder from hierarchy config and creates a new file.

        This updates only a constrained set of attributes:
        - table: description, tags, constraints
        - columns: add/remove, description, data_type, tags

        Args:
                source_table: Table sourced from Databricks metadata.
                file_path: Optional explicit path to the YAML file to patch/create.

        Returns:
                YamlUpdateReport with details about changes made.
        """

        resolved_file_path = cls._resolve_or_determine_file_path(source_table, file_path)
        if file_path is None:
            logger.debug(
                f"Determined file path for table {source_table.name}: {resolved_file_path}"
            )
        document = cls._load_yaml_document(resolved_file_path)
        models = document.get("kelp_models")
        if not isinstance(models, list):
            models = []

        model_name = source_table.name
        model_index = cls._find_model_index(models, model_name)
        if model_index is None:
            model = {"name": model_name}
            models.append(model)
            original_model = None
        else:
            model = models[model_index]
            # Keep a deep copy of original state for change detection
            original_model = copy.deepcopy(model)

        defaults = cls._get_hierarchy_defaults(source_table, resolved_file_path)
        cls._patch_model_dict(model, source_table, defaults)

        # Detect changes
        changes_made = original_model is None or model != original_model
        added_fields, updated_fields, removed_fields = cls._detect_changes(original_model, model)

        # Only write if something changed
        if changes_made:
            document["kelp_models"] = models
            logger.debug(
                f"Writing updated YAML for table {source_table.name} to {resolved_file_path}"
            )
            cls._write_yaml_document(resolved_file_path, document)

        return YamlUpdateReport(
            table_name=source_table.name,
            file_path=resolved_file_path,
            result_model=model,
            changes_made=changes_made,
            added_fields=added_fields,
            updated_fields=updated_fields,
            removed_fields=removed_fields,
        )

    @classmethod
    def _patch_model_dict(cls, model: dict, source_table: Table, defaults: dict) -> None:
        """Patch a single model dict in-place."""
        if "description" not in defaults:
            cls._set_or_remove(model, "description", source_table.description)
        if "tags" not in defaults:
            filtered_tags = cls._filter_tags(source_table.tags, defaults.get("tags"))
            cls._set_or_remove(model, "tags", filtered_tags)
        if "constraints" not in defaults:
            cls._set_or_remove(
                model, "constraints", cls._serialize_constraints(source_table.constraints)
            )

        existing_columns = model.get("columns")
        if not isinstance(existing_columns, list):
            existing_columns = []

        model["columns"] = cls._patch_columns(existing_columns, source_table.columns)

    @classmethod
    def _patch_columns(
        cls, existing_columns: list[dict], source_columns: list[Column]
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
    def _serialize_constraints(
        cls, constraints: list[PrimaryKeyConstraint | ForeignKeyConstraint]
    ) -> list[dict]:
        """Serialize constraints to YAML-friendly dicts."""
        result: list[dict] = []
        for constraint in constraints:
            if isinstance(constraint, PrimaryKeyConstraint):
                result.append(
                    constraint.model_dump()
                    # {
                    #     "name": constraint.name,
                    #     "type": "primary_key",
                    #             "columns": list(constraint.columns),
                    # }
                )
            elif isinstance(constraint, ForeignKeyConstraint):
                result.append(
                    constraint.model_dump()
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
    def _detect_changes(
        cls, original: dict | None, updated: dict
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
        for key in original:
            if key not in updated:
                removed.append(key)

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
    def _resolve_or_determine_file_path(
        cls, source_table: Table, file_path: str | Path | None
    ) -> Path:
        """Return the file path, using provided path, origin_file_path, or hierarchy determination."""
        # If explicit file_path provided, use it
        if file_path:
            return Path(file_path)

        # If table has origin_file_path, use it (don't resolve to absolute)
        if source_table.origin_file_path:
            return Path(source_table.origin_file_path)

        # Determine from hierarchy
        return cls._determine_new_file_path(source_table)

    @classmethod
    def _determine_new_file_path(cls, source_table: Table) -> Path:
        """Determine the correct folder path for a new table based on hierarchy config."""
        try:
            ctx = get_context()
        except Exception:
            raise ValueError("Cannot determine file path without runtime context")

        if not ctx.project_root or not ctx.project_config:
            raise ValueError(
                "Project root and config required to determine file path for new table"
            )

        # Map the table's schema/catalog to the hierarchy folder
        folder_key = cls._find_hierarchy_folder_for_schema(
            source_table.schema_, source_table.catalog, ctx.project_config.models
        )

        if not folder_key:
            logger.debug(
                f"Could not determine folder for table {source_table.name} "
                f"(schema={source_table.schema_}, catalog={source_table.catalog}). "
                f"Writing to root models path."
            )
            folder_key = ""

        # Construct the path: {project_root}/{metadata_paths[0]}/models/{folder_key}/{table_name}.yml
        metadata_paths = ctx.project_config.metadata_paths
        if not metadata_paths:
            metadata_paths = ["kelp_models"]

        base_path = Path(ctx.project_root) / metadata_paths[0]
        if folder_key:
            file_path = base_path / folder_key / f"{source_table.name}.yml"
        else:
            file_path = base_path / f"{source_table.name}.yml"

        return file_path

    @classmethod
    def _find_hierarchy_folder_for_schema(
        cls, schema: str | None, catalog: str | None, models_cfg: dict
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
            schema, catalog, models_cfg, "", top_level_schema, top_level_catalog
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
                schema, catalog, value, current_path, effective_schema, effective_catalog
            )
            if nested_path:
                return nested_path

        return None

    @classmethod
    def _get_hierarchy_defaults(cls, source_table: Table, file_path: Path) -> dict:
        """Get defaults applied via project config hierarchy for a model path."""
        try:
            ctx = get_context()
        except Exception:
            return {}

        models_cfg = ctx.project_config.models if ctx and ctx.project_config else None
        if not models_cfg:
            return {}

        candidate = source_table.origin_file_path or str(file_path)
        tpl_path = Path(candidate)
        if ctx.project_root and tpl_path.is_absolute():
            try:
                tpl_path = tpl_path.relative_to(Path(ctx.project_root))
            except ValueError:
                pass

        defaults: dict = {}
        return apply_cfg_hierarchy_to_dict_recursive(defaults, models_cfg, tpl_path=str(tpl_path))

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
        yaml_text = yaml.safe_dump(document, sort_keys=False, default_flow_style=False)
        abs_path.write_text(yaml_text, encoding="utf-8")

    @classmethod
    def _to_absolute_path(cls, file_path: Path) -> Path:
        """Convert a path to absolute, using project root if relative."""
        if file_path.is_absolute():
            return file_path

        try:
            ctx = get_context()
            if ctx and ctx.project_root:
                return Path(ctx.project_root).joinpath(file_path)
        except Exception:
            pass

        return file_path.resolve()
