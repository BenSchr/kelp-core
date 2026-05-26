"""ODCS (Open Data Contract Standard) CLI subcommand group.

Provides `kelp odcs import` and `kelp odcs export` commands for converting
between data contracts and kelp metadata.
"""

from pathlib import Path
from typing import Annotated

import yaml
from typer import Argument, Option, Typer

from kelp.cli.common_params import (
    debug_option,
    dry_run_option,
    kelp_project_path_option,
    manifest_file_path_option,
    target_option,
)

odcs_app = Typer(
    name="odcs",
    help="Open Data Contract Standard (ODCS) import and export.",
    no_args_is_help=True,
)


def _resolve_target(target: str | None) -> str | None:
    """Resolve a target from settings when not provided."""
    from kelp.config.settings import resolve_setting

    return target or resolve_setting("target", default=None)


def _check_datacontract_installed() -> None:
    """Check that datacontract-cli is installed, raise helpful error if not."""
    try:
        import datacontract  # noqa: F401
    except ImportError as e:
        raise SystemExit(
            "datacontract-cli is required for ODCS commands. "
            "Install with: pip install 'datacontract-cli'"
        ) from e


def _check_dqx_installed() -> None:
    """Check that databricks-labs-dqx[datacontract] is installed."""
    try:
        import databricks.labs.dqx  # noqa: F401
    except ImportError as e:
        raise SystemExit(
            "databricks-labs-dqx[datacontract] is required for DQX rule generation. "
            "Install with: pip install 'databricks-labs-dqx[datacontract]'"
        ) from e


@odcs_app.command("import")
def import_contract(
    source: Annotated[Path, Argument(help="Path to the data contract YAML file.")],
    output: Annotated[
        Path | None, Option("--output", "-o", help="Output file path. Prints to stdout if omitted.")
    ] = None,
    generate_dqx_rules: Annotated[
        bool, Option("--generate-dqx-rules", help="Generate DQX quality rules from the contract.")
    ] = False,
    patch: Annotated[
        bool,
        Option(
            "--patch", help="Patch existing Kelp model YAML files instead of printing all models."
        ),
    ] = False,
    dry_run: dry_run_option = False,
    project_file_path: kelp_project_path_option = None,
    target: target_option = None,
    manifest_file_path: manifest_file_path_option = None,
    debug: debug_option = False,
) -> None:
    """Import a data contract into kelp metadata format."""
    _check_datacontract_installed()

    from datacontract.data_contract import DataContract
    from datacontract.export.exporter_factory import exporter_factory

    from kelp.integration.odcs.custom_exporter import KelpExporter
    from kelp.models.model import Model

    exporter_factory.register_exporter("kelp", KelpExporter)  # ty: ignore[invalid-argument-type]

    from kelp.utils.logging import configure_logging

    log_level = "DEBUG" if debug else None
    if log_level:
        configure_logging(log_level)

    if not source.exists():
        raise SystemExit(f"File not found: {source}")

    if generate_dqx_rules:
        _check_dqx_installed()

    export_kwargs = {
        "generate_dqx_rules": generate_dqx_rules,
        "data_contract_file": str(source),
    }

    data_contract = DataContract(data_contract_file=str(source))
    result_raw = data_contract.export(
        export_format="kelp",  # ty: ignore[invalid-argument-type]
        **export_kwargs,  # ty: ignore[invalid-argument-type]
    )
    result_list: list[Model] = result_raw  # ty: ignore[invalid-assignment]

    if patch:
        from kelp.config import get_context, init
        from kelp.service.yaml_manager import YamlManager

        resolved_target = _resolve_target(target)
        init(
            project_file_path=project_file_path,
            target=resolved_target,
            manifest_file_path=manifest_file_path,
            refresh=True,
        )

        ctx = get_context()
        local_models_by_name = ctx.catalog_index.get_index("models")

        reports = []
        for model in result_list:
            local_model = local_models_by_name.get(model.name)
            if local_model is not None and local_model.origin_file_path:
                model.origin_file_path = local_model.origin_file_path
            reports.append(YamlManager.patch_model_yaml(model, dry_run=dry_run))

        changed_count = sum(1 for report in reports if report.changes_made)
        for report in reports:
            if report.changes_made:
                if dry_run:
                    print(f"  would patch: {report.model_name} -> {report.file_path}")
                else:
                    print(f"  patched: {report.model_name} -> {report.file_path}")
        if dry_run:
            print(f"Dry-run: {changed_count} model YAML file(s) would be patched.")
        else:
            print(f"Patched {changed_count} model YAML file(s).")
        return

    result_dict = {
        "kelp_models": [
            r.model_dump(exclude_defaults=True, exclude_none=True, by_alias=True)
            for r in result_list
        ]
    }

    yaml_result = yaml.safe_dump(result_dict, sort_keys=False, allow_unicode=True)

    if output:
        output.write_text(yaml_result, encoding="utf-8")
    else:
        print(yaml_result)


@odcs_app.command("export")
def export_contract(
    model: Annotated[str, Argument(help="Name of the kelp model to export.")],
    output: Annotated[
        Path | None, Option("--output", "-o", help="Output file path. Prints to stdout if omitted.")
    ] = None,
    project_file_path: kelp_project_path_option = None,
    target: target_option = None,
    manifest_file_path: manifest_file_path_option = None,
    debug: debug_option = False,
    include_server: Annotated[
        bool,
        Option(
            "--include-server",
            help="Include ODCS server (database/catalog and schema) when model contains catalog and schema.",
        ),
    ] = False,
    patch: Annotated[
        bool,
        Option(
            "--patch",
            help="Patch an existing contract YAML file, updating only the matching schema.",
        ),
    ] = False,
    dry_run: dry_run_option = False,
    contract_file: Annotated[
        Path | None,
        Option(
            "--contract-file",
            help="Existing contract YAML file to patch when --patch is used.",
        ),
    ] = None,
) -> None:
    """Export a kelp model to Open Data Contract Standard format."""
    _check_datacontract_installed()

    from kelp.integration.odcs import patch_contract_yaml_document
    from kelp.integration.odcs.custom_importer import KelpImporter
    from kelp.utils.logging import configure_logging

    log_level = "DEBUG" if debug else None
    if log_level:
        configure_logging(log_level)

    from kelp.config import init

    resolved_target = _resolve_target(target)
    ctx = init(
        project_file_path=project_file_path,
        target=resolved_target,
        manifest_file_path=manifest_file_path,
        refresh=True,
    )
    catalog = ctx.catalog

    model_obj = next((m for m in catalog.get("models", []) if m.name == model), None)
    if model_obj is None:
        raise SystemExit(f"Model '{model}' not found in kelp catalog.")

    import_args = {
        "name": model,
        "include_server": include_server,
    }

    importer = KelpImporter(import_format="kelp")
    contract = importer.import_source(source=model_obj, import_args=import_args)

    if patch:
        patch_source = contract_file or output
        if patch_source is None:
            raise SystemExit(
                "--patch requires --contract-file or --output pointing to an existing YAML"
            )
        if not patch_source.exists():
            raise SystemExit(f"Contract file not found for patching: {patch_source}")

        existing_text = patch_source.read_text(encoding="utf-8")
        existing_doc = yaml.safe_load(existing_text) or {}
        incoming_doc = yaml.safe_load(contract.to_yaml()) or {}
        patched_doc = patch_contract_yaml_document(existing_doc, incoming_doc)
        patched_yaml = yaml.safe_dump(patched_doc, sort_keys=False, allow_unicode=True)

        if dry_run:
            print(patched_yaml)
        elif output:
            output.write_text(patched_yaml, encoding="utf-8")
        else:
            patch_source.write_text(patched_yaml, encoding="utf-8")
        return

    result = contract.to_yaml()

    if output:
        output.write_text(result, encoding="utf-8")
    else:
        print(result)
