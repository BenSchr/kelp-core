from typing import Annotated

import typer


def _resolve_target(target: str | None) -> str | None:
    """Resolve a target from settings when not provided."""
    from kelp.config.settings import resolve_setting

    return target or resolve_setting("target", default=None)


def check_policies(
    config_path: Annotated[
        str | None,
        typer.Option("-c", help="Path to the kelp_project.yml"),
    ] = None,
    target: Annotated[str | None, typer.Option(help="Environment to check against")] = None,
    severity_filter: Annotated[
        str | None,
        typer.Option(
            "--severity",
            help="Only show violations at this severity level or above: 'warn' or 'error'",
        ),
    ] = None,
    fail_on: Annotated[
        str,
        typer.Option(
            help="Exit with code 1 when violations of this severity are found: 'warn' or 'error'",
        ),
    ] = "error",
    fast_exit: Annotated[
        bool | None,
        typer.Option(
            "--fast-exit/--no-fast-exit",
            help=(
                "Stop policy evaluation on first violating policy per model. "
                "Defaults to policy_config.fast_exit when not provided."
            ),
        ),
    ] = None,
    debug: Annotated[bool, typer.Option(help="Debug mode")] = False,
) -> None:
    """Check metadata governance policies for the Kelp project.

    Evaluates all configured policy rules against the loaded table catalog and
    reports any violations. Useful for CI/CD gating and governance audits.
    """
    from kelp.cli.output import print_error, print_message, print_success, print_warning
    from kelp.config import init
    from kelp.models.model import Model
    from kelp.models.policy import PolicySeverity
    from kelp.models.policy_definition import Policy
    from kelp.service.policy_manager import PolicyManager

    log_level = "DEBUG" if debug else None
    resolved_target = _resolve_target(target)

    # Initialize context WITHOUT running policy checks (we run them manually here
    # so we can control output and exit code).
    ctx = init(config_path, resolved_target, run_policy_checks=False, log_level=log_level)
    policy_cfg = ctx.project_settings.policy_config

    if not policy_cfg.enabled:
        print_warning(
            "⚠ Policy checks are disabled. "
            "Set 'policy_config.enabled: true' in kelp_project.yml to enable."
        )

    models: list[Model] = ctx.catalog_index.get_all("models")
    policies: list[Policy] = ctx.catalog_index.get_all("policies") or []
    if not policies:
        print_warning(
            "⚠ Policy checks are enabled but no policy files were loaded. "
            "Add policy YAML files under `policies_path` with `kelp_policies` definitions."
        )
        return

    manager = PolicyManager()
    effective_fast_exit = policy_cfg.fast_exit if fast_exit is None else fast_exit
    all_violations = manager.check_catalog(models, policies, fast_exit=effective_fast_exit)

    # Apply severity filter
    if severity_filter:
        try:
            min_severity = PolicySeverity(severity_filter)
        except ValueError:
            print_error(f"Invalid severity filter '{severity_filter}'. Use 'warn' or 'error'.")
            raise typer.Exit(code=2) from None

        if min_severity == PolicySeverity.error:
            all_violations = [v for v in all_violations if v.severity == PolicySeverity.error]

    if not all_violations:
        print_success(
            f"✓ Policy check complete: no violations found ({len(models)} model(s) checked)."
        )
        return

    # Display violations
    warn_count = sum(1 for v in all_violations if v.severity == PolicySeverity.warn)
    error_count = sum(1 for v in all_violations if v.severity == PolicySeverity.error)

    for violation in all_violations:
        location = violation.model_name
        if violation.column_name:
            location = f"{violation.model_name}.{violation.column_name}"

        if violation.severity == PolicySeverity.error:
            print_error(f"✗ [ERROR] {location} — {violation.message}")
        else:
            print_warning(f"⚠ [WARN]  {location} — {violation.message}")

    print_message("")
    print_message(
        f"Policy check complete: {error_count} error(s), {warn_count} warning(s) "
        f"across {len(models)} model(s)."
    )

    # Decide exit code
    try:
        fail_severity = PolicySeverity(fail_on)
    except ValueError:
        print_error(f"Invalid --fail-on value '{fail_on}'. Use 'warn' or 'error'.")
        raise typer.Exit(code=2) from None

    should_fail = (fail_severity == PolicySeverity.error and error_count > 0) or (
        fail_severity == PolicySeverity.warn and (error_count + warn_count) > 0
    )

    if should_fail:
        raise typer.Exit(code=1)
