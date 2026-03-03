"""Tests for the functions+ABAC fixture project."""

from pathlib import Path

from kelp.config import init


def test_functions_abacs_fixture_loads(functions_abacs_project_dir: Path) -> None:
    """Fixture project should load models, functions, and ABACs with resolved vars."""
    project_file = functions_abacs_project_dir / "kelp_project.yml"

    ctx = init(project_root=str(project_file))

    customers = ctx.catalog.get_table("customers")
    normalize_email = ctx.catalog.get_function("normalize_email")
    mask_ssn = ctx.catalog.get_function("mask_ssn")
    format_greeting = ctx.catalog.get_function("format_greeting")
    policy = ctx.catalog.get_abac("mask_ssn_policy")

    assert customers.catalog == "dev_data_catalog"
    assert customers.schema_ == "dev_data_schema"

    assert normalize_email.catalog == "dev_security_catalog"
    assert normalize_email.schema_ == "dev_security_schema"
    assert "lower(trim(email))" in normalize_email.body

    assert mask_ssn.catalog == "dev_security_catalog"
    assert mask_ssn.schema_ == "dev_security_schema"
    assert "XXX-XX-" in mask_ssn.body

    assert format_greeting.catalog == "dev_security_catalog"
    assert format_greeting.schema_ == "dev_security_schema"
    assert "    if name:" in format_greeting.body
    assert "    return None" in format_greeting.body
    assert "return format_greeting(name)" in format_greeting.body

    assert policy.securable_name == "dev_data_catalog.dev_data_schema.customers"
    assert policy.udf_name == "dev_security_catalog.dev_security_schema.mask_ssn"
