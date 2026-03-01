"""Tests for ABAC policy DDL generation."""

from kelp.catalog.abac_ddl import generate_create_abac_policy_ddl
from kelp.models.abac import AbacMatchColumn, AbacPolicy


def test_generate_create_row_filter_policy_ddl() -> None:
    """Generate CREATE policy SQL for row filter mode."""
    policy = AbacPolicy(
        name="hide_eu_customers",
        securable_type="SCHEMA",
        securable_name="main.sales",
        description="Hide EU customers",
        mode="ROW_FILTER",
        udf_name="main.security.non_eu_region",
        principals_to=["analysts"],
        match_columns=[AbacMatchColumn(condition="hasTag('geo_region')", alias="region")],
        using_columns=["region"],
    )

    ddl = generate_create_abac_policy_ddl(policy)

    assert "CREATE POLICY hide_eu_customers" in ddl
    assert "CREATE OR REPLACE POLICY" not in ddl
    assert "ON SCHEMA main.sales" in ddl
    assert "ROW FILTER main.security.non_eu_region" in ddl
    assert "TO `analysts`" in ddl
    assert "MATCH COLUMNS hasTag('geo_region') AS region" in ddl
    assert "USING COLUMNS (region)" in ddl


def test_generate_create_column_mask_policy_ddl() -> None:
    """Generate CREATE policy SQL for column mask mode."""
    policy = AbacPolicy(
        name="mask_ssn",
        securable_type="TABLE",
        securable_name="main.hr.employees",
        mode="COLUMN_MASK",
        udf_name="main.security.mask_ssn",
        target_column="ssn",
        principals_to=["analysts"],
        principals_except=["admins"],
    )

    ddl = generate_create_abac_policy_ddl(policy)

    assert "CREATE OR REPLACE POLICY mask_ssn" in ddl
    assert "COLUMN MASK main.security.mask_ssn" in ddl
    assert "ON COLUMN ssn" in ddl
    assert "COLUMN MASK main.security.mask_ssn ON COLUMN ssn" not in ddl
    assert "TO `analysts`" in ddl
    assert "EXCEPT `admins`" in ddl
