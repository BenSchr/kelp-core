"""Tests for kelp.models.abac module."""

from kelp.models.abac import AbacMatchColumn, AbacPolicy


def test_create_minimal_abac_policy() -> None:
    """Create ABAC policy with minimal required fields."""
    policy = AbacPolicy(
        name="mask_ssn",
        securable_type="SCHEMA",
        securable_name="main.hr",
        mode="COLUMN_MASK",
        udf_name="main.security.mask_ssn",
        target_column="ssn",
    )

    assert policy.name == "mask_ssn"
    assert policy.mode == "COLUMN_MASK"
    assert policy.target_column == "ssn"


def test_abac_policy_match_columns_and_principals() -> None:
    """Create ABAC policy with principals and MATCH COLUMNS clauses."""
    policy = AbacPolicy(
        name="hide_eu",
        securable_type="TABLE",
        securable_name="main.sales.customers",
        mode="ROW_FILTER",
        udf_name="main.security.non_eu_region",
        principals_to=["analysts"],
        principals_except=["admins"],
        match_columns=[AbacMatchColumn(condition="hasTag('geo_region')", alias="region")],
        using_columns=["region"],
    )

    assert policy.principals_to == ["analysts"]
    assert policy.principals_except == ["admins"]
    assert policy.match_columns[0].alias == "region"
