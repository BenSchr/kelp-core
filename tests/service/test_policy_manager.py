"""Tests for PolicyManager service."""

from __future__ import annotations

from kelp.models.model import Column, PrimaryKeyConstraint, SDPQuality
from kelp.models.model import Model as Table
from kelp.models.policy import (
    ColumnPolicyRule,
    ModelPolicyRule,
    PolicySeverity,
)
from kelp.models.policy_definition import Policy
from kelp.service.policy_manager import PolicyManager


def _make_table(
    name: str = "test_table",
    description: str | None = None,
    tags: dict | None = None,
    columns: list[Column] | None = None,
    constraints: list | None = None,
    origin_file_path: str | None = "models/test/test_table.yml",
    table_properties: dict | None = None,
    quality=None,
) -> Table:
    return Table(
        name=name,
        description=description,
        tags=tags or {},
        columns=columns or [],
        constraints=constraints or [],
        origin_file_path=origin_file_path,
        table_properties=table_properties or {},
        quality=quality,
    )


def _make_column(
    name: str = "col1",
    description: str | None = None,
    tags: dict | None = None,
) -> Column:
    return Column(name=name, description=description, tags=tags or {})


def _make_policy(
    name: str = "default_policy",
    applies_to: str | None = "models/*",
    table: ModelPolicyRule | None = None,
    column: ColumnPolicyRule | None = None,
) -> Policy:
    return Policy(
        name=name,
        applies_to=applies_to,
        model=table or ModelPolicyRule(),
        column=column or ColumnPolicyRule(),
    )


class TestPolicyManagerNoMatchingPolicy:
    def test_returns_empty_when_no_policy_matches(self) -> None:
        manager = PolicyManager()
        table = _make_table(origin_file_path="models/silver/orders.yml")
        policies = [_make_policy(applies_to="models/bronze/*")]

        assert manager.check_model(table, policies) == []
        assert manager.check_catalog([table], policies) == []

    def test_returns_empty_when_policy_list_empty(self) -> None:
        manager = PolicyManager()
        table = _make_table()
        assert manager.check_model(table, []) == []
        assert manager.check_catalog([table], []) == []


class TestTablePolicyRequireDescription:
    def setup_method(self) -> None:
        self.manager = PolicyManager()
        self.policies = [
            _make_policy(
                table=ModelPolicyRule(require_description=True, severity=PolicySeverity.warn)
            )
        ]

    def test_no_violation_when_description_present(self) -> None:
        table = _make_table(description="A valid description")
        assert self.manager.check_model(table, self.policies) == []

    def test_violation_when_description_missing(self) -> None:
        table = _make_table(description=None)
        violations = self.manager.check_model(table, self.policies)
        assert len(violations) == 1
        assert violations[0].rule == "require_description"
        assert violations[0].severity == PolicySeverity.warn
        assert violations[0].column_name is None


class TestTablePolicyRequireAnyTag:
    def setup_method(self) -> None:
        self.manager = PolicyManager()
        self.policies = [
            _make_policy(table=ModelPolicyRule(require_any_tag=True, severity=PolicySeverity.error))
        ]

    def test_no_violation_when_tags_present(self) -> None:
        table = _make_table(tags={"owner": "team"})
        assert self.manager.check_model(table, self.policies) == []

    def test_violation_when_no_tags(self) -> None:
        table = _make_table(tags={})
        violations = self.manager.check_model(table, self.policies)
        assert len(violations) == 1
        assert violations[0].rule == "require_any_tag"
        assert violations[0].severity == PolicySeverity.error


class TestTablePolicyRequireSpecificTags:
    def setup_method(self) -> None:
        self.manager = PolicyManager()
        self.policies = [_make_policy(table=ModelPolicyRule(require_tags=["owner", "domain"]))]

    def test_no_violation_when_all_required_tags_present(self) -> None:
        table = _make_table(tags={"owner": "team_a", "domain": "finance"})
        assert self.manager.check_model(table, self.policies) == []

    def test_violation_for_each_missing_tag(self) -> None:
        table = _make_table(tags={"owner": "team_a"})
        violations = self.manager.check_model(table, self.policies)
        assert len(violations) == 1
        assert violations[0].rule == "require_tags"
        assert "domain" in violations[0].message

    def test_violation_for_all_missing_tags(self) -> None:
        table = _make_table(tags={})
        violations = self.manager.check_model(table, self.policies)
        assert len(violations) == 2


class TestTablePolicyRequireConstraints:
    def setup_method(self) -> None:
        self.manager = PolicyManager()
        self.policies = [_make_policy(table=ModelPolicyRule(require_constraints=["primary_key"]))]

    def test_no_violation_when_primary_key_present(self) -> None:
        pk = PrimaryKeyConstraint(name="pk_test", columns=["id"])
        table = _make_table(constraints=[pk])
        assert self.manager.check_model(table, self.policies) == []

    def test_violation_when_no_constraints(self) -> None:
        table = _make_table(constraints=[])
        violations = self.manager.check_model(table, self.policies)
        assert len(violations) == 1
        assert violations[0].rule == "require_constraints"


class TestTablePolicyHasColumnsPositiveForm:
    """Test has_columns policy with positive form (required columns)."""

    def setup_method(self) -> None:
        self.manager = PolicyManager()

    def test_no_violation_when_all_required_columns_present(self) -> None:
        policies = [_make_policy(table=ModelPolicyRule(has_columns=["id", "name", "created_at"]))]
        columns = [
            _make_column(name="id"),
            _make_column(name="name"),
            _make_column(name="created_at"),
            _make_column(name="extra_column"),
        ]
        table = _make_table(columns=columns)
        assert self.manager.check_model(table, policies) == []

    def test_violation_when_required_column_missing(self) -> None:
        policies = [_make_policy(table=ModelPolicyRule(has_columns=["id", "name"]))]
        columns = [_make_column(name="id")]
        table = _make_table(columns=columns)
        violations = self.manager.check_model(table, policies)
        assert len(violations) == 1
        assert violations[0].rule == "has_columns"
        assert "name" in violations[0].message

    def test_violation_for_each_missing_column(self) -> None:
        policies = [_make_policy(table=ModelPolicyRule(has_columns=["id", "name", "email"]))]
        columns = [_make_column(name="id")]
        table = _make_table(columns=columns)
        violations = self.manager.check_model(table, policies)
        assert len(violations) == 2
        missing = {v.message for v in violations}
        assert any("name" in msg for msg in missing)
        assert any("email" in msg for msg in missing)


class TestTablePolicyHasColumnsNegationForm:
    """Test has_columns policy with negation form (forbidden columns)."""

    def setup_method(self) -> None:
        self.manager = PolicyManager()

    def test_no_violation_when_forbidden_columns_absent(self) -> None:
        policies = [
            _make_policy(
                table=ModelPolicyRule(has_columns=["temp", "debug", "test_column"], not_=True)
            )
        ]
        columns = [_make_column(name="id"), _make_column(name="name")]
        table = _make_table(columns=columns)
        assert self.manager.check_model(table, policies) == []

    def test_violation_when_forbidden_column_present(self) -> None:
        policies = [_make_policy(table=ModelPolicyRule(has_columns=["temp", "debug"], not_=True))]
        columns = [_make_column(name="id"), _make_column(name="temp")]
        table = _make_table(columns=columns)
        violations = self.manager.check_model(table, policies)
        assert len(violations) == 1
        assert violations[0].rule == "has_columns"
        assert "temp" in violations[0].message
        assert "must not have" in violations[0].message

    def test_violation_for_each_forbidden_column_present(self) -> None:
        policies = [_make_policy(table=ModelPolicyRule(has_columns=["temp", "debug"], not_=True))]
        columns = [
            _make_column(name="id"),
            _make_column(name="temp"),
            _make_column(name="debug"),
        ]
        table = _make_table(columns=columns)
        violations = self.manager.check_model(table, policies)
        assert len(violations) == 2
        assert all(v.rule == "has_columns" for v in violations)


class TestTablePolicyHasTableProperty:
    """Test has_table_property policy for requiring specific table properties."""

    def setup_method(self) -> None:
        self.manager = PolicyManager()

    def test_no_violation_when_all_properties_match_exactly(self) -> None:
        policies = [
            _make_policy(
                table=ModelPolicyRule(has_table_property={"delta.columnMapping.mode": "name"})
            )
        ]
        table = _make_table(
            table_properties={"delta.columnMapping.mode": "name", "extra_prop": "value"}
        )
        assert self.manager.check_model(table, policies) == []

    def test_violation_when_property_missing(self) -> None:
        policies = [
            _make_policy(table=ModelPolicyRule(has_table_property={"delta.format": "parquet"}))
        ]
        table = _make_table(table_properties={})
        violations = self.manager.check_model(table, policies)
        assert len(violations) == 1
        assert violations[0].rule == "has_table_property"
        assert "delta.format" in violations[0].message

    def test_violation_when_property_value_mismatch(self) -> None:
        policies = [
            _make_policy(table=ModelPolicyRule(has_table_property={"encryption_type": "AES"}))
        ]
        table = _make_table(table_properties={"encryption_type": "NONE"})
        violations = self.manager.check_model(table, policies)
        assert len(violations) == 1
        assert violations[0].rule == "has_table_property"
        assert "NONE" in violations[0].message
        assert "AES" in violations[0].message

    def test_no_violation_when_extra_properties_present(self) -> None:
        """Extra properties should be allowed (partial match)."""
        policies = [_make_policy(table=ModelPolicyRule(has_table_property={"owner": "data_team"}))]
        table = _make_table(
            table_properties={
                "owner": "data_team",
                "cost_center": "cc123",
                "retention_days": "90",
            }
        )
        assert self.manager.check_model(table, policies) == []


class TestTablePolicyHasQualityCheck:
    """Test has_quality_check policy requiring quality checks."""

    def setup_method(self) -> None:
        self.manager = PolicyManager()

    def test_no_violation_when_quality_configured(self) -> None:
        policies = [_make_policy(table=ModelPolicyRule(has_quality_check=True))]
        quality = SDPQuality(engine="sdp")
        table = _make_table(quality=quality)
        assert self.manager.check_model(table, policies) == []

    def test_violation_when_quality_not_configured(self) -> None:
        policies = [_make_policy(table=ModelPolicyRule(has_quality_check=True))]
        table = _make_table(quality=None)
        violations = self.manager.check_model(table, policies)
        assert len(violations) == 1
        assert violations[0].rule == "has_quality_check"
        assert "quality checks" in violations[0].message

    def test_no_violation_when_has_quality_check_false(self) -> None:
        """When has_quality_check=False (default), should not require quality."""
        policies = [_make_policy(table=ModelPolicyRule(has_quality_check=False))]
        table = _make_table(quality=None)
        assert self.manager.check_model(table, policies) == []


class TestColumnPolicyRules:
    def setup_method(self) -> None:
        self.manager = PolicyManager()

    def test_require_description_on_column(self) -> None:
        policies = [
            _make_policy(
                column=ColumnPolicyRule(require_description=True, severity=PolicySeverity.error)
            )
        ]
        col_ok = _make_column(name="id", description="Primary key")
        col_bad = _make_column(name="value", description=None)
        table = _make_table(columns=[col_ok, col_bad])

        violations = self.manager.check_model(table, policies)
        assert len(violations) == 1
        assert violations[0].column_name == "value"
        assert violations[0].severity == PolicySeverity.error


class TestMultiplePolicyMatching:
    """Test that all matching policies are applied, not just the first."""

    def setup_method(self) -> None:
        self.manager = PolicyManager()

    def test_multiple_policies_all_applied(self) -> None:
        """When multiple policies match, all should be applied."""
        policies = [
            _make_policy(
                name="bronze_standard",
                applies_to="models/bronze/*",
                table=ModelPolicyRule(require_description=True, severity=PolicySeverity.warn),
            ),
            _make_policy(
                name="all_standard",
                applies_to="models/*",
                table=ModelPolicyRule(require_tags=["owner"], severity=PolicySeverity.error),
            ),
        ]
        table = _make_table(
            name="test",
            description=None,
            tags={},
            origin_file_path="models/bronze/test.yml",
        )
        violations = self.manager.check_model(table, policies)
        # Should have violations from BOTH policies
        assert len(violations) == 2
        rules = {v.rule for v in violations}
        assert "require_description" in rules
        assert "require_tags" in rules

    def test_first_policy_only_if_second_no_match(self) -> None:
        """Only first matching policy violations if second doesn't match."""
        policies = [
            _make_policy(
                name="strict",
                applies_to="models/bronze/*",
                table=ModelPolicyRule(require_description=True, severity=PolicySeverity.error),
            ),
            _make_policy(
                name="second",
                applies_to="models/silver/*",  # Won't match
                table=ModelPolicyRule(require_tags=["owner"]),
            ),
        ]
        table = _make_table(description=None, origin_file_path="models/bronze/test.yml")
        violations = self.manager.check_model(table, policies)
        assert len(violations) == 1
        assert violations[0].severity == PolicySeverity.error

    def test_cumulative_violations_from_multiple_layers(self) -> None:
        """Policies can accumulate rules from different layers."""
        policies = [
            _make_policy(
                name="bronze_layer",
                applies_to="models/bronze/*",
                table=ModelPolicyRule(has_columns=["id"]),
            ),
            _make_policy(
                name="all_layers",
                applies_to="models/*",
                table=ModelPolicyRule(require_description=True),
            ),
        ]
        table = _make_table(
            description=None,
            columns=[],
            origin_file_path="models/bronze/customers.yml",
        )
        violations = self.manager.check_model(table, policies)
        # Should have 2 violations: missing id and missing description
        assert len(violations) == 2
        rules = {v.rule for v in violations}
        assert "has_columns" in rules
        assert "require_description" in rules


class TestNegationFormUsage:
    """Test practical usage of negation form."""

    def setup_method(self) -> None:
        self.manager = PolicyManager()

    def test_negation_mixed_with_positive_in_different_policies(self) -> None:
        """Negation in one policy, positive in another."""
        policies = [
            _make_policy(
                name="required_cols",
                applies_to="models/silver/*",
                table=ModelPolicyRule(has_columns=["id", "created_at", "updated_at"]),
            ),
            _make_policy(
                name="forbidden_cols",
                applies_to="models/silver/*",
                table=ModelPolicyRule(has_columns=["temp", "debug"], not_=True),
            ),
        ]
        table = _make_table(
            columns=[
                _make_column(name="id"),
                _make_column(name="created_at"),
                _make_column(name="updated_at"),
            ],
            origin_file_path="models/silver/orders.yml",
        )
        violations = self.manager.check_model(table, policies)
        # No violations - has all required, has none forbidden
        assert len(violations) == 0

    def test_negation_violation_with_positive_violation(self) -> None:
        """Both negation and positive form violations."""
        policies = [
            _make_policy(
                name="required_cols",
                applies_to="models/silver/*",
                table=ModelPolicyRule(has_columns=["id", "created_at"]),
            ),
            _make_policy(
                name="forbidden_cols",
                applies_to="models/silver/*",
                table=ModelPolicyRule(has_columns=["temp", "debug"], not_=True),
            ),
        ]
        table = _make_table(
            columns=[
                _make_column(name="id"),
                _make_column(name="temp"),
            ],
            origin_file_path="models/silver/orders.yml",
        )
        violations = self.manager.check_model(table, policies)
        # Should have 2 violations: missing created_at, and has forbidden temp
        assert len(violations) == 2
        rules = {v.rule for v in violations}
        assert rules == {"has_columns"}  # Both are has_columns violations
        messages = {v.message for v in violations}
        assert any("created_at" in msg and "missing" in msg for msg in messages)
        assert any("temp" in msg and "must not" in msg for msg in messages)


class TestPathPatternMatching:
    def setup_method(self) -> None:
        self.manager = PolicyManager()

    def test_exact_glob_match(self) -> None:
        assert PolicyManager._matches_path_pattern("models/bronze/customers.yml", "models/bronze/*")

    def test_no_match(self) -> None:
        assert not PolicyManager._matches_path_pattern(
            "models/silver/orders.yml", "models/bronze/*"
        )

    def test_wildcard_in_middle(self) -> None:
        assert PolicyManager._matches_path_pattern("models/silver/orders.yml", "*/silver/*")

    def test_backslash_normalisation(self) -> None:
        assert PolicyManager._matches_path_pattern(
            "models\\bronze\\customers.yml", "models/bronze/*"
        )


class TestPolicyNotAliasAndFastExit:
    def setup_method(self) -> None:
        self.manager = PolicyManager()

    def test_model_rule_accepts_not_alias(self) -> None:
        rule = ModelPolicyRule.model_validate({"has_columns": ["temp"], "not": True})
        assert rule.not_ is True

    def test_fast_exit_returns_first_violating_policy_per_model(self) -> None:
        policies = [
            _make_policy(
                name="first",
                applies_to="models/silver/*",
                table=ModelPolicyRule(require_description=True),
            ),
            _make_policy(
                name="second",
                applies_to="models/silver/*",
                table=ModelPolicyRule(require_tags=["owner"]),
            ),
        ]
        table = _make_table(description=None, tags={}, origin_file_path="models/silver/orders.yml")

        all_violations = self.manager.check_model(table, policies, fast_exit=False)
        fast_violations = self.manager.check_model(table, policies, fast_exit=True)

        assert len(all_violations) == 2
        assert len(fast_violations) == 1
        assert fast_violations[0].rule == "require_description"

    def test_fast_exit_applies_per_model_in_catalog(self) -> None:
        policies = [
            _make_policy(
                applies_to="models/*",
                table=ModelPolicyRule(require_description=True),
            ),
            _make_policy(
                applies_to="models/*",
                table=ModelPolicyRule(require_tags=["owner"]),
            ),
        ]
        models = [
            _make_table(
                name="m1", description=None, tags={}, origin_file_path="models/silver/m1.yml"
            ),
            _make_table(
                name="m2", description=None, tags={}, origin_file_path="models/bronze/m2.yml"
            ),
        ]

        violations = self.manager.check_catalog(models, policies, fast_exit=True)
        assert len(violations) == 2
