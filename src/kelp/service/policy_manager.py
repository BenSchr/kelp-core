"""Policy manager for evaluating metadata governance policies."""

from __future__ import annotations

import fnmatch
import logging
import re
from dataclasses import dataclass

from kelp.models.model import Model
from kelp.models.policy import ColumnPolicyRule, ModelPolicyRule, PolicySeverity, PolicyViolation
from kelp.models.policy_definition import Policy

logger = logging.getLogger(__name__)


@dataclass
class ResolvedPolicy:
    policy: Policy
    model_rule: ModelPolicyRule
    column_rule: ColumnPolicyRule


class PolicyManager:
    def check_model(
        self,
        model: Model,
        policies: list[Policy],
        fast_exit: bool = False,
    ) -> list[PolicyViolation]:
        """Check a single model against all matching policies.

        All policies with matching applies_to patterns are applied. This allows
        cumulative governance requirements across layers and subdirectories.

        Args:
            model: The model to check.
            policies: All available policy definitions.
            fast_exit: If True, return as soon as the first matching policy
                yields one or more violations for this model.

        Returns:
            List of policy violations from all matching policies.
        """
        violations: list[PolicyViolation] = []
        model_fqn = model.get_qualified_name()

        # Resolve all matching policies (not just the first)
        resolved_policies = self._resolve_policy_for_model(model, policies)
        if not resolved_policies:
            return []

        # Apply all matching policies (in order)
        for resolved_policy in resolved_policies:
            model_rules = resolved_policy.model_rule
            column_rules = resolved_policy.column_rule
            policy_violations = self._check_model_rules(model, model_fqn, model_rules, column_rules)
            violations.extend(policy_violations)
            if fast_exit and policy_violations:
                return violations

        return violations

    def _check_model_rules(
        self,
        model: Model,
        model_fqn: str,
        model_rules: ModelPolicyRule,
        column_rules: ColumnPolicyRule,
    ) -> list[PolicyViolation]:
        """Apply model and column rule checks for a single policy.

        Args:
            model: The model to check.
            model_fqn: Fully qualified model name.
            model_rules: Model-level rules to apply.
            column_rules: Column-level rules to apply.

        Returns:
            List of policy violations found.
        """
        violations: list[PolicyViolation] = []

        if model_rules.require_description and ((not model.description) != model_rules.not_):
            violations.append(
                PolicyViolation(
                    model_name=model_fqn,
                    rule="require_description",
                    message=(
                        f"Model '{model_fqn}' is missing a description."
                        if not model_rules.not_
                        else f"Model '{model_fqn}' must not have a description."
                    ),
                    severity=model_rules.severity,
                )
            )

        if model_rules.require_any_tag and ((not model.tags) != model_rules.not_):
            violations.append(
                PolicyViolation(
                    model_name=model_fqn,
                    rule="require_any_tag",
                    message=(
                        f"Model '{model_fqn}' has no tags."
                        if not model_rules.not_
                        else f"Model '{model_fqn}' must not have tags."
                    ),
                    severity=model_rules.severity,
                )
            )

        if model_rules.require_tags:
            if not model_rules.not_:
                violations.extend(
                    PolicyViolation(
                        model_name=model_fqn,
                        rule="require_tags",
                        message=f"Model '{model_fqn}' is missing required tag '{required_tag}'.",
                        severity=model_rules.severity,
                    )
                    for required_tag in model_rules.require_tags
                    if required_tag not in model.tags
                )
            else:
                violations.extend(
                    PolicyViolation(
                        model_name=model_fqn,
                        rule="require_tags",
                        message=f"Model '{model_fqn}' must not have tag '{forbidden_tag}'.",
                        severity=model_rules.severity,
                    )
                    for forbidden_tag in model_rules.require_tags
                    if forbidden_tag in model.tags
                )

        if model_rules.require_constraints:
            existing_types_normalised = set()
            for constraint in model.constraints:
                cname = type(constraint).__name__
                if "Primary" in cname:
                    existing_types_normalised.add("primary_key")
                elif "Foreign" in cname:
                    existing_types_normalised.add("foreign_key")
                else:
                    existing_types_normalised.add(cname.lower())

            if not model_rules.not_:
                violations.extend(
                    PolicyViolation(
                        model_name=model_fqn,
                        rule="require_constraints",
                        message=(
                            f"Model '{model_fqn}' is missing required constraint "
                            f"type '{required_constraint}'."
                        ),
                        severity=model_rules.severity,
                    )
                    for required_constraint in model_rules.require_constraints
                    if required_constraint not in existing_types_normalised
                )
            else:
                violations.extend(
                    PolicyViolation(
                        model_name=model_fqn,
                        rule="require_constraints",
                        message=(
                            f"Model '{model_fqn}' must not have constraint "
                            f"type '{forbidden_constraint}'."
                        ),
                        severity=model_rules.severity,
                    )
                    for forbidden_constraint in model_rules.require_constraints
                    if forbidden_constraint in existing_types_normalised
                )

        if model_rules.naming_pattern and (
            (not re.match(model_rules.naming_pattern, model.name)) != model_rules.not_
        ):
            violations.append(
                PolicyViolation(
                    model_name=model_fqn,
                    rule="naming_pattern",
                    message=(
                        f"Model name '{model.name}' does not match pattern '{model_rules.naming_pattern}'."
                        if not model_rules.not_
                        else f"Model name '{model.name}' must not match pattern '{model_rules.naming_pattern}'."
                    ),
                    severity=model_rules.severity,
                )
            )

        # Check for required/forbidden columns
        violations.extend(
            self._check_columns_rule(
                model,
                model_fqn,
                model_rules.has_columns,
                model_rules.not_,
                model_rules.severity,
            )
        )

        # Check for table properties
        if model_rules.has_table_property:
            violations.extend(
                self._check_table_property(
                    model_fqn,
                    model.table_properties,
                    model_rules.has_table_property,
                    model_rules.not_,
                    model_rules.severity,
                )
            )

        # Check for quality checks
        if model_rules.has_quality_check and ((not bool(model.quality)) != model_rules.not_):
            violations.append(
                PolicyViolation(
                    model_name=model_fqn,
                    rule="has_quality_check",
                    message=(
                        f"Model '{model_fqn}' must have quality checks configured."
                        if not model_rules.not_
                        else f"Model '{model_fqn}' must not have quality checks configured."
                    ),
                    severity=model_rules.severity,
                )
            )

        # Check column-level rules
        for column in model.columns:
            if column_rules.require_description and ((not column.description) != column_rules.not_):
                violations.append(
                    PolicyViolation(
                        model_name=model_fqn,
                        column_name=column.name,
                        rule="require_description",
                        message=(
                            f"Column '{column.name}' in model '{model_fqn}' is missing a description."
                            if not column_rules.not_
                            else f"Column '{column.name}' in model '{model_fqn}' must not have a description."
                        ),
                        severity=column_rules.severity,
                    )
                )

            if column_rules.require_any_tag and ((not column.tags) != column_rules.not_):
                violations.append(
                    PolicyViolation(
                        model_name=model_fqn,
                        column_name=column.name,
                        rule="require_any_tag",
                        message=(
                            f"Column '{column.name}' in model '{model_fqn}' has no tags."
                            if not column_rules.not_
                            else f"Column '{column.name}' in model '{model_fqn}' must not have tags."
                        ),
                        severity=column_rules.severity,
                    )
                )

            if column_rules.require_tags:
                if not column_rules.not_:
                    violations.extend(
                        PolicyViolation(
                            model_name=model_fqn,
                            column_name=column.name,
                            rule="require_tags",
                            message=(
                                f"Column '{column.name}' in model '{model_fqn}' "
                                f"is missing required tag '{required_tag}'."
                            ),
                            severity=column_rules.severity,
                        )
                        for required_tag in column_rules.require_tags
                        if required_tag not in column.tags
                    )
                else:
                    violations.extend(
                        PolicyViolation(
                            model_name=model_fqn,
                            column_name=column.name,
                            rule="require_tags",
                            message=(
                                f"Column '{column.name}' in model '{model_fqn}' "
                                f"must not have tag '{forbidden_tag}'."
                            ),
                            severity=column_rules.severity,
                        )
                        for forbidden_tag in column_rules.require_tags
                        if forbidden_tag in column.tags
                    )

            if column_rules.naming_pattern and (
                (not re.match(column_rules.naming_pattern, column.name)) != column_rules.not_
            ):
                violations.append(
                    PolicyViolation(
                        model_name=model_fqn,
                        column_name=column.name,
                        rule="naming_pattern",
                        message=(
                            f"Column name '{column.name}' does not match pattern '{column_rules.naming_pattern}'."
                            if not column_rules.not_
                            else f"Column name '{column.name}' must not match pattern '{column_rules.naming_pattern}'."
                        ),
                        severity=column_rules.severity,
                    )
                )

            if column.data_type and column_rules.naming_patterns_by_type:
                violations.extend(
                    PolicyViolation(
                        model_name=model_fqn,
                        column_name=column.name,
                        rule="naming_pattern_by_type",
                        message=(
                            f"Column '{column.name}' ({column.data_type}) "
                            f"does not match required pattern '{type_pattern.pattern}'."
                        ),
                        severity=column_rules.severity,
                    )
                    for type_pattern in column_rules.naming_patterns_by_type
                    if type_pattern.data_type.upper() == column.data_type.upper()
                    and ((not re.match(type_pattern.pattern, column.name)) != column_rules.not_)
                )

        return violations

    def _check_columns_rule(
        self,
        model: Model,
        model_fqn: str,
        has_columns: list[str],
        negate: bool,
        severity: PolicySeverity,
    ) -> list[PolicyViolation]:
        """Check column requirements.

        When ``negate`` is False, listed columns are required.
        When ``negate`` is True (policy uses ``not: true``), listed columns are forbidden.

        Args:
            model: The model to check.
            model_fqn: Fully qualified model name.
            has_columns: Column names from ``has_columns``.
            negate: Whether to invert semantics.
            severity: Severity level for violations.

        Returns:
            List of violations.
        """
        violations: list[PolicyViolation] = []
        existing_column_names = {col.name for col in model.columns}

        if has_columns:
            for required_col in has_columns:
                is_missing = required_col not in existing_column_names
                if is_missing != negate:
                    violations.append(
                        PolicyViolation(
                            model_name=model_fqn,
                            rule="has_columns",
                            message=(
                                f"Model '{model_fqn}' is missing required column '{required_col}'."
                                if not negate
                                else f"Model '{model_fqn}' must not have column '{required_col}'."
                            ),
                            severity=severity,
                        )
                    )

        return violations

    def _check_table_property(
        self,
        model_fqn: str,
        table_properties: dict,
        required_properties: dict,
        negate: bool,
        severity: PolicySeverity,
    ) -> list[PolicyViolation]:
        """Check if table has required properties (partial match).

        Verifies that all key-value pairs in required_properties exist in table_properties.
        Extra keys in table_properties are allowed (partial match).

        Args:
            model_fqn: Fully qualified model name for violation reporting.
            table_properties: Actual table properties to check against.
            required_properties: Required property key-value pairs to match.
            negate: Whether to invert semantics.
            severity: Severity level for violations.

        Returns:
            List of violations if properties don't match.
        """
        violations: list[PolicyViolation] = []

        for key, required_value in required_properties.items():
            key_missing = key not in table_properties
            value_matches = (not key_missing) and table_properties[key] == required_value

            if not negate and key_missing:
                violations.append(
                    PolicyViolation(
                        model_name=model_fqn,
                        rule="has_table_property",
                        message=f"Model '{model_fqn}' is missing required table property '{key}'.",
                        severity=severity,
                    )
                )
            elif not negate and not value_matches:
                violations.append(
                    PolicyViolation(
                        model_name=model_fqn,
                        rule="has_table_property",
                        message=(
                            f"Model '{model_fqn}' has table property '{key}' with value "
                            f"'{table_properties[key]}', expected '{required_value}'."
                        ),
                        severity=severity,
                    )
                )
            elif negate and value_matches:
                violations.append(
                    PolicyViolation(
                        model_name=model_fqn,
                        rule="has_table_property",
                        message=(
                            f"Model '{model_fqn}' must not have table property '{key}' "
                            f"with value '{required_value}'."
                        ),
                        severity=severity,
                    )
                )

        return violations

    def check_catalog(
        self,
        models: list[Model],
        policies: list[Policy],
        fast_exit: bool = False,
    ) -> list[PolicyViolation]:
        all_violations: list[PolicyViolation] = []
        for model in models:
            all_violations.extend(self.check_model(model, policies, fast_exit=fast_exit))
        return all_violations

    def _resolve_policy_for_model(
        self, model: Model, policies: list[Policy]
    ) -> list[ResolvedPolicy]:
        """Resolve all matching policies for a model.

        Returns all policies whose applies_to pattern matches the model's origin_file_path.
        Policies are checked in order, allowing cumulative application of rules.

        Args:
            model: The model to match.
            policies: All available policies.

        Returns:
            List of all matching ResolvedPolicy objects.
        """
        model_path = model.origin_file_path or ""
        return [
            ResolvedPolicy(
                policy=policy,
                model_rule=policy.model,
                column_rule=policy.column,
            )
            for policy in policies
            if policy.applies_to and self._matches_path_pattern(model_path, policy.applies_to)
        ]

    @staticmethod
    def _matches_path_pattern(path: str, pattern: str) -> bool:
        path = path.replace("\\", "/")
        pattern = pattern.replace("\\", "/")
        return fnmatch.fnmatch(path, pattern)

    @staticmethod
    def log_violations(violations: list[PolicyViolation]) -> None:
        for violation in violations:
            if violation.severity == PolicySeverity.error:
                logger.error("[POLICY ERROR] %s", violation.message)
            else:
                logger.warning("[POLICY WARN] %s", violation.message)

    @staticmethod
    def raise_if_errors(violations: list[PolicyViolation]) -> None:
        errors = [v for v in violations if v.severity == PolicySeverity.error]
        if errors:
            messages = "\n  ".join(v.message for v in errors)
            raise RuntimeError(f"Policy check failed with {len(errors)} error(s):\n  {messages}")
