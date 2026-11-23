"""Tests for pure direct-grant diff behavior."""

from kelp.catalog.uc_models import GrantDiff
from kelp.models.grant import Grant
from kelp.service.grant_manager import diff_grants


def _grant(principal: str, *privileges: str) -> Grant:
    return Grant(principal=principal, privileges=list(privileges))


def test_append_adds_missing_and_preserves_unconfigured_principals() -> None:
    diff = diff_grants(
        [_grant("Analysts", "SELECT", "MODIFY")],
        [_grant("analysts", "SELECT"), _grant("owners", "ALL PRIVILEGES")],
        "append",
    )

    assert diff == GrantDiff(grants={"Analysts": ["MODIFY"]})


def test_managed_revokes_extra_and_unconfigured_direct_grants() -> None:
    diff = diff_grants(
        [_grant("analysts", "SELECT")],
        [_grant("analysts", "SELECT", "MODIFY"), _grant("other", "ALL PRIVILEGES")],
        "managed",
    )

    assert diff.revokes == {"analysts": ["MODIFY"]}
    assert diff.revoke_all == ["other"]
    assert not diff.grants


def test_revoke_marker_removes_all_direct_privileges() -> None:
    diff = diff_grants(
        [_grant("analysts")],
        [_grant("analysts", "SELECT", "MODIFY")],
        "append",
    )

    assert diff.revoke_all == ["analysts"]
    assert not diff.revokes
    assert not diff.grants


def test_all_privileges_revoke_regrants_every_desired_privilege() -> None:
    diff = diff_grants(
        [_grant("analysts", "SELECT", "MODIFY")],
        [_grant("analysts", "ALL PRIVILEGES", "SELECT")],
        "managed",
    )

    assert diff.revoke_all == ["analysts"]
    assert diff.grants == {"analysts": ["MODIFY", "SELECT"]}


def test_all_privileges_desired_reconciles_direct_grants() -> None:
    diff = diff_grants(
        [_grant("analysts", "ALL PRIVILEGES")],
        [_grant("analysts", "SELECT")],
        "managed",
    )

    assert diff.revokes == {"analysts": ["SELECT"]}
    assert diff.grants == {"analysts": ["ALL PRIVILEGES"]}


def test_append_keeps_remote_all_privileges() -> None:
    diff = diff_grants(
        [_grant("analysts", "SELECT")],
        [_grant("analysts", "ALL PRIVILEGES")],
        "append",
    )

    assert not diff.has_changes


def test_revoke_all_is_skipped_when_principal_has_no_direct_grants() -> None:
    diff = diff_grants([_grant("analysts")], [], "append")

    assert not diff.has_changes


def test_all_privileges_transition_is_idempotent() -> None:
    desired = [_grant("analysts", "SELECT", "MODIFY")]
    actual = [_grant("analysts", "ALL PRIVILEGES", "SELECT")]
    first = diff_grants(desired, actual, "managed")
    after_apply = [_grant("analysts", "SELECT", "MODIFY")]

    assert first.revoke_all == ["analysts"]
    assert not diff_grants(desired, after_apply, "managed").has_changes
