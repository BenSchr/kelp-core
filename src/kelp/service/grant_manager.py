"""Pure grant-diff logic for Unity Catalog objects."""

from kelp.catalog.uc_models import GrantDiff
from kelp.models.grant import Grant, GrantMode

_ALL_PRIVILEGES = "ALL PRIVILEGES"


def _group_by_principal(grants: list[Grant] | None) -> tuple[dict[str, str], dict[str, set[str]]]:
    """Group grants by case-insensitive principal name."""
    names: dict[str, str] = {}
    privileges: dict[str, set[str]] = {}
    for grant in grants or []:
        key = grant.principal.casefold()
        names.setdefault(key, grant.principal)
        privileges.setdefault(key, set()).update(grant.privileges or [])
    return names, privileges


def diff_grants(
    desired: list[Grant] | None,
    actual: list[Grant] | None,
    mode: GrantMode,
) -> GrantDiff:
    """Compare direct grants, keeping effective/inherited permissions out of scope.

    Args:
        desired: Grants configured in project metadata.
        actual: Direct grants fetched from the target object.
        mode: ``append`` preserves unconfigured grants; ``managed`` removes them.

    Returns:
        Grant changes needed to reach the configured state.
    """
    diff = GrantDiff()
    if desired is None:
        return diff

    desired_names, desired_privileges = _group_by_principal(desired)
    actual_names, actual_privileges = _group_by_principal(actual)

    if mode == "managed":
        for key, actual_set in actual_privileges.items():
            if key not in desired_names and actual_set:
                diff.revoke_all.append(actual_names[key])

    for key, principal in desired_names.items():
        wanted = desired_privileges[key]
        existing = actual_privileges.get(key, set())

        if not wanted:
            if existing:
                diff.revoke_all.append(principal)
            continue

        if _ALL_PRIVILEGES in existing:
            if mode == "append" or _ALL_PRIVILEGES in wanted:
                continue
            diff.revoke_all.append(principal)
            diff.grants[principal] = sorted(wanted)
            continue

        if _ALL_PRIVILEGES in wanted:
            if mode == "managed" and existing:
                diff.revokes[principal] = sorted(existing)
            if _ALL_PRIVILEGES not in existing:
                diff.grants[principal] = [_ALL_PRIVILEGES]
            continue

        extra = existing - wanted
        missing = wanted - existing
        if mode == "managed" and extra:
            diff.revokes[principal] = sorted(extra)
        if missing:
            diff.grants[principal] = sorted(missing)

    diff.revoke_all = sorted(set(diff.revoke_all), key=str.casefold)
    return diff
