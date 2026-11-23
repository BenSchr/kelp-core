"""Unity Catalog grant configuration models and normalization."""

import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator

GrantMode = Literal["append", "managed"]

_PRIVILEGE_PATTERN = re.compile(r"[A-Z][A-Z0-9]*(?: [A-Z0-9]+)*\Z")


def normalize_privilege(value: str) -> str:
    """Normalize a privilege token and reject values unsafe for SQL rendering.

    Args:
        value: Privilege from project metadata or the remote catalog.

    Returns:
        Uppercase privilege name with spaces between words.

    Raises:
        ValueError: If the privilege is empty or contains unsupported characters.
    """
    normalized = " ".join(value.replace("_", " ").split()).upper()
    if not _PRIVILEGE_PATTERN.fullmatch(normalized):
        raise ValueError(f"Invalid privilege token: {value!r}")
    return normalized


class Grant(BaseModel):
    """Direct privileges assigned to a Unity Catalog principal."""

    principal: str = Field(description="User, service principal, or group")
    privileges: list[str] | None = Field(
        description="Unity Catalog privileges; an empty or null list marks a revoke",
    )

    @field_validator("principal")
    @classmethod
    def _validate_principal(cls, value: str) -> str:
        """Reject blank principals and control characters."""
        if not value or value != value.strip() or any(ord(char) < 32 for char in value):
            raise ValueError("Principal must be non-empty and contain no surrounding whitespace")
        return value

    @field_validator("privileges")
    @classmethod
    def _normalize_privileges(cls, value: list[str] | None) -> list[str]:
        """Normalize, deduplicate, and sort privileges; null is an empty list."""
        return sorted({normalize_privilege(privilege) for privilege in value or []})


def merge_grants(grants: list[Grant] | None) -> list[Grant] | None:
    """Merge grants by principal, unioning privileges and retaining revoke markers.

    Args:
        grants: Grants gathered from hierarchy defaults and object metadata.

    Returns:
        Sorted, merged grants, or ``None`` when no entries were configured.

    Raises:
        ValueError: If principals differ only by case.
    """
    if not grants:
        return None

    names: dict[str, str] = {}
    privileges_by_principal: dict[str, set[str]] = {}
    for grant in grants:
        folded = grant.principal.casefold()
        existing_name = names.get(folded)
        if existing_name is not None and existing_name != grant.principal:
            raise ValueError(
                f"Conflicting principal spellings differ only by case: "
                f"{existing_name!r} and {grant.principal!r}"
            )
        names[folded] = grant.principal
        privileges_by_principal.setdefault(folded, set()).update(grant.privileges or [])

    return [
        Grant(principal=names[key], privileges=sorted(privileges_by_principal[key]))
        for key in sorted(names)
    ]
