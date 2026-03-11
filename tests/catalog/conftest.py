"""Fixtures for catalog tests."""

from __future__ import annotations

import pytest

from kelp.catalog.uc_models import Column, RemoteCatalogConfig
from kelp.catalog.uc_models import Model as Table
from kelp.models.model import PrimaryKeyConstraint, TableType


@pytest.fixture
def default_config() -> RemoteCatalogConfig:
    """Return default remote catalog config."""
    return RemoteCatalogConfig()


@pytest.fixture
def append_config() -> RemoteCatalogConfig:
    """Return config with append mode for tags and properties."""
    return RemoteCatalogConfig(
        table_tag_mode="append",
        column_tag_mode="append",
        table_property_mode="append",
    )


@pytest.fixture
def replace_config() -> RemoteCatalogConfig:
    """Return config with replace mode for tags."""
    return RemoteCatalogConfig(
        table_tag_mode="replace",
        column_tag_mode="replace",
        table_property_mode="append",
    )


@pytest.fixture
def managed_config() -> RemoteCatalogConfig:
    """Return config with managed mode and specific managed keys."""
    return RemoteCatalogConfig(
        table_tag_mode="managed",
        managed_table_tags=["environment", "owner"],
        column_tag_mode="managed",
        managed_column_tags=["pii", "sensitive"],
        table_property_mode="managed",
        managed_table_properties=["delta.enableChangeDataFeed"],
    )


@pytest.fixture
def sample_local_table() -> Table:
    """Return a sample local table with tags, properties, and constraints."""
    return Table(
        name="test_table",
        catalog="test_catalog",
        schema_="test_schema",
        description="Test table description",
        tags={"environment": "dev", "owner": "data_team"},
        table_properties={"delta.enableChangeDataFeed": "true"},
        columns=[
            Column(
                name="id",
                data_type="INT",
                description="Primary key",
                tags={"pii": "false"},
            ),
            Column(
                name="email",
                data_type="STRING",
                description="User email",
                tags={"pii": "true", "sensitive": "true"},
            ),
            Column(
                name="created_at",
                data_type="TIMESTAMP",
                description="Creation timestamp",
            ),
        ],
        constraints=[
            PrimaryKeyConstraint(name="pk_test", columns=["id"]),
        ],
        cluster_by=["id"],
        cluster_by_auto=False,
        table_type=TableType.MANAGED,
    )


@pytest.fixture
def sample_remote_table() -> Table:
    """Return a sample remote table with different tags and properties."""
    return Table(
        name="test_table",
        catalog="test_catalog",
        schema_="test_schema",
        description="Old description",
        tags={"environment": "prod", "deprecated": "true"},
        table_properties={
            "delta.enableChangeDataFeed": "false",
            "delta.autoOptimize": "true",
        },
        columns=[
            Column(
                name="id",
                data_type="INT",
                description="Old primary key description",
                tags={"pii": "true"},
            ),
            Column(
                name="email",
                data_type="STRING",
                description="Old email description",
                tags={"pii": "true"},
            ),
            Column(
                name="created_at",
                data_type="TIMESTAMP",
                description="Creation timestamp",
            ),
        ],
        constraints=[
            PrimaryKeyConstraint(name="pk_test", columns=["id"]),
        ],
        cluster_by=["created_at"],
        cluster_by_auto=False,
        table_type=TableType.MANAGED,
    )


@pytest.fixture
def empty_remote_table() -> Table:
    """Return a remote table with no tags, properties, or constraints."""
    return Table(
        name="test_table",
        catalog="test_catalog",
        schema_="test_schema",
        description="Empty remote table",
        tags={},
        table_properties={},
        columns=[
            Column(
                name="id",
                data_type="INT",
            ),
            Column(
                name="name",
                data_type="STRING",
            ),
        ],
        constraints=[],
        table_type=TableType.MANAGED,
    )
