from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema


class Source(BaseModel):
    """Source definition for Kelp projects.

    Represents a data source (volume, table, or raw path) that can be referenced
    in pipelines using kp.source(). Sources provide a single point of configuration
    for data locations and connection options.

    Attributes:
        origin_file_path: Path to the source YAML file defining this source.
        name: Source name (unique identifier).
        source_type: Type of source ('volume', 'table', 'raw_path').
        path: Physical path for volume or raw_path sources (e.g., '/Volumes/catalog/schema/volume').
        catalog: Catalog name for table or volume sources.
        schema_: Schema name for table or volume sources.
        table_name: Name for table or view sources.
        volume_name: Volume name for volume sources (constructs path as /Volumes/catalog/schema/volume_name).
        options: Dictionary of options specific to this source (e.g., format, headers).
        description: Human-readable description of the source.
    """

    origin_file_path: SkipJsonSchema[str] | None = Field(
        default=None,
        description="Path to the source YAML file defining this source",
    )
    name: str = Field(
        description="Source name (unique identifier)",
    )
    source_type: Literal["volume", "table", "raw_path"] = Field(
        default="table",
        description="Type of source: volume, table, or raw_path",
    )
    path: str | None = Field(
        default=None,
        description="Physical path for volume or raw_path sources",
    )
    catalog: str | None = Field(
        default=None,
        description="Catalog name for table or volume sources",
    )
    schema_: str | None = Field(
        default=None,
        alias="schema",
        description="Schema name for table or volume sources",
    )
    table_name: str | None = Field(
        default=None,
        description="Name for table or view sources",
    )
    volume_name: str | None = Field(
        default=None,
        description="Volume name for volume sources (constructs /Volumes/catalog/schema/volume_name)",
    )
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Source-specific options (e.g., format, headers, encoding)",
    )
    description: str | None = Field(
        default=None,
        description="Human-readable description of the source",
    )

    # Model Config
    model_config = ConfigDict(
        validate_by_name=True,
        validate_by_alias=True,
        serialize_by_alias=True,
        use_enum_values=True,
    )

    @property
    def fqn(self) -> str | None:
        """Get the fully qualified name for table sources.

        Returns:
            Fully qualified name (catalog.schema.model_name) for table sources,
            or None for other source types.
        """
        if self.source_type == "table" and self.catalog and self.schema_ and self.table_name:
            return f"{self.catalog}.{self.schema_}.{self.table_name}"
        return None

    @property
    def volume_fqn(self) -> str | None:
        """Get the fully qualified path for volume sources.

        Constructs the volume path as /Volumes/catalog/schema/volume_name.

        Returns:
            Volume path (/Volumes/catalog/schema/volume_name) for volume sources,
            or None for other source types.
        """
        if self.source_type == "volume" and self.catalog and self.schema_ and self.volume_name:
            return f"/Volumes/{self.catalog}/{self.schema_}/{self.volume_name}"
        return None

    def get_path(self) -> str:
        """Get the path for this source.

        For table sources, returns the fully qualified name.
        For volume sources with catalog, schema, and volume_name defined,
        returns the constructed volume path (/Volumes/catalog/schema/volume_name).
        Otherwise, returns the explicit path field.
        For raw_path sources, returns the path directly.

        Returns:
            The path or fully qualified name for this source.

        Raises:
            ValueError: If the source configuration is incomplete.
        """
        if self.source_type == "table":
            fqn = self.fqn
            if not fqn:
                raise ValueError(
                    f"Table source '{self.name}' requires catalog, schema, and model_name"
                )
            return fqn
        if self.source_type == "volume":
            # Try constructed path first (catalog + schema + volume_name)
            volume_path = self.volume_fqn
            if volume_path:
                return volume_path
            # Fall back to explicit path
            if self.path:
                return self.path
            raise ValueError(
                f"Volume source '{self.name}' requires either path or (catalog, schema, volume_name)"
            )
        if self.source_type == "raw_path":
            if not self.path:
                raise ValueError(f"Source '{self.name}' requires a path")
            return self.path
        raise ValueError(f"Unknown source_type: {self.source_type}")
