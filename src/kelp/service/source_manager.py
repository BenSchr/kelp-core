"""Source manager for Kelp projects.

Provides methods to retrieve and resolve data sources (volumes, tables, raw paths)
that can be referenced in pipelines using kp.source().
"""

from kelp.config import get_context
from kelp.models.source import Source


class SourceManager:
    """Manager for accessing and resolving sources in Kelp projects."""

    @classmethod
    def get_source(cls, name: str) -> Source:
        """Get a source by name.

        Retrieves a source definition from the project catalog.

        Args:
            name: Source name to retrieve.

        Returns:
            Source object with all configuration.

        Raises:
            KeyError: If the source name is not found in the catalog.
        """
        ctx = get_context()
        return ctx.catalog.get_source(name)

    @classmethod
    def get_path(cls, name: str) -> str:
        """Get the path for a source.

        Returns the fully qualified path for the source, which differs based on
        the source type:
        - For table sources: returns catalog.schema.table_name
        - For volume sources: returns the volume path
        - For raw_path sources: returns the path value

        Args:
            name: Source name.

        Returns:
            Path string suitable for use in pipelines.

        Raises:
            KeyError: If the source is not found in the catalog.
            ValueError: If the source configuration is incomplete.
        """
        source = cls.get_source(name)
        return source.get_path()

    @classmethod
    def get_options(cls, name: str) -> dict:
        """Get the options dictionary for a source.

        Options contain source-specific configuration such as format, headers,
        encoding, or other DataFrame reader/writer options.

        Args:
            name: Source name.

        Returns:
            Dictionary of source-specific options.

        Raises:
            KeyError: If the source is not found in the catalog.
        """
        source = cls.get_source(name)
        return source.options
