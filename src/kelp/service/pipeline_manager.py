from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines as sp

from kelp.models.project_config import QuarantineConfig
from kelp.models.table import Table
from kelp.utils.common import find_path_by_name
from kelp.utils.databricks import get_table_from_dbx_sdk

logger = logging.getLogger(__name__)


@dataclass
class PipelineInfo:
    """Information about a detected pipeline.

    Attributes:
        target: Target environment name (e.g., 'dev', 'prod')
        name: Pipeline name
        id: Pipeline ID
    """

    target: str
    name: str
    id: str

    def __str__(self) -> str:
        """Format for display."""
        return f"{self.name} (target: {self.target})"


class PipelineManager:
    """Service for fetching and managing tables from Databricks pipelines."""

    def __init__(self, w: WorkspaceClient | None = None, profile: str | None = None):
        """Initialize PipelineManager with optional WorkspaceClient or profile."""
        self.w = w or WorkspaceClient(profile=profile)

    def fetch_pipeline_tables(
        self,
        pipeline_id: str,
        quarantine_config: QuarantineConfig | None = None,
    ) -> list[Table]:
        """Fetch all relevant tables from a Databricks pipeline.

        Args:
            pipeline_id: The Databricks pipeline ID
            quarantine_config: Optional config to filter out quarantine/validation tables

        Returns:
            List of Table objects from the pipeline

        """
        logger.info("Fetching tables from pipeline %s", pipeline_id)

        # Get pipeline updates
        updates = self._fetch_updates(pipeline_id)
        if not updates:
            logger.warning("No updates found for pipeline %s", pipeline_id)
            return []

        # Fetch events for the updates
        event_map = self._events_for_window(pipeline_id, updates)

        # Extract dataset names
        dataset_names = self._extract_dataset_names(updates, event_map)
        logger.info("Found %s datasets in pipeline", len(dataset_names))

        # Filter relevant datasets
        if quarantine_config:
            dataset_names = [
                name for name in dataset_names if self._is_relevant_dataset(name, quarantine_config)
            ]
            logger.info("Filtered to %s relevant datasets", len(dataset_names))

        # Fetch table metadata from Databricks
        tables = []
        for dataset_name in dataset_names:
            try:
                table = get_table_from_dbx_sdk(dataset_name, w=self.w)
                tables.append(table)
                logger.debug("Fetched metadata for %s", dataset_name)
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to fetch table %s: %s", dataset_name, e)
                continue

        logger.info("Successfully fetched %s tables", len(tables))
        return tables

    def _fetch_updates(self, pipeline_id: str) -> list[sp.UpdateInfo]:
        """Fetch pipeline updates, sorted by creation time descending."""
        page = self.w.pipelines.list_updates(pipeline_id=pipeline_id, max_results=5)
        updates = list(page.updates or [])
        updates.sort(key=lambda u: u.creation_time, reverse=True)
        return updates

    def _events_for_window(
        self,
        pipeline_id: str,
        updates: list[sp.UpdateInfo],
    ) -> dict[str, list[sp.PipelineEvent]]:
        """Fetch events for the given pipeline updates."""
        if not updates:
            return {}

        if updates[0].creation_time is None:
            return {}
        start_iso = self._to_iso_ts(updates[0].creation_time)
        end_iso = None
        if len(updates) > 1 and updates[1].creation_time is not None:
            end_iso = self._to_iso_ts(updates[1].creation_time)

        logger.debug("Fetching events for window [%s, %s)", start_iso, end_iso)
        event_filter = f"timestamp <= '{start_iso}'"
        if end_iso:
            event_filter += f" AND timestamp >= '{end_iso}'"

        events = list(
            self.w.pipelines.list_pipeline_events(pipeline_id=pipeline_id, filter=event_filter),
        )

        update_ids = [u.update_id for u in updates if u.update_id]
        update_event_map: dict[str, list[sp.PipelineEvent]] = {
            update_id: [] for update_id in update_ids
        }
        for event in events:
            if event.origin and event.origin.update_id in update_event_map:
                update_event_map[event.origin.update_id].append(event)

        return update_event_map

    def _extract_dataset_names(
        self,
        updates: list[sp.UpdateInfo],
        event_map: dict[str, list[sp.PipelineEvent]],
    ) -> list[str]:
        """Extract unique dataset names from pipeline events."""
        names: set[str] = set()
        for update in updates:
            if update.update_id and update.update_id in event_map:
                for event in event_map.get(update.update_id, []):
                    if (
                        event.event_type == "dataset_definition"
                        and event.origin
                        and event.origin.dataset_name
                    ):
                        names.add(event.origin.dataset_name)
            if self._is_full_update(update):
                break  # stop once we hit a full update

        return sorted(names)

    def _is_full_update(self, update_info: sp.UpdateInfo) -> bool:
        """Check if an update is a full graph update."""
        if update_info.state != "COMPLETED":
            return False
        frs = update_info.full_refresh_selection
        rs = update_info.refresh_selection
        return (not frs or len(frs) == 0) and (not rs or len(rs) == 0)

    def _is_relevant_dataset(self, fqn: str, q_conf: QuarantineConfig) -> bool:
        """Check if a dataset is relevant (not quarantine/validation table)."""
        fqn_parts = fqn.split(".")
        if len(fqn_parts) < 3:
            logger.debug("Invalid dataset name %s, expected catalog.schema.table", fqn)
            return False

        dataset_name = fqn_parts[-1]

        if dataset_name.startswith(q_conf.quarantine_prefix) and dataset_name.endswith(
            q_conf.quarantine_suffix,
        ):
            logger.debug("Skipping quarantined dataset %s", dataset_name)
            return False

        if dataset_name.startswith(q_conf.validation_prefix) and dataset_name.endswith(
            q_conf.validation_suffix,
        ):
            logger.debug("Skipping validation dataset %s", dataset_name)
            return False

        return True

    @staticmethod
    def _to_iso_ts(ms: int) -> str:
        """Convert millisecond epoch to RFC3339 UTC timestamp."""
        ts = dt.datetime.fromtimestamp(ms / 1000.0).replace(tzinfo=dt.UTC)
        return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    @classmethod
    def _extract_pipelines_from_target(cls, target_folder, target_name: str) -> list[PipelineInfo]:
        """Extract pipeline information from a target folder.

        Tries resources.json first, then falls back to terraform.tfstate or .backup variant.

        Args:
            target_folder: Path to the target folder
            target_name: Name of the target

        Returns:
            List of PipelineInfo found, empty list if none found
        """
        # Try resources.json first
        resource_path = target_folder / "resources.json"
        if resource_path.exists() and resource_path.is_file():
            try:
                with resource_path.open() as f:
                    resources = json.load(f)
                pipelines = []
                for res, data in resources.get("state", {}).items():
                    if res.startswith("resources.pipelines"):
                        pipeline_id = data.get("__id__")
                        pipeline_name = data.get("state", {}).get("name", "unknown")
                        if pipeline_id:
                            pipelines.append(
                                PipelineInfo(
                                    target=target_name,
                                    name=pipeline_name,
                                    id=pipeline_id,
                                )
                            )
                if pipelines:
                    logger.debug("Detected pipelines from resources.json: %s", pipelines)
                    return pipelines
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to parse resources.json: %s", e)

        # Fall back to terraform.tfstate
        terraform_state_path = target_folder / "terraform" / "terraform.tfstate"
        if not terraform_state_path.exists():
            # Try .backup variant
            terraform_state_path = target_folder / "terraform" / "terraform.tfstate.backup"

        if terraform_state_path.exists() and terraform_state_path.is_file():
            try:
                with terraform_state_path.open() as f:
                    tf_state = json.load(f)
                pipelines = []
                for resource in tf_state.get("resources", []):
                    if resource.get("type") == "databricks_pipeline":
                        for instance in resource.get("instances", []):
                            attributes = instance.get("attributes", {})
                            pipeline_id = attributes.get("id")
                            pipeline_name = attributes.get("name", "unknown")
                            if pipeline_id:
                                pipelines.append(
                                    PipelineInfo(
                                        target=target_name,
                                        name=pipeline_name,
                                        id=pipeline_id,
                                    )
                                )
                if pipelines:
                    logger.debug("Detected pipelines from terraform.tfstate: %s", pipelines)
                    return pipelines
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to parse terraform.tfstate: %s", e)

        return []

    @classmethod
    def detect_pipelines(cls, target: str | None = None) -> list[PipelineInfo]:
        """Detect pipelines in local files.

        Searches for pipelines from:
        1. .databricks/bundle/<target>/resources.json
        2. .databricks/bundle/<target>/terraform/terraform.tfstate

        If target is not specified, searches all available target folders.
        If multiple targets have pipelines, prompts user to select one.

        Args:
            target: Optional target name. If None, searches all targets.

        Returns:
            List of detected PipelineInfo.
        """
        folder = find_path_by_name(".", ".databricks")
        if not folder:
            logger.warning(
                "No .databricks folder found for pipeline detection",
            )
            return []

        bundle_folder = folder / "bundle"
        if not bundle_folder.exists():
            logger.warning(
                "No .databricks/bundle folder found for pipeline detection",
            )
            return []

        # If target is specified, search only that target
        if target:
            target_folder = bundle_folder / target
            if not target_folder.exists():
                logger.warning(
                    "No .databricks/bundle/%s folder found for pipeline detection",
                    target,
                )
                return []
            return cls._extract_pipelines_from_target(target_folder, target)

        # If target is not specified, search all available target folders
        logger.debug("No target specified, searching all available targets for pipelines")
        if not bundle_folder.is_dir():
            logger.warning(".databricks/bundle is not a directory")
            return []

        all_pipelines_by_target: dict[str, list[PipelineInfo]] = {}
        for target_dir in sorted(bundle_folder.iterdir()):
            if not target_dir.is_dir():
                continue
            target_name = target_dir.name
            logger.debug("Checking target folder: %s", target_name)
            pipelines = cls._extract_pipelines_from_target(target_dir, target_name)
            if pipelines:
                all_pipelines_by_target[target_name] = pipelines
                logger.info(
                    "Found %d pipeline(s) in target '%s'",
                    len(pipelines),
                    target_name,
                )

        if not all_pipelines_by_target:
            logger.warning(
                "No resources.json or terraform.tfstate found in any target folder",
            )
            return []

        # If only one target has pipelines, return them
        if len(all_pipelines_by_target) == 1:
            return next(iter(all_pipelines_by_target.values()))

        # Multiple targets have pipelines - prompt user to select
        import typer

        typer.echo("\nMultiple targets with pipelines found:")
        targets_list = sorted(all_pipelines_by_target.keys())
        for i, target_name in enumerate(targets_list, 1):
            pipelines = all_pipelines_by_target[target_name]
            pipeline_names = ", ".join(p.name for p in pipelines)
            typer.echo(f"  {i}. {target_name}: {pipeline_names}")

        choice = typer.prompt("Select target", type=int)
        if 1 <= choice <= len(targets_list):
            selected_target = targets_list[choice - 1]
            logger.info("User selected target: %s", selected_target)
            return all_pipelines_by_target[selected_target]

        typer.secho("Invalid selection", fg=typer.colors.RED)
        raise typer.Exit(1)

    @classmethod
    def detect_pipeline_ids(cls, target: str | None = None) -> list[str]:
        """Detect pipeline IDs (deprecated - use detect_pipelines instead).

        Args:
            target: Optional target name.

        Returns:
            List of detected pipeline IDs.
        """
        pipelines = cls.detect_pipelines(target=target)
        return [p.id for p in pipelines]
