from __future__ import annotations

import datetime as dt
import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines as sp

from kelp.models.project_config import QuarantineConfig
from kelp.models.table import Table
from kelp.utils.common import find_path_by_name
from kelp.utils.databricks import get_table_from_dbx_sdk

logger = logging.getLogger(__name__)


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
        logger.info(f"Fetching tables from pipeline {pipeline_id}")

        # Get pipeline updates
        updates = self._fetch_updates(pipeline_id)
        if not updates:
            logger.warning(f"No updates found for pipeline {pipeline_id}")
            return []

        # Fetch events for the updates
        event_map = self._events_for_window(pipeline_id, updates)

        # Extract dataset names
        dataset_names = self._extract_dataset_names(updates, event_map)
        logger.info(f"Found {len(dataset_names)} datasets in pipeline")

        # Filter relevant datasets
        if quarantine_config:
            dataset_names = [
                name for name in dataset_names if self._is_relevant_dataset(name, quarantine_config)
            ]
            logger.info(f"Filtered to {len(dataset_names)} relevant datasets")

        # Fetch table metadata from Databricks
        tables = []
        for dataset_name in dataset_names:
            try:
                table = get_table_from_dbx_sdk(dataset_name, w=self.w)
                tables.append(table)
                logger.debug(f"Fetched metadata for {dataset_name}")
            except Exception as e:
                logger.warning(f"Failed to fetch table {dataset_name}: {e}")
                continue

        logger.info(f"Successfully fetched {len(tables)} tables")
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

        start_iso = self._to_iso_ts(updates[0].creation_time)
        end_iso = None
        if len(updates) > 1:
            end_iso = self._to_iso_ts(updates[1].creation_time)

        logger.debug(f"Fetching events for window [{start_iso}, {end_iso})")
        event_filter = f"timestamp <= '{start_iso}'"
        if end_iso:
            event_filter += f" AND timestamp >= '{end_iso}'"

        events = list(
            self.w.pipelines.list_pipeline_events(pipeline_id=pipeline_id, filter=event_filter)
        )

        update_event_map = {k: [] for k in set(u.update_id for u in updates)}
        for event in events:
            if event.origin.update_id in update_event_map:
                update_event_map[event.origin.update_id].append(event)

        return update_event_map

    def _extract_dataset_names(
        self, updates: list[sp.UpdateInfo], event_map: dict[str, list[sp.PipelineEvent]]
    ) -> list[str]:
        """Extract unique dataset names from pipeline events."""
        names: set[str] = set()
        for update in updates:
            for event in event_map.get(update.update_id, []):
                if event.event_type == "dataset_definition":
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
            logger.debug(f"Invalid dataset name {fqn}, expected catalog.schema.table")
            return False

        dataset_name = fqn_parts[-1]

        if dataset_name.startswith(q_conf.quarantine_prefix) and dataset_name.endswith(
            q_conf.quarantine_suffix
        ):
            logger.debug(f"Skipping quarantined dataset {dataset_name}")
            return False

        if dataset_name.startswith(q_conf.validation_prefix) and dataset_name.endswith(
            q_conf.validation_suffix
        ):
            logger.debug(f"Skipping validation dataset {dataset_name}")
            return False

        return True

    @staticmethod
    def _to_iso_ts(ms: int) -> str:
        """Convert millisecond epoch to RFC3339 UTC timestamp."""
        ts = dt.datetime.fromtimestamp(ms / 1000.0).replace(tzinfo=dt.UTC)
        return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    @classmethod
    def detect_pipeline_ids(self, target) -> list[str]:
        """Detect pipeline IDs in local files"""
        # search for .databricks/bundle/<target> folder
        # look for resources.json and extract pipeline_id from there
        # return list of unique pipeline_ids found

        # Find .databricks/bundle/<target>/**/*.json files search childs and parents two times both
        folder = find_path_by_name(".", ".databricks")
        if not folder:
            logger.warning(f"No .databricks/bundle/{target} folder found for pipeline detection")
            return []
        target_folder = folder / "bundle" / target
        if not target_folder.exists():
            logger.warning(f"No .databricks/bundle/{target} folder found for pipeline detection")
            return []
        resource_path = target_folder / "resources.json"
        if not resource_path.exists() or not resource_path.is_file():
            logger.warning(f"No resources.json found in {folder} for pipeline detection")
            return []
        try:
            import json

            with open(resource_path) as f:
                resources = json.load(f)
            pipeline_ids = set()
            for res, data in resources["state"].items():
                if res.startswith("resources.pipelines"):
                    id = data.get("__id__")
                    if id:
                        pipeline_ids.add(id)
        except Exception as e:
            logger.warning(f"Failed to read resources.json for pipeline detection: {e}")
            return []

        logger.debug(f"Detected pipeline IDs: {pipeline_ids}")
        return list(pipeline_ids)
