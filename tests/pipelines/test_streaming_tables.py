"""Tests for kelp.pipelines.streaming_tables wrappers."""

from unittest.mock import MagicMock, patch

from kelp.pipelines import streaming_tables


def test_materialized_view_merges_metadata_params() -> None:
    """materialized_view should merge call-site params with metadata and call dp decorator."""
    mock_sdp_table = MagicMock()
    mock_sdp_table.params.return_value = {
        "name": "catalog.schema.my_mv",
        "comment": "metadata comment",
    }

    with (
        patch("kelp.pipelines.streaming_tables.ModelManager.build_sdp_model") as mock_build,
        patch("kelp.pipelines.streaming_tables.dp.materialized_view") as mock_dp_mv,
    ):
        mock_build.return_value = mock_sdp_table
        mock_dp_mv.return_value = lambda f: f

        def my_mv():
            return None

        decorator = streaming_tables.materialized_view(name="my_mv", private=True)
        assert decorator is not None
        decorator(my_mv)

        mock_build.assert_called_once_with("my_mv", soft_handle=True)
        mock_sdp_table.params.assert_called_once_with(exclude=[])
        assert mock_dp_mv.called
        params = mock_dp_mv.call_args.kwargs
        assert params["name"] == "catalog.schema.my_mv"
        assert params["comment"] == "metadata comment"
        assert params["private"] is True


def test_materialized_view_supports_no_parentheses_form() -> None:
    """materialized_view should support @decorator form without parentheses."""
    mock_sdp_table = MagicMock()
    mock_sdp_table.params.return_value = {"name": "catalog.schema.inline_mv"}

    with (
        patch("kelp.pipelines.streaming_tables.ModelManager.build_sdp_model") as mock_build,
        patch("kelp.pipelines.streaming_tables.dp.materialized_view") as mock_dp_mv,
    ):
        mock_build.return_value = mock_sdp_table
        mock_dp_mv.return_value = lambda f: f

        def inline_mv():
            return None

        result = streaming_tables.materialized_view(inline_mv)

        assert result is None
        mock_build.assert_called_once_with("inline_mv", soft_handle=True)
        mock_sdp_table.params.assert_called_once_with(exclude=[])
        mock_dp_mv.assert_called_once()
