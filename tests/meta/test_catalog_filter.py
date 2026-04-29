"""Tests for MetaCatalog.filter_by and _is_filter_match."""

from __future__ import annotations

from types import SimpleNamespace

from kelp.meta.catalog_index import MetaCatalog, _is_filter_match


class TestIsFilterMatch:
    """Unit tests for the recursive filter matcher."""

    def test_scalar_match(self) -> None:
        assert _is_filter_match("abc", "abc") is True

    def test_scalar_no_match(self) -> None:
        assert _is_filter_match("abc", "xyz") is False

    def test_int_match(self) -> None:
        assert _is_filter_match(1, 1) is True
        assert _is_filter_match(1, 2) is False

    def test_bool_match(self) -> None:
        assert _is_filter_match(True, True) is True
        assert _is_filter_match(True, False) is False

    def test_dict_subset_match(self) -> None:
        actual = {"group": "abc", "team": "data"}
        assert _is_filter_match(actual, {"group": "abc"}) is True

    def test_dict_subset_no_match(self) -> None:
        actual = {"group": "abc"}
        assert _is_filter_match(actual, {"group": "xyz"}) is False

    def test_dict_missing_key(self) -> None:
        assert _is_filter_match({}, {"group": "abc"}) is False

    def test_dict_multiple_keys_and(self) -> None:
        actual = {"group": "abc", "team": "data"}
        assert _is_filter_match(actual, {"group": "abc", "team": "data"}) is True
        assert _is_filter_match(actual, {"group": "abc", "team": "other"}) is False

    def test_nested_dict(self) -> None:
        actual = {"deployment": {"group": "abc", "region": "eu"}}
        assert _is_filter_match(actual, {"deployment": {"group": "abc"}}) is True

    def test_nested_dict_no_match(self) -> None:
        actual = {"deployment": {"group": "abc"}}
        assert _is_filter_match(actual, {"deployment": {"group": "xyz"}}) is False

    def test_expected_dict_but_actual_scalar(self) -> None:
        assert _is_filter_match("flat", {"group": "abc"}) is False

    def test_deeply_nested(self) -> None:
        actual = {"a": {"b": {"c": 1}}}
        assert _is_filter_match(actual, {"a": {"b": {"c": 1}}}) is True
        assert _is_filter_match(actual, {"a": {"b": {"c": 2}}}) is False

    def test_sentinel_value_does_not_match(self) -> None:
        """When the attribute doesn't exist, _SENTINEL is passed as actual."""
        from kelp.meta.catalog_index import _SENTINEL

        assert _is_filter_match(_SENTINEL, "abc") is False


class TestFilterBy:
    """Tests for MetaCatalog.filter_by."""

    @staticmethod
    def _make_catalog(items: list[SimpleNamespace]) -> MetaCatalog:
        return MetaCatalog({"models": items})

    # -- scalar attr filtering --

    def test_filter_by_scalar_attr(self) -> None:
        a = SimpleNamespace(name="a", schema_="bronze")
        b = SimpleNamespace(name="b", schema_="silver")
        catalog = self._make_catalog([a, b])

        result = catalog.filter_by("models", "schema_", "bronze")

        assert result == [a]

    def test_filter_by_scalar_no_match(self) -> None:
        a = SimpleNamespace(name="a", schema_="bronze")
        catalog = self._make_catalog([a])

        result = catalog.filter_by("models", "schema_", "gold")

        assert result == []

    # -- dict attr filtering (meta-style) --

    def test_filter_by_dict_attr(self) -> None:
        a = SimpleNamespace(name="a", meta={"group": "abc"})
        b = SimpleNamespace(name="b", meta={"group": "xyz"})
        c = SimpleNamespace(name="c", meta={})
        catalog = self._make_catalog([a, b, c])

        result = catalog.filter_by("models", "meta", {"group": "abc"})

        assert result == [a]

    def test_filter_by_nested_dict_attr(self) -> None:
        a = SimpleNamespace(name="a", meta={"deploy": {"env": "prod"}})
        b = SimpleNamespace(name="b", meta={"deploy": {"env": "dev"}})
        catalog = self._make_catalog([a, b])

        result = catalog.filter_by("models", "meta", {"deploy": {"env": "prod"}})

        assert result == [a]

    # -- missing attr --

    def test_objects_without_attr_are_skipped(self) -> None:
        a = SimpleNamespace(name="a")  # no schema_ attribute
        b = SimpleNamespace(name="b", schema_="bronze")
        catalog = self._make_catalog([a, b])

        result = catalog.filter_by("models", "schema_", "bronze")

        assert result == [b]

    # -- caching --

    def test_cache_returns_same_result(self) -> None:
        a = SimpleNamespace(name="a", meta={"group": "abc"})
        catalog = self._make_catalog([a])

        first = catalog.filter_by("models", "meta", {"group": "abc"})
        second = catalog.filter_by("models", "meta", {"group": "abc"})

        assert first is second  # cached instance

    def test_refresh_index_clears_filter_cache(self) -> None:
        a = SimpleNamespace(name="a", meta={"group": "abc"})
        catalog = self._make_catalog([a])

        first = catalog.filter_by("models", "meta", {"group": "abc"})
        catalog.refresh_index("models")
        second = catalog.filter_by("models", "meta", {"group": "abc"})

        assert first == second
        assert first is not second  # cache was cleared

    # -- dict objects --

    def test_dict_objects_use_dict_access(self) -> None:
        catalog = MetaCatalog(
            {"models": [{"name": "a", "meta": {"group": "abc"}}, {"name": "b", "meta": {}}]}
        )

        result = catalog.filter_by("models", "meta", {"group": "abc"})

        assert len(result) == 1
        assert result[0]["name"] == "a"

    def test_dict_objects_scalar_filter(self) -> None:
        catalog = MetaCatalog(
            {"models": [{"name": "a", "schema": "bronze"}, {"name": "b", "schema": "silver"}]}
        )

        result = catalog.filter_by("models", "schema", "bronze")

        assert len(result) == 1
        assert result[0]["name"] == "a"
