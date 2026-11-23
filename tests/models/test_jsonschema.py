"""Tests for Kelp project JSON schema generation."""

from kelp.models.jsonschema import generate_json_schema


def test_project_hierarchy_schema_validates_grants_at_folder_levels() -> None:
    schema = generate_json_schema()
    hierarchy = schema["$defs"]["GrantHierarchyNode"]

    assert hierarchy["patternProperties"][r"^\+grants$"] == {
        "type": "array",
        "items": {"$ref": "#/$defs/Grant"},
    }
    properties = schema["$defs"]["ProjectConfig"]["properties"]
    for key in ("models", "metric_views", "functions"):
        assert {"$ref": "#/$defs/GrantHierarchyNode"} in properties[key]["allOf"]
