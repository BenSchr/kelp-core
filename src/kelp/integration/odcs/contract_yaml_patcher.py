def _list_item_identity(item: dict) -> str | None:
    """Build an identity key for dict list merge matching."""
    for key in ("name", "id", "property"):
        value = item.get(key)
        if isinstance(value, str):
            return f"{key}:{value}"
    return None


def merge_preserving_existing(existing, incoming):
    """Deep-merge incoming values while preserving local-only keys.

    Unlike model YAML patching, this merge does NOT preserve ${...} template
    scalar values in contract documents.
    """
    if isinstance(existing, dict) and isinstance(incoming, dict):
        merged = dict(existing)
        for key, value in incoming.items():
            if key in merged:
                merged[key] = merge_preserving_existing(merged[key], value)
            else:
                merged[key] = value
        return merged

    if isinstance(existing, list) and isinstance(incoming, list):
        if all(isinstance(i, dict) for i in existing) and all(
            isinstance(i, dict) for i in incoming
        ):
            existing_by_id = {
                _list_item_identity(item): item
                for item in existing
                if _list_item_identity(item) is not None
            }
            if existing_by_id:
                merged_list = []
                used_ids: set[str] = set()
                incoming_ids = [
                    _list_item_identity(new_item)
                    for new_item in incoming
                    if isinstance(new_item, dict)
                ]

                for item in existing:
                    item_id = _list_item_identity(item)
                    if item_id and item_id in incoming_ids:
                        incoming_item = next(
                            (
                                new_item
                                for new_item in incoming
                                if isinstance(new_item, dict)
                                and _list_item_identity(new_item) == item_id
                            ),
                            None,
                        )
                        if incoming_item is not None:
                            merged_list.append(merge_preserving_existing(item, incoming_item))
                            used_ids.add(item_id)
                        else:
                            merged_list.append(item)
                    else:
                        merged_list.append(item)

                for item in incoming:
                    item_id = _list_item_identity(item) if isinstance(item, dict) else None
                    if (item_id is None or item_id not in used_ids) and item not in merged_list:
                        merged_list.append(item)
                return merged_list

        merged_list = list(existing)
        for item in incoming:
            if item not in merged_list:
                merged_list.append(item)
        return merged_list

    return incoming


def patch_contract_yaml_document(existing_doc: dict, incoming_doc: dict) -> dict:
    """Patch existing ODCS contract doc with incoming single-schema update.

    Matches schema by `name` first, then `physicalName`, and only patches the
    matched schema while preserving others.
    """
    patched = dict(existing_doc)

    for key, value in incoming_doc.items():
        if key == "schema":
            continue
        if key in patched:
            patched[key] = merge_preserving_existing(patched[key], value)
        else:
            patched[key] = value

    incoming_schemas = incoming_doc.get("schema")
    if not isinstance(incoming_schemas, list) or not incoming_schemas:
        return patched

    incoming_schema = incoming_schemas[0]
    if not isinstance(incoming_schema, dict):
        return patched

    existing_schemas = patched.get("schema")
    if not isinstance(existing_schemas, list):
        existing_schemas = []

    incoming_name = incoming_schema.get("name")
    incoming_physical_name = incoming_schema.get("physicalName")

    match_index = None
    for index, schema in enumerate(existing_schemas):
        if not isinstance(schema, dict):
            continue
        if incoming_name and schema.get("name") == incoming_name:
            match_index = index
            break

    if match_index is None and incoming_physical_name:
        for index, schema in enumerate(existing_schemas):
            if not isinstance(schema, dict):
                continue
            if schema.get("physicalName") == incoming_physical_name:
                match_index = index
                break

    if match_index is None:
        existing_schemas.append(incoming_schema)
    else:
        existing_schemas[match_index] = merge_preserving_existing(
            existing_schemas[match_index],
            incoming_schema,
        )

    patched["schema"] = existing_schemas
    return patched
