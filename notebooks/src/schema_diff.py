import json

# -------------------------------
# Load schema JSON from file
# -------------------------------
def load_schema(path):
    with open(path, "r") as f:
        return json.load(f)
    
# -------------------------------
# Core schema comparison function
# -------------------------------
def compute_schema_diff(old, new, path_prefix = ""):
    """
    Recursively compares old and new schema definitions to detect:
    - Added fields
    - Removed fields
    - Type-changed fields
    - (Renames handled separately)

    Args:
        old (dict): Old schema with "fields"
        new (dict): New schema with "fields"
        path_prefix (str): Dotted path for nested fields

    Returns:
        dict: {
            "added": [...],
            "removed": [...],
            "type_changed": [...],
            "renamed": []  # to be filled separately
        }
    """
    diff = {"added": [], "removed": [], "type_changed": [], "renamed": []}
    old_fields = {f['name']: f['type'] for f in old['fields']}
    new_fields = {f['name']: f['type'] for f in new['fields']}

    for field_name, new_type in new_fields.items():
        full_name = f"{path_prefix}.{field_name}" if path_prefix else field_name

        if field_name not in old_fields:
            diff['added'].append(full_name)
        else:
            old_type = old_fields[field_name]

            if isinstance(new_type, dict) and isinstance(old_type, dict):
                nested_diff = compute_schema_diff(
                    {"fields": old_type["fields"]},
                    {"fields": new_type["fields"]},
                    path_prefix = full_name
                )

                for key in diff:
                    diff[key].extend(nested_diff[key])
            elif old_type != new_type:
                diff['type_changed'].append({
                        'col': full_name,
                        'from': old_type,
                        'to': new_type
                    })

    for field_name in old_fields:
        if field_name not in new_fields:
            full_name = f"{path_prefix}.{field_name}" if path_prefix else field_name
            diff['removed'].append(full_name)

    return diff

# -------------------------------
# Detect likely renames post-diff
# -------------------------------
def detect_renames(diff, old_fields_map, new_fields_map, rename_hints=None):
    """
    Detect renamed fields by matching added + removed fields.

    Args:
        diff (dict): Schema diff result from compute_schema_diff
        old_fields_map (dict): field name → type in old schema
        new_fields_map (dict): field name → type in new schema
        rename_hints (dict, optional): Manual overrides {old_name: new_name}

    Returns:
        None: Updates diff in-place (renamed, added, removed)
    """
    renamed = []

    # Apply rename hints first
    if rename_hints:
        for old_name, new_name in rename_hints.items():
            if old_name in diff["removed"] and new_name in diff["added"]:
                if old_fields_map.get(old_name) == new_fields_map.get(new_name):
                    renamed.append({"from": old_name, "to": new_name})
                    diff["removed"].remove(old_name)
                    diff["added"].remove(new_name)

    # Heuristic detection (based on name + type match)
    for added in diff["added"][:]:
        for removed in diff["removed"][:]:
            if added.lower().startswith(removed.lower()) or removed.lower().startswith(added.lower()):
                if old_fields_map.get(removed) == new_fields_map.get(added):
                    renamed.append({"from": removed, "to": added})
                    diff["removed"].remove(removed)
                    diff["added"].remove(added)
                    break

    diff["renamed"].extend(renamed)

# -------------------------------
# One-call wrapper for full drift detection
# -------------------------------
def schema_drift_agent(old_schema, new_schema, rename_hints=None):
    """
    Full agent that computes schema drift including rename detection.

    Args:
        old_schema (dict): Old schema JSON
        new_schema (dict): New schema JSON
        rename_hints (dict): Optional manual rename mappings

    Returns:
        dict: Schema drift result with added, removed, type_changed, renamed
    """
    diff = compute_schema_diff(old_schema, new_schema)

    # Flatten top-level field names only (dot-prefixed nested not supported here)
    old_map = {f["name"]: f["type"] for f in old_schema["fields"]}
    new_map = {f["name"]: f["type"] for f in new_schema["fields"]}

    detect_renames(diff, old_map, new_map, rename_hints)
    return diff