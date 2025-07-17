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
                # Mark as renamed
                renamed.append({"from": old_name, "to": new_name})
                diff["removed"].remove(old_name)
                diff["added"].remove(new_name)

                # Track type change if types differ
                if old_name in old_fields_map and new_name in new_fields_map:
                    if old_fields_map[old_name] != new_fields_map[new_name]:
                        diff["type_changed"].append({
                            'col': new_name,
                            'from': old_fields_map[old_name],
                            'to': new_fields_map[new_name]
                        })

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

    # Flatten top-level and nested fields for rename/type tracking
    def flatten_fields(schema, prefix=""):
        field_map = {}
        for field in schema["fields"]:
            name = field["name"]
            full_name = f"{prefix}.{name}" if prefix else name
            f_type = field["type"]

            if isinstance(f_type, dict) and f_type.get("type") == "struct":
                nested = flatten_fields({"fields": f_type["fields"]}, full_name)
                field_map.update(nested)
            else:
                field_map[full_name] = f_type
        return field_map

    old_map = flatten_fields(old_schema)
    new_map = flatten_fields(new_schema)

    detect_renames(diff, old_map, new_map, rename_hints)
    return diff