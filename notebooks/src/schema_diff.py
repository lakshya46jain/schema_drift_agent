import json

# Loading the JSON file
def load_schema(path):
    with open(path, "r") as f:
        return json.load(f)
    
# Computing the difference between the 2 schemas
def compute_schema_diff(old, new, path_prefix = ""):
    diff = {"added": [], "removed": [], "type_changed": []}
    old_fields = {f['name']: f['type'] for f in old['fields']}
    new_fields = {f['name']: f['type'] for f in new['fields']}

    for field_name, new_type in new_fields.items():
        full_name = ""
        
        if path_prefix:
            full_name = f"{path_prefix}.{field_name}"
        else:
            full_name = field_name

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
            full_name = ""

            if path_prefix:
                full_name = f"{path_prefix}.{field_name}"
            else:
                full_name = field_name

            diff['removed'].append(full_name)

    return diff