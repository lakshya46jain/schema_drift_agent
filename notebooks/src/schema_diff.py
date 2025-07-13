import json

# Loading the JSON file
def load_schema(path):
    with open(path, "r") as f:
        return json.load(f)
    
# Computing the difference between the 2 schemas
def compute_schema_diff(old, new):
    diff = {"added": [], "removed": [], "type_changed": []}
    old_fields = {f['name']: f['type'] for f in old['fields']}
    new_fields = {f['name']: f['type'] for f in new['fields']}

    for name, t in new_fields.items():
        if name not in old_fields:
            diff['added'].append(name)
        elif old_fields[name] != t:
            diff['type_changed'].append({'col': name, 'from': old_fields[name], 'to': t})

    for name in old_fields:
        if name not in new_fields:
            diff['removed'].append(name)

    return diff