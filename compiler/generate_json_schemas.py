import json
import os
from pathlib import Path
from util import parse_event_yamls
from model.field_type import FieldType

EVENT_DIR = Path("configs/events")
PARENT_DIR = Path("lambda/generated/schemas")

JSON_FIELD_MAP = {
    FieldType.STRING: {"type": "string"},
    FieldType.FLOAT: {"type": "number"},
    FieldType.TIMESTAMP: {"type": "string", "format": "date-time"},
    FieldType.INT: {"type": "integer"},
    FieldType.BOOLEAN: {"type": "boolean"},
    FieldType.JSON: {"type": "object"},
}

for schema in parse_event_yamls(EVENT_DIR):
    out_dir = PARENT_DIR / schema.name
    out_dir.mkdir(parents=True, exist_ok=True)
    version = schema.version
    out_json = {"type": "object"}
    properties = {}
    required = []
    for field in schema.fields:
        col = field.name
        typ = field.type
        properties[col] = JSON_FIELD_MAP[typ]
        if field.required:
            required.append(col)
    
    out_json["properties"] = properties
    if required:
        out_json["required"] = required

    with open(out_dir / f"v{version}.json", "w") as f:
        json.dump(out_json, f, indent=2, sort_keys=True)
