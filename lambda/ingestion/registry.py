import json
from pathlib import Path

SCHEMA_DIR = Path("generated/schemas")

REGISTRY = {}

def load_schemas():
    for event_dir in SCHEMA_DIR.iterdir():
        event_type = event_dir.name
        REGISTRY[event_type] = {}

        for version_file in event_dir.glob("v*.json"):
            version = version_file.stem[1:]  # remove "v"
            with open(version_file) as f:
                REGISTRY[event_type][version] = json.load(f)

load_schemas()
