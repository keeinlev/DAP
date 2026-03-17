import yaml
import glob
import logging

logger = logging.getLogger()

def load_yaml_file(filepath):
    """Loads a single YAML file safely into a Python dictionary."""
    with open(filepath, 'r') as file:
        data = yaml.safe_load(file)
    return data

def merge_yaml_files(file_patterns):
    """
    Merges content from multiple YAML files found by file_patterns 
    into a single dictionary.
    """
    merged_data = {}
    for pattern in file_patterns:
        for filepath in glob.glob(pattern):
            print(f"Merging file: {filepath}")
            data = load_yaml_file(filepath)
            if isinstance(data, dict):
                if "name" in data and data["name"] not in merged_data:
                    merged_data[data.pop("name")] = data
                else:
                    logger.warning(f"{filepath} either did not have a `name` field or had a duplicate event name and was skipped.")
            else:
                logger.warning(f"{filepath} did not load a dictionary and was skipped.")
    return merged_data


EVENTS = merge_yaml_files(["/events/*.yaml"])

print(EVENTS)

TYPE_MAP = {
    "string": str,
    "int": int,
    "float": float,
    "boolean": bool,
    "timestamp": int,
}


def validate_event(event_name, data):
    if event_name not in EVENTS:
        raise ValueError(f"Unknown event: {event_name}")

    schema = EVENTS[event_name]["schema"]

    for field, field_type in schema.items():
        if field not in data:
            raise ValueError(f"Missing field: {field}")

        expected = TYPE_MAP.get(field_type)
        if expected and not isinstance(data[field], expected):
            raise ValueError(f"{field} must be {field_type}")
