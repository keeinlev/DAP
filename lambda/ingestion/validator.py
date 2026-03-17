from jsonschema import validate
from registry import REGISTRY

def validate_event(event):
    event_type = event["event_type"]
    version = str(event["event_version"])
    payload = event["payload"]

    schema = REGISTRY[event_type][version]

    validate(instance=payload, schema=schema)
