import yaml
from model import *

nlstc = "\n    , "
nldtc = "\n        , "
nlttc = "\n            , "

nlsta = "\n    AND "
nldta = "\n        AND "
nltta = "\n            AND "

def parse_event_yamls(dir) -> list[EventSchema]:
    schemas = []
    for path in dir.glob("*.yaml"):
        event = yaml.safe_load(open(path))
        fields = event["schema"]
        schemas.append(EventSchema(
            event["name"],
            event["version"],
            [ Field(fname, FieldType.parse(f["type"]), f["required"]) for fname, f in fields.items() ]
        ))
    return schemas
