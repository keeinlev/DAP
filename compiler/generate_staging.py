from pathlib import Path
from util import parse_event_yamls
from model.field_type import FieldType

EVENT_DIR = Path("configs/events")
OUT_DIR = Path("dbt/models/generated/staging")
OUT_DIR.mkdir(parents=True, exist_ok=True)

schema_yaml = {
    "version": 2,
    "models": []
}

for schema in parse_event_yamls(EVENT_DIR):
    name = schema.name
    sf_fields = []
    for field in schema.fields:
        col = field.name
        typ = field.type
        snowflake_type = "STRING"
        if typ == FieldType.FLOAT:
            snowflake_type = "FLOAT"
        elif typ == FieldType.TIMESTAMP:
            snowflake_type = "TIMESTAMP"
        elif typ == FieldType.INT:
            snowflake_type = "NUMBER"
        elif typ == FieldType.BOOLEAN:
            snowflake_type = "BOOLEAN"

        sf_fields.append(f"data:{col}::{snowflake_type} AS {col}")
    fields_str = ",\n        ".join(sf_fields)
    sql = f"""
WITH base AS (
    SELECT
        MD5(TO_VARCHAR(data) || TO_VARCHAR(ingested_at)) AS event_id,
        {fields_str},
        ingested_at AS _ingested_at
    FROM {{{{ source('raw', 'events') }}}}
    WHERE event_name = '{name}'
    {{% if is_incremental() %}}
    AND ingested_at > (SELECT max(_ingested_at) FROM {{{{ this }}}})
    {{% endif %}}
)
SELECT * FROM base
"""

    (OUT_DIR / f"stg_{name}.sql").write_text(sql)

    schema_yaml["models"].append({
        "name": f"stg_{name}",
        "columns": [
            {"name": f.name, "tests": ["not_null"]}
            for f in schema.fields
        ] + [{"name": "event_id", "tests": ["not_null"]}]
    })

import yaml
(OUT_DIR / Path("schema.yml")).write_text(yaml.dump(schema_yaml))