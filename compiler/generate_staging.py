from pathlib import Path
import yaml

EVENT_DIR = Path("configs/events")
OUT_DIR = Path("dbt/models/staging")
OUT_DIR.mkdir(parents=True, exist_ok=True)

schema_yaml = {
    "version": 1,
    "models": []
}

for path in EVENT_DIR.glob("*.yaml"):
    event = yaml.safe_load(open(path))
    name = event["name"]
    schema = event["schema"]

    fields = []
    for col, typ in schema.items():
        snowflake_type = "STRING"
        if typ == "float":
            snowflake_type = "FLOAT"
        elif typ == "timestamp":
            snowflake_type = "TIMESTAMP"

        fields.append(f"data:{col}::{snowflake_type} AS {col}")

    fields_str = ",\n        ".join(fields)
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

    schema["event_id"] = "string"

    schema_yaml["models"].append({
        "name": f"stg_{name}",
        "columns": [
            {"name": c, "tests": ["not_null"]}
            for c in schema.keys()
        ]
    })

import yaml
(Path("dbt/models/staging/schema.yml")).write_text(yaml.dump(schema_yaml))