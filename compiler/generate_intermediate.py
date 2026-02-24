from pathlib import Path
import yaml

EVENT_GROUPS_DIR = Path("configs/event_groups")
OUT_DIR = Path("dbt/models/intermediate")
OUT_DIR.mkdir(parents=True, exist_ok=True)

schema_yaml = {
    "version": 2,
    "models": []
}

for path in EVENT_GROUPS_DIR.glob("*.yaml"):
    event_group = yaml.safe_load(open(path))
    events = event_group["events"]
    name = event_group["name"]
    filename = f"int_{name}"

    schema = {
        "event_type": "string",
        "event_id": "string",
        event_group["actor_key"]: "string",
        event_group["session_key"]: "string",
        event_group["object_key"]: "string",
        "ts": "timestamp",
        "_ingested_at": "timestamp",
    }

    selects = []

    for event in events:
        selects.append(f"""
  SELECT
    '{event}' AS event_type,
    event_id,
    {event_group["actor_key"]},
    {event_group["session_key"]},
    {event_group["object_key"]},
    ts,
    _ingested_at
  FROM {{{{ ref('stg_{event}') }}}}
""")

    sql = "WITH unioned AS (" + "\n  UNION ALL\n".join(selects) + """),

ordered AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY request_id
      ORDER BY ts
    ) AS event_seq
  FROM unioned
)

SELECT *
FROM ordered
"""


    (OUT_DIR / f"{filename}.sql").write_text(sql)

    schema_yaml["models"].append({
        "name": filename,
        "columns": [
            {"name": c, "tests": ["not_null"]}
            for c in schema.keys()
        ]
    })

import yaml
(OUT_DIR / Path("schema.yml")).write_text(yaml.dump(schema_yaml))