from pathlib import Path
import yaml
from util import *

MART_PIPELINES_DIR = Path("configs/pipelines")
OUT_DIR = Path("dbt/models/mart")
OUT_DIR.mkdir(parents=True, exist_ok=True)

schema_yaml = {
    "version": 2,
    "models": []
}

for path in MART_PIPELINES_DIR.glob("*.yaml"):

    # Extract the yaml body
    pipeline_spec = yaml.safe_load(open(path))
    source = pipeline_spec["source"]
    group_keys = pipeline_spec["group_keys"]
    name = pipeline_spec["name"]
    metrics = pipeline_spec["metrics"]
    incremental_time_field = pipeline_spec["incremental_time_field"]
    row_differentiator = pipeline_spec["source_row_differentiator"]
    filename = f"mart_{name}"

    # For dbt auto tests
    schema = { k: v["type"] for k, v in group_keys.items() }

    # Do some string manips to get clean SQL aliases and expressions
    source_alias = "".join([ s[0] for s in source.split("_") ])
    group_key_strs = {}
    for gk, body in group_keys.items():
        if "transforms" not in body:
            # Tag ambiguous columns with the source alias
            # Allow injecting of any alias later
            group_key_strs[gk] = f"%s.{gk}"
        else:
            # Handle list of transforms, nest them appropriately
            assert "field" in body, f"if defining a group key with a list of transforms, the operand 'field' must be provided for source {source}"
            transforms = body['transforms']
            field = body['field']

            transform_str = ""
            transform_end_stack = []
            for transform in transforms:
                if isinstance(transform, str):
                    # Single input transform, just the transform name
                    transform_str += f"{transform}("
                    transform_end_stack.append(")") # Stack the closing parentheses so we can add them at the end
                elif isinstance(transform, dict):
                    # Multiple input transform, need to specify in the yaml what the other args are and where the actual event field is in the arg list
                    transform_name = transform["name"]
                    assert "args" in transform, f"if defining a transform with additional hardcoded positional arguments for transform {transform_name}, please specify the additional args under the 'args' key, in correct order"
                    assert "field_pos" in transform, f"if defining a transform with additional hardcoded positional arguments for transform {transform_name}, please specify the position of the field {field} under the 'field_pos' key, 0-indexed"
                    extra_args = [ f"'{arg}'" for arg in transform["args"] ]
                    field_pos = transform["field_pos"]
                    args1 = ", ".join(extra_args[:field_pos]) + (", " if field_pos > 0 else "") # The args before the event field
                    args2 = (", " if field_pos < len(extra_args) else "") + ", ".join(extra_args[field_pos:]) # The args after the event field
                    transform_str += f"{transform_name}({args1}"
                    transform_end_stack.append(args2 + ")") # Stack the second part of the args + the closing parenthesis

            transform_str += f"%s.{field}" # Allow for injecting any alias here later

            # Do the closing parentheses/second portion of args for each transform we nested
            for _ in range(len(transforms)):
                transform_str += transform_end_stack.pop()
            
            # This is where separating is important, since we want to hold onto the <expression AS alias> in the SELECT, but we only want <alias> for the GROUP BY
            group_key_strs[gk] = transform_str

    # We will populate these as we loop through metrics
    selects = []
    required_fields = set()
    required_joins = set()
    aliases = {source: source_alias}

    for metric, body in metrics.items():
        event_type = body["event"]
        agg_type = body["agg"]
        src_fields = body.get("fields", None)

        # Dynamically create short form table aliases
        event_alias = "".join([ s[0] for s in event_type.split("_") ])
        if event_type not in aliases:
            # Handle collisions on aliases, if the tables have the same initials
            i = 0
            while f"{event_alias}{'' if i == 0 else i}" in aliases.values():
                i += 1
                event_alias = f"{event_alias}{i}"
            aliases[event_type] = event_alias

        # Defining aggregation codegen
        if agg_type == "count":
            selects.append(f"COUNT(CASE WHEN {row_differentiator}='{event_type}' THEN 1 END) AS {metric}")
            schema[metric] = "int"

        elif agg_type == "avg":
            assert src_fields is not None, f"agg type 'avg' requires 'fields' to be non-empty list for metric {metric}"
            assert len(src_fields) == 1, f"agg type 'avg' requires 'fields' to be a list of length 1 for metric {metric}"

            field_new_alias = f"{event_alias}__{src_fields[0]}"
            required_joins.add(event_type)
            required_fields.add(f"{event_alias}.{src_fields[0]} AS {field_new_alias}")

            selects.append(f"AVG(CASE WHEN {row_differentiator}='{event_type}' THEN {field_new_alias} END) AS {metric}")
            schema[metric] = "float"

    # A bunch of string formatting and manipulation to get the right expressions
    group_key_selects = nldtc.join([ f"{v % source_alias} AS {k}" for k, v in group_key_strs.items() ])
    required_fields_selects = nlstc.join(required_fields)
    metric_selects = nlstc.join(selects)
    join_stmts = '\n'.join([ f"LEFT JOIN {{{{ ref('stg_{join_table}') }}}} {(join_alias := aliases[join_table])}\n    ON {source_alias}.event_id = {join_alias}.event_id\n    AND {source_alias}.{row_differentiator}='{join_table}'" for join_table in required_joins ])

    group_keys_str = nlstc.join(group_keys.keys())

    # Annoying but necessary for including all past events which share the same primary keys as new incoming events, so we don't overwrite past
    source1 = [ v % source_alias for v in group_key_strs.values() ]
    source2 = [ v % 'new_rows' for v in group_key_strs.values() ]
    incremental_filter = nldta.join([ f"{s1} = {s2}" for s1, s2 in zip(source1, source2) ])

    sql = f"""{{{{ config(
    materialized='incremental',
    unique_key=[{','.join([ f"'{k}'" for k in group_keys.keys() ])}]
) }}}}

WITH base AS (
    SELECT
        {source_alias}.{row_differentiator}
        , {source_alias}.{incremental_time_field}
        , {group_key_selects}
        , {required_fields_selects}
    FROM {{{{ ref('{source}') }}}} {source_alias}
    {join_stmts}
    {{% if is_incremental() %}}
    WHERE EXISTS (
        SELECT 1
        FROM {{{{ ref('{source}') }}}} new_rows
        WHERE new_rows.{incremental_time_field} > (
            SELECT COALESCE(MAX({{{{ this }}}}.{incremental_time_field}), '1900-01-01')
            FROM {{{{ this }}}}
        )
        AND {incremental_filter}
    )
    {{% endif %}}
)
SELECT
    MAX({incremental_time_field}) as {incremental_time_field}
    , {group_keys_str}
    , {metric_selects}
FROM base
GROUP BY\n    {group_keys_str}"""

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
