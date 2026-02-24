from pathlib import Path
import yaml

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
    filename = f"mart_{name}"

    # For dbt auto tests
    schema = { k: v["type"] for k, v in group_keys.items() }

    # Do some string manips to get clean SQL aliases and expressions
    source_alias = "".join([ s[0] for s in source.split("_") ])
    group_key_strs = {}
    for gk, body in group_keys.items():
        if "transforms" not in body:
            # Tag ambiguous columns with the source alias
            # Key to separate select vs group by expressions for when we need to handle column transformations that map to new column aliases
            group_key_strs[gk] = {
                "select_expr": f"{source_alias}.{gk}",
                "groupby_expr": f"{source_alias}.{gk}",
            }
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

            transform_str += f"{source_alias}.{field}" # Inject the event field

            # Do the closing parentheses/second portion of args for each transform we nested
            for _ in range(len(transforms)):
                transform_str += transform_end_stack.pop()
            
            # This is where separating is important, since we want to hold onto the <expression AS alias> in the SELECT, but we only want <alias> for the GROUP BY
            group_key_strs[gk] = {
                "select_expr": transform_str,
                "groupby_expr": gk,
            }

    # We will populate these as we loop through metrics
    selects = []
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

        if agg_type == "count":
            selects.append(f"COUNT(CASE WHEN {source_alias}.event_type='{event_type}' THEN 1 END) AS {metric}")
            schema[metric] = "int"

        elif agg_type == "avg":
            assert src_fields is not None, f"agg type 'avg' requires 'fields' to be non-empty list for metric {metric}"
            assert len(src_fields) == 1, f"agg type 'avg' requires 'fields' to be a list of length 1 for metric {metric}"

            required_joins.add(event_type)

            selects.append(f"AVG(CASE WHEN {source_alias}.event_type='{event_type}' THEN {aliases[event_type]}.{src_fields[0]} END) AS {metric}")
            schema[metric] = "float"
    
    group_key_selects = '\n    , '.join([ f"{v['select_expr']} AS {k}" for k, v in group_key_strs.items() ])
    metric_selects = '\n    , '.join(selects)
    join_stmts = '\n'.join([ f"LEFT JOIN {{{{ ref('stg_{join_table}') }}}} {(join_alias := aliases[join_table])}\n    ON {source_alias}.event_id = {join_alias}.event_id\n    AND {source_alias}.event_type='{join_table}'" for join_table in required_joins ])
    group_bys = '\n    , '.join([ v["groupby_expr"] for v in group_key_strs.values() ])

    sql = f"""SELECT
    {group_key_selects}
    , {metric_selects}
FROM {{{{ ref('{source}') }}}} {source_alias}
{join_stmts}
GROUP BY\n    {group_bys}"""

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
