# Declarative Analytics Platform (DAP)
## February 23, 2026 - Present

![dap](https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExODhoNjVkOG05aXZ3anBnbXlneTg0cTJvdHd2Y29wczZib2g5c2tkYiZlcD12MV9naWZzX3NlYXJjaCZjdD1n/zCHnEV6OQXQoZKEabT/giphy.gif)

DAP is a small declarative analytics system for defining metrics in YAML and compiling them into dbt models on Snowflake.

The goal is to separate *metric intent* from *SQL implementation*, while keeping everything transparent and easy to inspect.

Metrics and dimensions are described in YAML. A Python compiler generates dbt marts from these definitions.

This project is exploratory and focused on correctness and simplicity rather than completeness.

---

## Stack

- Flask (event ingestion)
- S3 (raw landing)
- Snowflake (storage + compute)
- dbt (transformations)
- Python (YAML → SQL compiler)
- Airflow (planned orchestration)
- Terraform (planned infrastructure)

---

## High-level flow

```

events → Flask → S3 → Snowflake raw
|
v
YAML events schemas
|
v
generate_staging.py
|
v
dbt staging
|
v
YAML event groups
|
v
generate_intermediate.py
|
v
dbt intermediate (event spine)
|
v
YAML pipelines
|
v
generate_mart.py
|
v
dbt marts

```

---

## Repository layout

```

configs/
  events/               # Atomic events definitions
  event_groups/         # Grouping events based on actors, objects, sessions
  pipelines/            # Metric definitions

dbt/
  models/
    staging/            # Turns raw JSON Snowflake event log table -> canonical events views
    intermediate/       # Extracts primary keys based on event group YAML and unions into one table
    mart/               # Tranforms, joins, and aggregates the events in a group based on YAML file


compiler/
    generate_staging.py         # YAML → dbt SQL compilers
    generate_intermediate.py
    generate_mart.py

    util.py                     # Formatting helpers

````

---

## YAML pipeline definition

Each pipeline defines:

- source table
- grouping keys (dimensions)
- metrics
- incremental field
- event differentiator

Minimal example:

```yaml
name: product_metrics
source: int_product_impressions

group_keys:
  user_id:
    type: string

  product_id:
    type: string

  date:
    type: date
    field: ts
    transforms:
      - date
      - name: convert_timezone
        args: ["UTC", "America/Toronto"]
        field_pos: 2

metrics:
  impressions:
    event: product_view
    agg: count

  avg_dwell_time:
    event: product_view
    agg: avg
    fields: [dwell_time]

incremental_time_field: _ingested_at
source_row_differentiator: event_type
````

---

## Generating marts

Run:

```bash
python generate_mart.py
```

This will:

* read YAML pipelines
* generate dbt mart models
* generate `schema.yml`

Then run dbt normally:

```bash
dbt run --select mart
```

---

## Design goals

* Declarative metric definitions
* Generated SQL is inspectable
* Minimal abstractions
* Explicit joins and projections
* Easy to modify and extend

Non-goals:

* Full semantic layer
* BI integration
* Automatic lineage UI
* General-purpose query planner

---

## Status

Currently implemented:

* YAML → dbt mart compiler
* group-based incremental marts
* event-driven metric definitions

Planned:

* Airflow orchestration
* Terraform-managed infrastructure
* environment separation
* YAML validation

---

## Notes

This is an exploratory project, there will be some rough edges.
