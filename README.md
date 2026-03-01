# Declarative Analytics Platform (DAP)
## February 23, 2026 - Present

![dap](https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExODhoNjVkOG05aXZ3anBnbXlneTg0cTJvdHd2Y29wczZib2g5c2tkYiZlcD12MV9naWZzX3NlYXJjaCZjdD1n/zCHnEV6OQXQoZKEabT/giphy.gif)

DAP is a small declarative analytics system for defining metrics in YAML and compiling them into dbt models on Snowflake.

The goal is to abstract away both the SQL implementation and the infrastructure setup, which democratizes both metrics generation and ETL pipelining, while keeping everything transparent and easy to inspect.

Metrics and dimensions are described in YAML. A Python compiler generates dbt marts from these definitions.

This project is exploratory and focused on correctness and simplicity rather than completeness.

---

## System Requirements

- Python 3.12+
- AWS CLI
- Terraform
- Git Bash (if on Windows)
- AWS Console Account with Admin Permissions
- Snowflake Account with Admin Permissions

---

## Cloud Setup
### This is required to get your Terraform cloud access up and running for both AWS and Snowflake.

## 1. AWS Setup

(You can follow [this tutorial](https://www.youtube.com/watch?v=Qfg6hRY4Tq0), starting at timestamp 16:37 until 25:43, or follow the steps below)

The following is a guide designed for everyone and anyone to follow, If you know what you are doing please go ahead and skip this. I am writing this guide on a Saturday night at 9pm for goodness' sake I will not take any crap for long documentation.

1. Log into your AWS account on the Console.

2. Go to the IAM service.

3. In "Roles", click "Create role"

4. Under "Trusted entity type", select "AWS account", then "This account" in the below selection, then "Next"
    ![role-trusted-entity](./docs/screenshots/Screen%20Shot%202026-02-28%20at%206.48.31%20PM.png)

5. In "Permission policies", select "Administrator Access"
    ![role-admin-access](./docs/screenshots/Screen%20Shot%202026-02-28%20at%206.52.08%20PM.png), then "Next".

6. For "Role Name", set to "TerraformExecutionRole", or something similar to your liking, then "Create role". Make note of the Role ARN on the new role's detail page.
    ![role-arn](./docs/screenshots/Screen%20Shot%202026-02-28%20at%208.32.02%20PM.png)

7. In "Policies", click "Create policy"

8. Under "Select a service", search for and select "STS".
    ![policy-sts](./docs/screenshots/Screen%20Shot%202026-02-28%20at%208.40.01%20PM.png)

9. Under "Actions allowed", search for and select "AssumeRole"
    ![policy-assume-role](./docs/screenshots/Screen%20Shot%202026-02-28%20at%208.41.52%20PM.png)

10. Under "Resources", select "Specific", then "Add ARNs" below.
    ![policy-resources](./docs/screenshots/Screen%20Shot%202026-02-28%20at%208.43.36%20PM.png)

11. In the "Specify ARNs" pop-up, select "This account" under "Resource in", then enter "TerraformExecutionRole" or whatever name you gave the previously created role under "Resource role name with path", then click "Add ARNs".
    ![policy-arn](./docs/screenshots/Screen%20Shot%202026-02-28%20at%208.44.54%20PM.png)

12. Click "Next" at the bottom of the page.

13. For "Policy name", enter "AssumeTerraformRole", then select "Create Policy".

14. In "Users", click "Create user".

15. Set "User name" to "terraform-admin", click "Next".

16. Under "Permissions options", select "Attach policies directly", then search for and select "AssumeTerraformRole". Click "Next".
    ![user-permissions](./docs/screenshots/Screen%20Shot%202026-02-28%20at%208.55.23%20PM.png)

17. Click "Create User".

18. Click into the new "terraform-admin" user from the "Users" tab, and then "Security Credentials".
    ![user-credentials](./docs/screenshots/Screen%20Shot%202026-02-28%20at%208.58.04%20PM.png)

19. Under "Access keys", click "Create access key".

20. Select "Other", then click "Next". Then, click "Create access key"

21. Under "Retrieve access keys", copy and record the Access key ID and Secret access key somewhere secure. Do not store these keys in any code or repository. You will need these later in the [Local Machine Setup](#local-machine-setup)
    ![user-access-key](./docs/screenshots/Screen%20Shot%202026-02-28%20at%209.00.53%20PM.png)

## 2. Snowflake Setup

1. Log into your Snowflake account.

2. In your Homepage URL, you should see `https://app.snowflake.com/<XXXXXXX>/<YYYYYYY>/#/homepage`. `<XXXXXXX>` is your Organization Name, and `<YYYYYYY>` is your Account Name. Together, `<organization_name>-<account_name>` is your Account ID. Make note of these, as well as the username/password you used to sign in.

## Local Machine Setup

### Let me be clear, uhh, you must do both Cloud and Local Machine Setups.
![obamna](https://media.tenor.com/NoMf8P40Kt4AAAAe/obama-let-me-be-clear.png)

1. Run `pip install -r requirements.txt` in the project root directory.

2. Run `aws configure --profile terraform-admin-user`.

3. Paste the `terraform-admin` user's Access key and Secret access key when prompted, then your preferred AWS region (the default for the repo is `us-east-2`, you can change this in `infra/core/variables.tf`), you can leave "Default output format" blank.

4. Open `~/.aws/config` (`\Users\<your-pc-user>\.aws\config` in Windows) in any text editor. Paste at the bottom on a new line:
```
[profile terraform-admin]
source_profile = terraform-admin-user
region = <preferred-aws-region>
role_arn = arn:aws:iam::xxxxxxxxxxxx:role/<terraform-admin-role-name>
```
Where `role_arn` is taken from Step 6 in [AWS Setup](#1-aws-setup), and `region` is whatever AWS region you choose to use.

5. In a `.env` file, paste the following:
```
SNOWFLAKE_ORGANIZATION_NAME=<your-snowflake-org-name>
SNOWFLAKE_ACCOUNT_NAME=<your-snowflake-account-name>
SNOWFLAKE_USER=<your-snowflake-username>
SNOWFLAKE_PASSWORD=<your-snowflake-password>
```
Each variable can be retrieved by following the [Snowflake Setup](#2-snowflake-setup)

6. In `infra/`, run `terraform init`.

7. In `infra/`, run `terraform workspace new <env-name>`. \<env-name\> is something like `dev`, `prod`, `staging`, etc.

8. In `infra/`, run `. ./bootstrap.sh` in Bash or `.\bootstrap.sh` in CMD. (This bootstrap performs a double Terraform apply and is necessary for initializing the Snowflake storage integration with AWS for copying raw S3 files into Snowflake).

9. For any future Terraform updates, you can simply run `dotenv run -- terraform plan` and `dotenv run -- terraform apply`.

10. All set!

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
