import os
import yaml
from pathlib import Path

snowflake_dev_yaml = {
    "dap_analytics_dev": {
        "target": "dev",
        "outputs": {
            "dev": {
                "type": "snowflake",
                "account": "${SNOWFLAKE_ACCOUNT}",
                "user": "${SNOWFLAKE_USER}",
                "password": "${SNOWFLAKE_PASS}",
                "role": "ACCOUNTADMIN",    # For dev only
                "database": "${DBT_SNOWFLAKE_READ_DB}",
                "warehouse": "${DBT_SNOWFLAKE_WH}",
                "schema": "${DBT_SNOWFLAKE_OUTPUT_SCHEMA}",
                "threads": 4,
                "client_session_keep_alive": False,
            }
        }
    }
}

(Path(os.environ.get('HOMEPATH')) / Path(".dbt/profiles.yml")).write_text(yaml.dump(snowflake_dev_yaml))
