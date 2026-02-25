import os
import sys
from dotenv import load_dotenv
from snowflake import connector



if __name__ == "__main__":

    load_dotenv()

    if len(sys.argv) == 1:
        print("python get_snowflake_integration_vars.py <deployment-env>     e.g. dev")
        exit(1)
    
    env = sys.argv[1]

    connection = connector.connect(
        account=f"{os.environ['SNOWFLAKE_ORGANIZATION_NAME']}-{os.environ['SNOWFLAKE_ACCOUNT_NAME']}",
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD']
    )

    cursor = connection.cursor()

    res = cursor.execute(f"DESC INTEGRATION s3_{env}_bucket_access;").fetchall()

    desired_properties = (
        'STORAGE_AWS_IAM_USER_ARN',
        'STORAGE_AWS_EXTERNAL_ID',
    )

    res = { x[0] : x[2] for x in res if x[0] in desired_properties }

    cursor.close()
    connection.close()

    for key, value in res.items(): print(key, value)

    exit(0)


# DESC=$(snowflake -o exit_on_error=true -q "DESC INTEGRATION s3_${ENV}_bucket_access;")

# SNOWFLAKE_IAM_USER_ARN=$(echo "$DESC" | grep STORAGE_AWS_IAM_USER_ARN | awk '{print $4}')
# SNOWFLAKE_EXTERNAL_ID=$(echo "$DESC" | grep STORAGE_AWS_EXTERNAL_ID | awk '{print $4}')

# echo "Snowflake IAM user ARN: $SNOWFLAKE_IAM_USER_ARN"
# echo "Snowflake external ID: $SNOWFLAKE_EXTERNAL_ID"

# export TF_VAR_snowflake_aws_iam_user_arn=$SNOWFLAKE_IAM_USER_ARN
# export TF_VAR_snowflake_external_id=$SNOWFLAKE_EXTERNAL_ID