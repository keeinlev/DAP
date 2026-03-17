import json
import uuid
import time
import boto3
from validator import validate_event

kinesis = boto3.client("kinesis")
STREAM_NAME = "dap-kinesis-stream-dev" # IMPORTANT: CHANGE THIS TO BE INJECTED BY WORKFLOW


def handler(event, context):
    body = event["body"]
    events = body["events"]

    records = []

    for e in events:
        validate_event(e)

        envelope = {
            "event_id": str(uuid.uuid4()),
            "event_type": e["event_type"],
            "event_version": e["event_version"],
            "ingested_at": int(time.time() * 1000),
            "payload": e["payload"],
        }

        records.append({
            "Data": json.dumps(envelope),
            "PartitionKey": envelope["event_type"]
        })

    kinesis.put_records(
        StreamName=STREAM_NAME,
        Records=records
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "ok"})
    }
