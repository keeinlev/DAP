from flask import Flask, request, jsonify
import boto3
import json
import os
from datetime import datetime, timezone
from validator import validate_event

app = Flask(__name__)

S3_BUCKET = os.environ["EVENT_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

endpoint = os.environ.get("AWS_ENDPOINT_URL")

if endpoint:
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=endpoint
    )
else:
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION
    )


@app.route("/events", methods=["POST"])
def ingest_event():
    payload = request.get_json()

    if not payload:
        return jsonify({"error": "Invalid JSON"}), 400

    event_name = payload.get("event_name")
    data = payload.get("data")

    if not event_name or not data:
        return jsonify({"error": "event_name and data required"}), 400

    # Schema validation
    try:
        validate_event(event_name, data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    now = datetime.now(timezone.utc)

    record = {
        "event_name": event_name,
        "ingested_at": now.isoformat(),
        "data": data
    }

    # Partitioned S3 path
    date_str = now.strftime("%Y-%m-%d")
    key = f"{event_name}/date={date_str}/{data['user_id']}-{data['request_id']}-{data['product_id']}-{int(now.timestamp() * 10**3)}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json"
    )

    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)