
WITH base AS (
    SELECT
        MD5(TO_VARCHAR(data) || TO_VARCHAR(ingested_at)) AS event_id,
        data:user_id::STRING AS user_id,
        data:visit_id::STRING AS visit_id,
        data:product_id::STRING AS product_id,
        data:request_id::STRING AS request_id,
        data:dwell_time::FLOAT AS dwell_time,
        data:ts::TIMESTAMP AS ts,
        ingested_at AS _ingested_at
    FROM {{ source('raw', 'events') }}
    WHERE event_name = 'product_view'
    {% if is_incremental() %}
    AND ingested_at > (SELECT max(_ingested_at) FROM {{ this }})
    {% endif %}
)
SELECT * FROM base
