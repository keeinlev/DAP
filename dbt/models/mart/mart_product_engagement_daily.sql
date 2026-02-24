{{ config(
    materialized='incremental',
    unique_key=['user_id', 'product_id', 'date']
) }}

WITH base AS (
    SELECT
        ipi.event_type
        , ipi._ingested_at
        , ipi.user_id AS user_id
        , ipi.product_id AS product_id
        , DATE(CONVERT_TIMEZONE('UTC', 'America/Toronto', ipi.ts)) AS date
        , pv.dwell_time AS dwell_time
    FROM {{ ref('int_product_impressions') }} ipi
    LEFT JOIN {{ ref('stg_product_view') }} pv
    ON ipi.event_id = pv.event_id
    AND ipi.event_type='product_view'
    {% if is_incremental() %}
    WHERE EXISTS (
        SELECT 1
        FROM {{ ref('int_product_impressions') }} new_rows
        WHERE new_rows._ingested_at > (
            SELECT COALESCE(MAX({{ this }}._ingested_at), '1900-01-01')
            FROM {{ this }}
        )
        AND ipi.user_id = new_rows.user_id
        AND ipi.product_id = new_rows.product_id
        AND DATE(CONVERT_TIMEZONE('UTC', 'America/Toronto', ipi.ts)) = DATE(CONVERT_TIMEZONE('UTC', 'America/Toronto', new_rows.ts))
    )
    {% endif %}
)
SELECT
    MAX(_ingested_at) as _ingested_at
    , user_id
    , product_id
    , date
    , COUNT(CASE WHEN event_type='product_view' THEN 1 END) AS impressions
    , AVG(CASE WHEN event_type='product_view' THEN dwell_time END) AS avg_dwell_time
    , COUNT(CASE WHEN event_type='product_click' THEN 1 END) AS clicks
    , COUNT(CASE WHEN event_type='product_add_to_cart' THEN 1 END) AS add_to_carts
    , COUNT(CASE WHEN event_type='product_order' THEN 1 END) AS orders
FROM base
GROUP BY
    user_id
    , product_id
    , date