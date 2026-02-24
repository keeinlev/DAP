SELECT
    ipi.user_id AS user_id
    , ipi.product_id AS product_id
    , DATE(CONVERT_TIMEZONE('UTC', 'America/Toronto', ipi.ts)) AS date
    , COUNT(CASE WHEN ipi.event_type='product_view' THEN 1 END) AS impressions
    , AVG(CASE WHEN ipi.event_type='product_view' THEN pv.dwell_time END) AS avg_dwell_time
    , COUNT(CASE WHEN ipi.event_type='product_click' THEN 1 END) AS clicks
    , COUNT(CASE WHEN ipi.event_type='product_add_to_cart' THEN 1 END) AS add_to_carts
    , COUNT(CASE WHEN ipi.event_type='product_order' THEN 1 END) AS orders
FROM {{ ref('int_product_impressions') }} ipi
LEFT JOIN {{ ref('stg_product_view') }} pv
    ON ipi.event_id = pv.event_id
    AND ipi.event_type='product_view'
GROUP BY
    ipi.user_id
    , ipi.product_id
    , date