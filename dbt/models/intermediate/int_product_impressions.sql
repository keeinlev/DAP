WITH unioned AS (
  SELECT
    'product_view' AS event_type,
    event_id,
    user_id,
    request_id,
    product_id,
    ts,
    _ingested_at
  FROM {{ ref('stg_product_view') }}

  UNION ALL

  SELECT
    'product_click' AS event_type,
    event_id,
    user_id,
    request_id,
    product_id,
    ts,
    _ingested_at
  FROM {{ ref('stg_product_click') }}

  UNION ALL

  SELECT
    'product_add_to_cart' AS event_type,
    event_id,
    user_id,
    request_id,
    product_id,
    ts,
    _ingested_at
  FROM {{ ref('stg_product_add_to_cart') }}

  UNION ALL

  SELECT
    'product_order' AS event_type,
    event_id,
    user_id,
    request_id,
    product_id,
    ts,
    _ingested_at
  FROM {{ ref('stg_product_order') }}
),

ordered AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY request_id
      ORDER BY ts
    ) AS event_seq
  FROM unioned
)

SELECT *
FROM ordered
