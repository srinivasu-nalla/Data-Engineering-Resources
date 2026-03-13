{{
  config(
    materialized = 'incremental',
    unique_key = 'host_id'
    )
}}

select
    host_id,
    REPLACE(host_name, ' ', '_') AS host_name,
    HOST_SINCE,
    IS_SUPERHOST,
    RESPONSE_RATE,
    CASE
        WHEN response_rate >= 95 THEN 'VERY GOOD'
        WHEN response_rate >= 80 THEN 'GOOD'
        WHEN response_rate >= 60 THEN 'FAIR'
        ELSE 'POOR'
    END AS response_rate_quality,
    created_at
from {{ ref('bronze_hosts') }}
    