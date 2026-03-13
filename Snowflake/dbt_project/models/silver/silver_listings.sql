{{
  config(
    materialized = 'incremental',
    unique_key = 'listing_id'
    )
}}

SELECT
    listing_id,
    host_id,
    property_type,
    room_type,
    city,
    country,
    accommodates,
    bedrooms,
    bathrooms,
    price_per_night,
    {{ tag('CAST(price_per_night as INT)') }} AS price_per_night_tag,
    created_at
FROM {{ ref('bronze_listings') }}