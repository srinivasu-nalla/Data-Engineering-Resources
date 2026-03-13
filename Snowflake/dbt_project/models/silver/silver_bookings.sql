{{
  config(
    materialized = 'incremental',
    unique_key = 'booking_id'
    )
}}

SELECT
    booking_id,
    listing_id,
    booking_date,
    {{ multiply('NIGHTS_BOOKED', 'BOOKING_AMOUNT', 2) }} + CLEANING_FEE + SERVICE_FEE AS TOTAL_AMOUNT,
    BOOKING_STATUS,
    created_at
FROM {{ ref('bronze_bookings') }}