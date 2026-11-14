-- Satu baris per order: waktu setiap tahap menurut event yang sudah tiba dari Kafka.
-- Incremental berdasarkan WAKTU TIBA (ingested_at), bukan waktu kejadian: event yang
-- terlambat tetap terambil di run berikutnya, karena waktu tibanya baru. Untuk setiap
-- order yang menerima event baru, semua event-nya dihitung ulang lalu barisnya diganti.
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

with events as (

    select * from {{ ref('stg_order_events') }}

),

changed_orders as (

    select distinct order_id
    from events
    {% if is_incremental() %}
        -- Mundur beberapa menit dari event terakhir yang sudah diposes: transaksi consumer
        -- yang commit sedikit terlambat tetap terambil. Order yang terambil dua kali hanya
        -- dihitung ulang, hasilnya sama.
        where ingested_at > (
            select coalesce(max(last_ingested_at), '1900-01-01'::timestamptz)
            from {{ this }} -- noqa: RF02
        ) - interval '{{ var("stream_lookback_minutes") }} minutes'
    {% endif %}

),

order_events as (

    select events.*
    from events
    inner join changed_orders on events.order_id = changed_orders.order_id

)

select
    order_id,
    max(customer_id) as customer_id,
    max(event_ts) filter (where event_type = 'order_created') as created_at,
    max(event_ts) filter (where event_type = 'order_approved') as approved_at,
    max(event_ts) filter (where event_type = 'order_shipped') as shipped_at,
    max(event_ts) filter (where event_type = 'order_delivered') as delivered_at,
    max(estimated_delivery_ts) as estimated_delivery_at,
    -- Tahap terkini dari urutan bisnis, bukan dari event yang terakhir tiba
    max(stage_number) as current_stage_number,
    count(*) as event_count,
    bool_or(arrived_out_of_order) as has_out_of_order_event,
    max(ingested_at) as last_ingested_at
from order_events
group by order_id
