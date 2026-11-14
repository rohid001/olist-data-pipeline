-- Satu baris per event order dari Kafa (raw.order_events, diisi streaming/consumer.py).
-- Duplikat sudah dibuang di consumer (primary key_event_id), jadi di sini hanya ubah tipe
-- data dan tambahkan urutan tahap.
with source as (

    select * from {{ source('raw', 'order_events') }}

),

typed as (

    select
        event_id,
        event_type,
        order_id,
        customer_id,
        event_ts::timestamp as event_ts,
        estimated_delivery_ts::timestamp as estimated_delivery_ts,
        case event_type
            when 'order_created' then 1
            when 'order_approved' then 2
            when 'order_shipped' then 3
            when 'order_delivered' then 4
        end as stage_number,
        produced_at::timestamptz as produced_at,
        _partition as kafka_partition,
        _offset as kafka_offset,
        _ingested_at as ingested_at
    from source

)

select
    *,
    -- Semua event satu order ada di partisi yang sama, jadi urutan offset = urutan tiba.
    -- tiba tidak berurutan = sebelumnya sudah tiba event dari tahap yang lebih lanjut.
    coalesce(
        stage_number < max(stage_number) over (
            partition by order_id
            order by kafka_offset
            rows between unbounded preceding and 1 preceding
        ),
        false
    ) as arrived_out_of_order
from typed
