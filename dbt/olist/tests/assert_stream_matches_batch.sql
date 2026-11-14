-- Event yang sudah tiba lewat Kafka harus sama dengan timestamp di data batch (stg_orders).
-- Tahap yang belum tiba (masih null di stream) tidak dibandingkan, karena stream bisa
-- tertinggal dari batch. Hasil yang benar: 0 baris.

select
    lifecycle.order_id,
    lifecycle.created_at,
    orders.order_purchase_timestamp
from {{ ref('fct_order_lifecycle') }} as lifecycle
inner join {{ ref('stg_orders') }} as orders on lifecycle.order_id = orders.order_id
where
    (
        lifecycle.created_at is not null
        and lifecycle.created_at is distinct from orders.order_purchase_timestamp
    )
    or (
        lifecycle.approved_at is not null
        and lifecycle.approved_at is distinct from orders.order_approved_at
    )
    or (
        lifecycle.shipped_at is not null
        and lifecycle.shipped_at is distinct from orders.order_delivered_carrier_date
    )
    or (
        lifecycle.delivered_at is not null
        and lifecycle.delivered_at is distinct from orders.order_delivered_customer_date
    )
