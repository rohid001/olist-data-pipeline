-- Kinerja proses order per bulan pembelian, dari data streaming: berapa lama order
-- disetujui, berapa lama sampai diterima, dan berapa persen tiba sebelum perkiraan.
with lifecycle as (

    select * from {{ ref('fct_order_lifecycle') }}
    where created_at is not null

)

select
    date_trunc('month', created_at)::date as purchase_month,
    count(*) as orders,
    count(approved_at) as approved_orders,
    count(delivered_at) as delivered_orders,
    round(
        avg(extract(epoch from approved_at - created_at) / 3600)::numeric, 2
    ) as avg_hours_to_approve,
    round(
        avg(extract(epoch from delivered_at - created_at) / 86400):: numeric, 2
    ) as avg_days_to_deliver,
    round(
        100.0 * count(*) filter (where delivered_at::date <= estimated_delivery_at::date)
        / nullif(count(delivered_at), 0),
        2
    ) as on_time_pct,
    count(*) filter (where has_out_of_order_event) as orders_with_out_of_order_events
from lifecycle
group by 1
