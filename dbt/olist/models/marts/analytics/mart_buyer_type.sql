-- Port Q2 project DA (sql/03_rfm_analysis.sql): kontribusi revenue one-time vs repeat buyer.
with customer_orders as (

    select * from {{ ref('int_customer_orders') }}
    where payment_total is not null

),

customer_summary as (

    select
        customer_unique_id,
        count(distinct order_id) as frequency,
        sum(payment_total) as monetary
    from customer_orders
    group by customer_unique_id

),

buyer_type as (

    select
        monetary,
        case when frequency = 1 then 'One-time buyer' else 'Repeat buyer' end as customer_type
    from customer_summary

),

aggregated as (

    select
        customer_type,
        count(*) as customer_count,
        sum(monetary) as total_revenue
    from buyer_type
    group by customer_type

)

select
    customer_type,
    customer_count,
    round(total_revenue, 2) as total_revenue,
    round(100.0 * total_revenue / sum(total_revenue) over (), 2) as revenue_pct,
    round(100.0 * customer_count / sum(customer_count) over (), 2) as customer_pct
from aggregated