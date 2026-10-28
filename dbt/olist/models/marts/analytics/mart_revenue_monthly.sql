-- Port Q3 + Q5 project DA (sql/04_revenue_trend.sql): revenue bulanan dan growth MoM.
-- Revenue = SUM(price) item dari order delivered, tanpa ongkir (freight_value).
with order_items as (

    select * from {{ ref('fct_order_items') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

monthly_revenue as (

    select
        orders.order_month as month,
        sum(order_items.price) as revenue
    from orders
    inner join order_items on orders.order_id = order_items.order_id
    where orders.order_status = 'delivered'
    group by orders.order_month

)

select
    month,
    round(revenue, 2) as revenue,
    round(lag(revenue) over (order by month), 2) as previous_month_revenue,
    round(
        100.0 * (revenue - lag(revenue) over (order by month))
        / nullif(lag(revenue) over (order by month), 0), 2
    ) as growth_mom_pct
from monthly_revenue
