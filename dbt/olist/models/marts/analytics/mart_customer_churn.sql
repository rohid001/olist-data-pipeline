-- Port Q6b project DA (sql/05_churn_indicator.sql): status churn per pelanggan.
-- 'At Risk' = tidak order lagi lebih dari churn_threshold_days (180 hari, di dbt_project.yml)
-- sejak order terakhir di dataset.
with customer_orders as (

    select * from {{ ref('int_customer_orders') }}

),

reference_date as (

    select max(order_purchase_timestamp) as max_date from {{ ref('fct_orders') }}

),

customer_last_order as (

    select
        customer_unique_id,
        max(order_purchase_timestamp) as last_order_date,
        count(distinct order_id) as total_orders
    from customer_orders
    group by customer_unique_id

)

select
    customer_last_order.customer_unique_id,
    customer_last_order.total_orders,
    customer_last_order.last_order_date,
    reference_date.max_date::date
    - customer_last_order.last_order_date::date as days_since_last_order,
    case
        when
            reference_date.max_date::date - customer_last_order.last_order_date::date
            > {{ var('churn_threshold_days') }}
            then 'At Risk'
        else 'Active'
    end as churn_status
from customer_last_order
cross join reference_date
