-- Port Q7 project DA (sql/05_churn_indicator.sql): rata-rata review score per status churn.
-- stg_order_reviews sudah bebas duplikat review_id (pengganti VIEW order_reviews_dedup).
with customer_orders as (

    select * from {{ ref('int_customer_orders') }}

),

reviews as (

    select * from {{ ref('stg_order_reviews') }}

),

reference_date as (

    select max(order_purchase_timestamp) as max_date from {{ ref('fct_orders') }}

),

customer_status as (

    select
        customer_orders.customer_unique_id,
        case
            when
                reference_date.max_date::date - max(customer_orders.order_purchase_timestamp)::date
                > {{ var('churn_threshold_days') }}
                then 'At Risk'
            else 'Active'
        end as churn_status
    from customer_orders
    cross join reference_date
    group by customer_orders.customer_unique_id, reference_date.max_date

),

customer_avg_review as (

    select
        customer_orders.customer_unique_id,
        avg(reviews.review_score) as avg_review_score
    from customer_orders
    inner join reviews on customer_orders.order_id = reviews.order_id
    group by customer_orders.customer_unique_id

)

select
    customer_status.churn_status,
    round(avg(customer_avg_review.avg_review_score), 2) as avg_review_score,
    count(*) as customer_count
from customer_status
inner join
    customer_avg_review
    on customer_status.customer_unique_id = customer_avg_review.customer_unique_id
group by customer_status.churn_status
