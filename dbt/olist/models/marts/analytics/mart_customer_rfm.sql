-- Port Q1 project DA (sql/03_rfm_analysis.sql): skor dan segmen rfm per pelanggan.
-- Aturan yang wajib terbawa:
--  * pelanggan = customer_unique_id, order delivered saja (lewat int_customer_orders)
--  * F-score business rule, bukan NTILE (97% pelanggan hanya order 1 kali)
--  * tiebreakeer customer_unique_id supaya NTILE selalu sama
--  * reference date = order terakhir dari SEMUA status, bukan hanya delivered
with customer_orders as (

    select * from {{ ref('int_customer_orders') }}
    -- Project DA memakai inner join ke total pembayaran, jadi order tanpa pembayaran tidak ikut
    where payment_total is not null

),

reference_date as (

    select max(order_purchase_timestamp) as max_date from {{ ref('fct_orders') }}

),

customer_rfm_raw as (

    select
        customer_unique_id,
        max(order_purchase_timestamp) as last_order_date,
        count(distinct order_id) as frequency,
        sum(payment_total) as monetary
    from customer_orders
    group by customer_unique_id

),

customer_rfm_scored as (

    select
        customer_rfm_raw.*,
        reference_date.max_date::date - customer_rfm_raw.last_order_date::date as recency_days,
        ntile(5) over (
            order by
                reference_date.max_date::date - customer_rfm_raw.last_order_date::date desc,
                customer_rfm_raw.customer_unique_id
        ) as r_score,
        case
            when customer_rfm_raw.frequency = 1 then 1
            when customer_rfm_raw.frequency = 2 then 3
            when customer_rfm_raw.frequency >= 3 then 5
        end as f_score,
        ntile(5) over (
            order by customer_rfm_raw.monetary asc, customer_rfm_raw.customer_unique_id asc
        ) as m_score
    from customer_rfm_raw
    cross join reference_date

)

select
    customer_unique_id,
    recency_days,
    frequency,
    round(monetary, 2) as monetary,
    r_score,
    f_score,
    m_score,
    concat(r_score, f_score, m_score) as rfm_code,
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
        when r_score >= 3 and f_score >= 3 then 'Loyal Customers'
        when r_score >= 4 and f_score <= 2 then 'New Customers'
        when r_score <= 2 and f_score >= 4 then 'At Risk (dulu loyal)'
        when r_score <= 2 and f_score <= 2 then 'Lost / Hibernating'
        else 'Others'
    end as rfm_segment
from customer_rfm_scored
