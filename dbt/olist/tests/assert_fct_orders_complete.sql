-- Fct_orders adalah model incremental, jadi bisa saja ada batch yang terlewat.
-- Test ini gagal bila ada order di staging yang tidak ada di fct_orders, atau sebaliknya.
-- Hasil yang benar: 0 baris.
select
    coalesce(staging.order_id, facts.order_id) as order_id,
    case when facts.order_id is null then 'missing in fct_orders' else 'extra in fct_orders' end
        as problem
from {{ ref('stg_orders') }} as staging
full outer join {{ ref('fct_orders') }} as facts on staging.order_id = facts.order_id
where staging.order_id is null or facts.order_id is null
