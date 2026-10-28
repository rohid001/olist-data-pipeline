-- Total revenue harus sama di tiga mart yang menghitungnya dengan cara berbeda:
-- KPI overview, jumlah revenue bulanan, dan jumlah revenue per state.
-- Hasil yang benar : 0 baris.
with totals as (

    select
        (select total_revenue from {{ ref('mart_overview_kpi') }}) as overview_revenue,
        (select sum(revenue) from {{ ref('mart_revenue_monthly') }}) as monthly_revenue,
        (select sum(total_revenue) from {{ ref('mart_revenue_per_state') }}) as state_revenue

)

select *
from totals
where
    overview_revenue <> monthly_revenue
    or overview_revenue <> state_revenue
