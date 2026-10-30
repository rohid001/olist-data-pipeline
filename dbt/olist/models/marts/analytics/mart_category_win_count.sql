-- Port query pendukung project DA (08_category_win_count.sql, dulu hanya ada di Power BI):
-- berapa state yang menjadikan tiap kategori sebagai kategori dengan margin kasar terbesar.
-- Dipakai kartu "kategori unggulan" di halaman Overview dashboard.
with category_per_state as (

    select * from {{ ref('mart_category_per_state') }}

)

select
    category,
    count(*) as state_count
from category_per_state
group by category