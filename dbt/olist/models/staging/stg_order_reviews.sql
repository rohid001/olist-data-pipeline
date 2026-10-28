-- Satu baris per review_id, Pengganti VIEW order_reviews_dedup di project DA
-- 789 review_id muncul di lebih dari satu order (814 baris kelebihan); semua barisnya
-- punya skor dan waktu jawab yang sama, jadi order_id dipakai sabagai pemacah seri
-- supa baris yang dipertahankan selalu sama setiap kali model dijalankan.
with source as (

    select * from {{ source('raw', 'order_reviews') }}

),

typed as (

    select
        review_id,
        order_id,
        review_score::int as review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date::timestamp as review_creation_date,
        review_answer_timestamp::timestamp as review_answer_timestamp,
        _batch_id,
        _loaded_at
    from source

),

ranked as (

    select
        *,
        row_number() over (
            partition by review_id
            order by review_answer_timestamp desc nulls last, order_id asc
        ) as row_num
    from typed

)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    _batch_id,
    _loaded_at
from ranked
where row_num = 1
