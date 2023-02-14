{{ config(materialized="view") }}

with
    source as (select * from {{ ref("int_average_ratings") }}),

    pivot_by_review_type as (
        select
            filled_for,
            year_and_quarter,
            cast(regexp_extract(question, r'q(\d+)_.*') as int64) as question_number,
            ifnull(self_rating, 0) as self_rating,
            ifnull(peer_rating, 0) as peer_rating,
            ifnull(managee_rating, 0) as managee_rating,
            ifnull(manager_rating, 0) as manager_rating
        from
            source pivot (
                max(avg_rating) for review_type in (
                    '1 - For Self' as self_rating,
                    '2 - For Peer' as peer_rating,
                    '3 - For Managee' as managee_rating,
                    '4 - For Manager' as manager_rating
                )
            )
        order by 1, 2, 3
    )

select *
from pivot_by_review_type
