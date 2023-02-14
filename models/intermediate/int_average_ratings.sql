{{ config(materialized="view") }}

with
    source as (select * from {{ ref("int_unpivot_ratings") }}),

    average_rating as (
        select
            filled_for,
            year_and_quarter,
            review_type,
            question,
            concat(
                filled_for, '--', cast(year_and_quarter as string), '--', question
            ) as concat_id,
            avg(rating) as avg_rating,
            count(distinct review_id) as count_of_responses
        from source

        where rating is not null

        group by 1, 2, 3, 4
        order by 1, 2, 3, 4
    )

select *
from average_rating
