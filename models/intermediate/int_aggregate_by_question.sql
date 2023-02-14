{{ config(materialized="view") }}

with
    source as (select * from {{ ref("int_unpivot_ratings") }}),

    aggregate_by_question as (
        select
            filled_for,
            year_and_quarter,
            cast(regexp_extract(question, r'q(\d+)_.*') as int64) as question_number,
            avg(rating) as overall_avg_rating,
            count(distinct review_id) as count_of_responses
        from source

        group by 1, 2, 3
        order by 1, 2, 3

    )

select *
from aggregate_by_question
