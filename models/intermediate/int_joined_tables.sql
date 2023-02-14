{{ config(materialized="view") }}

with
    source1 as (select * from {{ ref("int_pivot_by_review_type") }}),
    source2 as (select * from {{ source("gk_work_management", "questions") }}),
    source3 as (select * from {{ ref("int_aggregate_by_question") }}),

    joined_tables as (
        select
            filled_for,
            year_and_quarter,
            question_number,
            self_rating,
            peer_rating,
            managee_rating,
            manager_rating,
            short_form,
            long_form,
            overall_avg_rating
        from source1
        left join source2 using (question_number)
        left join source3 using (filled_for, year_and_quarter, question_number)
    )

select *
from joined_tables
