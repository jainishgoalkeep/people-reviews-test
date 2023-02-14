{{ config(materialized="table") }}

with
    source1 as (select * from {{ ref("int_joined_tables") }}),
    source2 as (select * from {{ ref("int_aggregate_by_review_type") }})

    SELECT 
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
   FROM source1

UNION ALL
 SELECT 
    filled_for,
    year_and_quarter,
    question_number,
    self_rating,
    peer_rating,
    managee_rating,
    manager_rating,
    short_form,
    long_form,
    overall_rating_by_review_type AS overall_avg_rating
   from source2
