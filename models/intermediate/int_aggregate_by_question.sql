{{ config(materialized="view") }}

with
    source as (select * from {{ ref("int_unpivot_ratings") }}),

    aggregate_by_question as (
        select 
            filled_for,
            year_and_quarter,
            CAST(REGEXP_EXTRACT(question, r'q(\d+)_.*') AS INT64) AS question_number,
            AVG(rating) AS overall_avg_rating,
            COUNT(DISTINCT review_id) AS count_of_responses
        from source

        GROUP BY 
         1, 2, 3
        ORDER BY 
         1, 2, 3   
       
    )

select *
from aggregate_by_question
