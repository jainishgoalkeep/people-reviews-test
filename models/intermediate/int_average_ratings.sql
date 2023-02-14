{{ config(materialized="view") }}

with
    source as (select * from {{ ref("int_unpivot_ratings") }}),

    average_rating as (
        select 
            filled_for,
            year_and_quarter,
            review_type,
            question,
            CONCAT(filled_for, '--', CAST(year_and_quarter AS STRING), '--', question) as concat_id,
            AVG(rating) AS avg_rating,
            COUNT(DISTINCT review_id) AS count_of_responses
        from source
           
        Where rating is not null
        
        GROUP BY 
            1, 2, 3, 4
        ORDER BY 
            1, 2, 3, 4 
    )

select *
from average_rating