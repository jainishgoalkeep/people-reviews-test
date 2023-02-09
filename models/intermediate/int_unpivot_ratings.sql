{{ config(materialized="view") }}

with
    source as (select * from {{ ref("stg_review_for_individual") }}),

    unpivot_ratings as (
        select 
            review_id,
            question,
            rating
        from source
            UNPIVOT(rating FOR question IN(q9_pm_skills, q1_data_driven))
        Where rating is not null
    )

select *
from unpivot_ratings



