{{ config(materialized="view") }}

with
    source as (select * from {{ ref("int_unpivot_ratings") }}),

    aggregate_by_review_type as (
        select 
            filled_for,
            year_and_quarter,
            cast(null as int64) AS question_number,
            cast(null as float64) as self_rating, 
            cast(null as float64) as peer_rating, 
            cast(null as float64) as managee_rating, 
            cast(null as float64) as manager_rating,
            concat('Review Type = ', review_type) as short_form, 
            case 
                when review_type = '1 - For Self' then '1 - Overall Rating From Self'
                when review_type = '2 - For Peer' then '2 - Overall Rating From Peer'
                when review_type = '4 - For Manager' then '3 - Overall Rating From Managee'
                when review_type = '3 - For Managee' then '4 - Overall Rating From Manager'
                else null 
            end as long_form, 
            avg(rating) as overall_rating_by_review_type
        from source

        GROUP BY 
            filled_for, 
            year_and_quarter, 
            cast(null as int64), 
            cast(null as float64), 
            short_form, 
            long_form
     
    )

select *
from aggregate_by_review_type
