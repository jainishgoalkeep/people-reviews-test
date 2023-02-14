{{ config(materialized="view") }}

with
    source as (select * from {{ ref("int_average_ratings") }}),

    pivot_by_review_type as (
        select 
            filled_for,
            year_and_quarter,
            CAST(REGEXP_EXTRACT(question, r'q(\d+)_.*') AS INT64) AS question_number,
            IFNULL(self_rating, 0) AS self_rating,
            IFNULL(peer_rating, 0) AS peer_rating,
            IFNULL(managee_rating, 0) AS managee_rating,
            IFNULL(manager_rating, 0) AS manager_rating
        from source
           PIVOT (
                 MAX(avg_rating) FOR review_type IN (
                '1 - For Self' AS self_rating,
                '2 - For Peer' AS peer_rating,
                '3 - For Managee' AS managee_rating,
                '4 - For Manager' AS manager_rating
                    )
                )
        ORDER BY 1, 2, 3
    )

select *
from pivot_by_review_type



