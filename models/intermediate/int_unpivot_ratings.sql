{{ config(materialized="view") }}

with
    source as (select * from {{ ref("stg_review_for_individual") }}),

    unpivot_ratings as (
        select 
            review_id,
            filled_for,
            year_and_quarter,
            review_type,
            question,
            rating
        from source
            UNPIVOT(rating FOR question IN(q1_data_driven,q2_people_focused,q3_achieve_mission,q4_given_headsup,q5_open_to_feedback,q6_willingness_to_learn,q7_delivered_high_quality,q8_relationships,q9_pm_skills,q10_problem_solution,q11_delivering_output,q20_there_when_needed,q21_solutions_provided,q22_tech_knowledge_support,q23_timely_responsible_feedback,q24_feel_respected,q25_values_input,q26_give_motivation,q27_give_freedom,q28_facilitated_growth))
        Where rating is not null 
    )

select *
from unpivot_ratings



