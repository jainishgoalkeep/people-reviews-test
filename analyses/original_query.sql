WITH unpivot_ratings AS (
         SELECT review_for_individual.id AS review_id,
            review_for_individual.filled_for,
            review_for_individual.year_and_quarter,
            review_for_individual.review_type,
            unpivoted.rating,
            unpivoted.question
           FROM {{ ref('stg_review_for_individual') }}
             CROSS JOIN LATERAL ( VALUES (review_for_individual.q1_data_driven,'q1_data_driven'::text), (review_for_individual.q2_people_focused,'q2_people_focused'::text), (review_for_individual.q3_achieve_mission,'q3_achieve_mission'::text), (review_for_individual.q4_given_headsup,'q4_given_headsup'::text), (review_for_individual.q5_open_to_feedback,'q5_open_to_feedback'::text), (review_for_individual.q6_willingness_to_learn,'q6_willingness_to_learn'::text), (review_for_individual.q7_delivered_high_quality,'q7_delivered_high_quality'::text), (review_for_individual.q8_relationships,'q8_relationships'::text), (review_for_individual.q9_pm_skills,'q9_pm_skills'::text), (review_for_individual.q10_problem_solution,'q10_problem_solution'::text), (review_for_individual.q11_delivering_output,'q11_delivering_output'::text), (review_for_individual.q20_there_when_needed,'q20_there_when_needed'::text), (review_for_individual.q21_solutions_provided,'q21_solutions_provided'::text), (review_for_individual.q22_tech_knowledge_support,'q22_tech_knowledge_support'::text), (review_for_individual.q23_timely_responsible_feedback,'q23_timely_responsible_feedback'::text), (review_for_individual.q24_feel_respected,'q24_feel_respected'::text), (review_for_individual.q25_values_input,'q25_values_input'::text), (review_for_individual.q26_give_motivation,'q26_give_motivation'::text), (review_for_individual.q27_give_freedom,'q27_give_freedom'::text), (review_for_individual.q28_facilitated_growth,'q28_facilitated_growth'::text)) unpivoted(rating, question)
          WHERE unpivoted.rating IS NOT NULL AND review_for_individual.status::text = 'Complete'::text
        ), average_ratings AS (
         SELECT unpivot_ratings.filled_for,
            unpivot_ratings.year_and_quarter,
            unpivot_ratings.review_type,
            unpivot_ratings.question,
            (((unpivot_ratings.filled_for || '--'::text) || unpivot_ratings.year_and_quarter::text) || '--'::text) || unpivot_ratings.question AS concat_id,
            avg(unpivot_ratings.rating) AS avg_rating,
            count(DISTINCT unpivot_ratings.review_id) AS count_of_responses
           FROM unpivot_ratings
          GROUP BY unpivot_ratings.filled_for, unpivot_ratings.year_and_quarter, unpivot_ratings.review_type, unpivot_ratings.question
          ORDER BY unpivot_ratings.filled_for, unpivot_ratings.year_and_quarter, unpivot_ratings.review_type, unpivot_ratings.question
        ), aggregate_by_question AS (
         SELECT unpivot_ratings.filled_for,
            unpivot_ratings.year_and_quarter,
            ltrim(split_part(unpivot_ratings.question, '_'::text, 1), 'q'::text)::integer AS question_number,
            avg(unpivot_ratings.rating) AS overall_avg_rating,
            count(DISTINCT unpivot_ratings.review_id) AS count_of_responses
           FROM unpivot_ratings
          GROUP BY unpivot_ratings.filled_for, unpivot_ratings.year_and_quarter, (ltrim(split_part(unpivot_ratings.question, '_'::text, 1), 'q'::text)::integer)
          ORDER BY unpivot_ratings.filled_for, unpivot_ratings.year_and_quarter, (ltrim(split_part(unpivot_ratings.question, '_'::text, 1), 'q'::text)::integer)
        ), pivot_by_review_type AS (
         SELECT average_ratings.filled_for,
            average_ratings.year_and_quarter,
            ltrim(split_part(average_ratings.question, '_'::text, 1), 'q'::text)::integer AS question_number,
            max(
                CASE
                    WHEN average_ratings.review_type::text = '1 - For Self'::text THEN average_ratings.avg_rating
                    ELSE NULL::numeric
                END) AS self_rating,
            max(
                CASE
                    WHEN average_ratings.review_type::text = '2 - For Peer'::text THEN average_ratings.avg_rating
                    ELSE NULL::numeric
                END) AS peer_rating,
            max(
                CASE
                    WHEN average_ratings.review_type::text = '3 - For Managee'::text THEN average_ratings.avg_rating
                    ELSE NULL::numeric
                END) AS managee_rating,
            max(
                CASE
                    WHEN average_ratings.review_type::text = '4 - For Manager'::text THEN average_ratings.avg_rating
                    ELSE NULL::numeric
                END) AS manager_rating
           FROM average_ratings
          GROUP BY average_ratings.filled_for, average_ratings.year_and_quarter, (ltrim(split_part(average_ratings.question, '_'::text, 1), 'q'::text)::integer)
          ORDER BY average_ratings.filled_for, average_ratings.year_and_quarter, (ltrim(split_part(average_ratings.question, '_'::text, 1), 'q'::text)::integer)
        ), aggregate_by_review_type AS (
         SELECT unpivot_ratings.filled_for,
            unpivot_ratings.year_and_quarter,
            NULL::integer AS question_number,
            NULL::numeric AS self_rating,
            NULL::numeric AS peer_rating,
            NULL::numeric AS managee_rating,
            NULL::numeric AS manager_rating,
            'Review Type = '::text || unpivot_ratings.review_type::text AS short_form,
                CASE
                    WHEN unpivot_ratings.review_type::text = '1 - For Self'::text THEN '1 - Overall Rating From Self'::text
                    WHEN unpivot_ratings.review_type::text = '2 - For Peer'::text THEN '2 - Overall Rating From Peer'::text
                    WHEN unpivot_ratings.review_type::text = '4 - For Manager'::text THEN '3 - Overall Rating From Managee'::text
                    WHEN unpivot_ratings.review_type::text = '3 - For Managee'::text THEN '4 - Overall Rating From Manager'::text
                    ELSE NULL::text
                END AS long_form,
            avg(unpivot_ratings.rating) AS overall_rating_by_review_type
           FROM unpivot_ratings
          GROUP BY unpivot_ratings.filled_for, unpivot_ratings.year_and_quarter, NULL::integer, NULL::numeric, ('Review Type = '::text || unpivot_ratings.review_type::text), (
                CASE
                    WHEN unpivot_ratings.review_type::text = '1 - For Self'::text THEN '1 - Overall Rating From Self'::text
                    WHEN unpivot_ratings.review_type::text = '2 - For Peer'::text THEN '2 - Overall Rating From Peer'::text
                    WHEN unpivot_ratings.review_type::text = '4 - For Manager'::text THEN '3 - Overall Rating From Managee'::text
                    WHEN unpivot_ratings.review_type::text = '3 - For Managee'::text THEN '4 - Overall Rating From Manager'::text
                    ELSE NULL::text
                END)
        ), joined_tables AS (
         SELECT pivot_by_review_type.filled_for,
            pivot_by_review_type.year_and_quarter,
            pivot_by_review_type.question_number,
            pivot_by_review_type.self_rating,
            pivot_by_review_type.peer_rating,
            pivot_by_review_type.managee_rating,
            pivot_by_review_type.manager_rating,
            questions.short_form,
            questions.long_form,
            aggregate_by_question.overall_avg_rating
           FROM pivot_by_review_type
             LEFT JOIN "people-management".questions USING (question_number)
             LEFT JOIN aggregate_by_question USING (filled_for, year_and_quarter, question_number)
        )
 SELECT joined_tables.filled_for,
    joined_tables.year_and_quarter,
    joined_tables.question_number,
    joined_tables.self_rating,
    joined_tables.peer_rating,
    joined_tables.managee_rating,
    joined_tables.manager_rating,
    joined_tables.short_form,
    joined_tables.long_form,
    joined_tables.overall_avg_rating
   FROM joined_tables
UNION ALL
 SELECT aggregate_by_review_type.filled_for,
    aggregate_by_review_type.year_and_quarter,
    aggregate_by_review_type.question_number,
    aggregate_by_review_type.self_rating,
    aggregate_by_review_type.peer_rating,
    aggregate_by_review_type.managee_rating,
    aggregate_by_review_type.manager_rating,
    aggregate_by_review_type.short_form,
    aggregate_by_review_type.long_form,
    aggregate_by_review_type.overall_rating_by_review_type AS overall_avg_rating
   FROM aggregate_by_review_type;