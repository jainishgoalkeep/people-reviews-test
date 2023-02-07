select 
    question_number,
    column_name,
    short_form,
    long_form
from {{ source('gk_work_management', 'questions') }}