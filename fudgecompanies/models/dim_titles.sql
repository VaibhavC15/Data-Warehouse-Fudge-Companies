with stg_titles as 
(
    select
        title_id,  
        title_name,
        title_type,
        title_synopsis,
        title_release_year,
        title_runtime,
        title_rating,
        replace(to_date(title_date_modified)::varchar,'-','')::int as date_modified_key
    from {{source('fudgeflix','ff_titles')}}
)
select {{ dbt_utils.generate_surrogate_key(['stg_titles.title_id']) }} as title_key,
    stg_titles.* 
from stg_titles 