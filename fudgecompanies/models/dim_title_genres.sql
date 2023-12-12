with stg_title_genres as (
    select 
        {{ dbt_utils.generate_surrogate_key(['tg_title_id']) }} as title_key,
        tg_title_id,
        tg_genre_name
    from {{ source('fudgeflix','ff_title_genres')}}
)
select * from stg_title_genres order by title_key
