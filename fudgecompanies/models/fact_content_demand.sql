with stg_title_metrics as (
    select 
        {{ dbt_utils.generate_surrogate_key(['at_title_id']) }} as title_key,
        at_title_id,
        count(*) as total_views,
        sum(case when at_rating is null then 0 else 1 end) as total_ratings,
        avg(at_rating) as avg_rating
    from {{source('fudgeflix','ff_account_titles')}}
    group by at_title_id
)

select * from stg_title_metrics