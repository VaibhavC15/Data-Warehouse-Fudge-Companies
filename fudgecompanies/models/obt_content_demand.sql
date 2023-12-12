with f_content_demand as (
    select * from {{ ref('fact_content_demand') }}
),
d_titles as (
    select * from {{ ref('dim_titles') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
)

select 
    t.*,
    dt.*,
    cd.avg_rating,
    cd.total_views,
    cd.total_ratings
    
    from f_content_demand cd
    left join d_titles t on cd.title_key = t.title_key
    left join d_date dt on t.date_modified_key = dt.datekey
