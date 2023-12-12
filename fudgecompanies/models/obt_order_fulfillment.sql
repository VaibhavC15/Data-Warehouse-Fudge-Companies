with f_order_fulfillment as (
    select * from {{ ref('fact_order_fulfillment') }}
),
d_customers as (
    select * from {{ ref('dim_customers') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
)

select 
    c.*,
    f.order_date_key,
    dt.*,
    f.shipped_date_key,
    f.ship_via,
    f.products_on_order,
    f.quantity,
    f.order_to_shipped_days,
    f.fulfillment_status
    from f_order_fulfillment f
    left join d_customers c on f.customer_key = c.customer_key
    left join d_date dt on f.order_date_key = dt.datekey
