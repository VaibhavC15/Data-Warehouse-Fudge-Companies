
---{{ config(materialized='table') }}

with stg_orders as 
(
    select
        order_id,  
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key, 
        replace(to_date(order_date)::varchar,'-','')::int as order_date_key,
        replace(to_date(shipped_date)::varchar,'-','')::int as shipped_date_key,
        ship_via
    from {{source('fudgemart','fm_orders')}}
),
stg_order_details as
(
    select 
        order_id,
        count(order_qty) as products_on_order, 
        sum(order_qty) as quantity
    from {{source('fudgemart','fm_order_details')}}
    group by order_id
),
stg_ship_via as (
    select * from {{source('fudgemart','fm_shipvia_lookup')}}
)
select  
    o.*,
    od.products_on_order,
    od.quantity,
    o.shipped_date_key - o.order_date_key as order_to_shipped_days,
    case 
        when (o.shipped_date_key - o.order_date_key)<= 14 
            then 'On-time Delivery'
        else 'Late Delivery' 
    end as fulfillment_status
from stg_orders o
    join stg_order_details od on o.order_id = od.order_id
    join stg_ship_via s on s.ship_via = o.ship_via
