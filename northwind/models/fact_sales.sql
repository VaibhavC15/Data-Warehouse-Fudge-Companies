with stg_orders as 
(
    select
        Orderid,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey, 
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey
    from {{source('northwind','Orders')}}
),

stg_order_details as
( 
    select  
        Orderid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        Quantity,
        Quantity*UnitPrice as extendedpriceamount,
        Quantity*UnitPrice*Discount as discountamount,
        Quantity*UnitPrice*(1-Discount) as soldamount
    from 
        {{source('northwind','Order_Details')}}
)

select
    o.*,
    od.productkey,
    od.Quantity,
    od.extendedpriceamount,
    od.discountamount,
    od.soldamount
from stg_order_details od
    join stg_orders o on od.Orderid = o.Orderid