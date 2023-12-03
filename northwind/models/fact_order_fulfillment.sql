with stg_orders as 
(
    select
        OrderID,  
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey, 
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey,
        replace(to_date(shippeddate)::varchar,'-','')::int as shippeddatekey,
        replace(to_date(requireddate)::varchar,'-','')::int as requireddatekey,
        shipname,
        shipaddress,
        shipcity,
        shipregion,
        shippostalcode,
        shipcountry,
        freight,
        shipvia
    from {{source('northwind','Orders')}}
),
stg_order_details as
(
    select 
        orderid,
        sum(Quantity) as quantityonorder, 
        sum(Quantity*UnitPrice*(1-Discount)) as totalorderamount
    from {{source('northwind','Order_Details')}}
    group by orderid
),
stg_shippers as (
    select * from {{source('northwind','Shippers')}}
)
select  
    o.*,
    s.companyname as shippercompanyname,
    od.quantityonorder, od.totalorderamount,
    o.shippeddatekey - o.orderdatekey as daysfromordertoshipped,
    o.requireddatekey - o.orderdatekey as daysfromordertorequired,
    o.shippeddatekey - o.requireddatekey as shippedtorequireddelta,
    case when o.shippeddatekey - o.requireddatekey <=0 then 'Y' else 'N' end as shippedontime
from stg_orders o
    join stg_order_details od on o.orderid = od.orderid
    join stg_shippers s on s.shipperid = o.shipvia
