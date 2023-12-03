with f_order_fulfillment as (
    select * from {{ ref('fact_order_fulfillment') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_employee as (
    select * from {{ ref('dim_employee') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
)
select 
    d_customer.*, 
    d_employee.*, 
    d_date.*,
    f.orderid, f.orderdatekey, f.shippeddatekey, f.requireddatekey, f.shipname, f.shipaddress, f.shipcity, f.shipregion, f.shippostalcode, f.shipcountry,
    f.freight, f.shipvia, f.shippercompanyname, f.quantityonorder, f.totalorderamount, f.daysfromordertoshipped, f.daysfromordertorequired, f.shippedtorequireddelta, f.shippedontime
    from f_order_fulfillment as f
    left join d_customer on f.customerkey = d_customer.customerkey
    left join d_employee on f.employeekey = d_employee.employeekey
    left join d_date on f.orderdatekey = d_date.datekey
