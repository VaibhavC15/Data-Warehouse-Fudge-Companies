with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_employee as (
    select * from {{ ref('dim_employee') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
),
d_product as (
    select * from {{ ref('dim_product') }}
)

select
    s.Orderid,
    d_customer.*,
    d_employee.*,
    d_date.*,
    s.orderdatekey,
    d_product.*,
    s.Quantity,
    s.extendedpriceamount,
    s.discountamount,
    s.soldamount
from f_sales as s
    left join d_customer on s.customerkey = d_customer.customerkey
    left join d_employee on s.employeekey = d_employee.employeekey
    left join d_product on s.productkey = d_product.productkey
    left join d_date on s.orderdatekey = d_date.datekey

