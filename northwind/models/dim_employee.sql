with stg_employees as (
    select * from {{source('northwind','Employees')}}
),
stg_supervisors as (
    select * from {{source('northwind','Employees')}}
)
select 
    {{ dbt_utils.generate_surrogate_key(['e.employeeid']) }} 
       as employeekey, 
    e.employeeid,
    concat(e.lastname ,', ' , e.firstname) as employeenamelastfirst,
    concat(e.firstname  , ' ' , e.lastname) as employeenamefirstlast,
    e.title as employeetitle,
    concat(s.lastname ,', ' , s.firstname) as supervisornamelastfirst,
    concat(s.firstname  , ' ' , s.lastname) as supervisornamefirstlast
from stg_employees e
    left join stg_supervisors s on e.reportsto = s.employeeid
