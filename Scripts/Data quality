--check for the number of duplicate rows
Select cst_id, count(*)
from silver_cmr_customer_info
group by cst_id
having COUNT(*)> 1


--- rank the rows by recent date
select * from 
	( Select *,ROW_NUMBER() OVER(PARTITION BY cst_id order by cst_create_date desc)  'Flag' 
	from silver_cmr_customer_info )t
	where Flag ='1'

----check for spaces in between the names,gender 
select cst_firstname 
from silver_cmr_customer_info
where  cst_firstname !=Trim(cst_firstname)

---checking for data Consistancy in tables
select distinct cst_marital_status 
from silver_cmr_customer_info



--validating Date data 
select Nullif(sls_order_dt,0) sls_oder_dt
from bronze_cmr_sales_info
where sls_order_dt <=0 Or len(sls_order_dt)!=8 or sls_order_dt > 20500101 or sls_order_dt <19000101

---- validating date data
select * from bronze_cmr_sales_info
where  sls_order_dt > sls_due_dt or sls_order_dt > sls_ship_dt

