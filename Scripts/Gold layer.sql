--Gold Layer Views----

CREATE VIEW gold_dim_customers As
select ROW_NUMBER() Over(ORDER BY d.cst_id) 'customer_key',
	   d.cst_id  'customer_id',
	   d.cst_key 'customer_number',
	   d.cst_firstname 'first_name',
	   d.cst_lastname   'last_name',
	   d.cst_marital_status 'marital_status',
	   v.cntry 'country',
	   Case when d.cst_gndr != 'n/a' then d.cst_gndr
	   Else Coalesce(c.gen,'n/a') 
	   End 'gender',
	   c.bDATE  'birth_date',
	   d.cst_create_date 'created_date'
from silver_cmr_customer_info d
left join silver_erp_customer_info c
on d.cst_key =c.cid
left join silver_erp_location v
on d.cst_key =v.cid;


CREATE VIEW gold_dim_products AS
select ROW_NUMBER() Over(ORDER BY v.prd_id) 'product_key',
		v.prd_id 'product_id',
		v.prd_key 'product_number' ,
		v.prd_nm   'product_name',
		v.prd_key_id 'product_key_number',
		v.prd_cost  'product_cost',
		v.prd_line   'product_line',
		v.prd_start_dt  'start_date' ,
		v.prd_end_dt     'end_date',
		v.cat_id   'category_id',
		b.cat      'category',
		b.subcat    'sub-category',
		b.maINTenance 'maintainance'
from silver_cmr_product_info v
left join silver_erp_category_info b
on b.id =v.cat_id
where v.prd_end_dt is null


CREATE VIEW gold_facts_sales AS
select
		s.sls_ord_num 'order_number',
		p.product_key 'product_key',
		c.customer_id 'customer_key',
		s.sls_order_dt 'sales_oder_date',
		s.sls_ship_dt   'sales_ship_date',
		s.sls_due_dt   'sales_due_date',
		s.sls_sales  'sales',
		s.sls_quantity  'sales_quantity',
		s.sls_price   'sale_price'
from silver_cmr_sales_info s
left join gold_dim_customers c
on s.sls_cust_id =c.customer_id
left join gold_dim_products p
on s.sls_prd_key =p.product_key_number
