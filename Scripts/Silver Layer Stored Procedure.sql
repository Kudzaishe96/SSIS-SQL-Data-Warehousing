-----Truncate and insert the clean version table
CREATE OR ALTER PROCEDURE load_silver As
	BEGIN
		BEGIN TRY

				PRINT '=================TRUNCATE silver_cmr_customer_info ======================'

				TRUNCATE TABLE silver_cmr_customer_info;


				PRINT '=================INSERT INTO Silver_cmr_customer_info====================='

				INSERT INTO silver_cmr_customer_info
						select cst_id,
								cst_key,
								Trim(cst_firstname) 'cst_firstname', 
								Trim(cst_lastname) 'cst_lastname',
								Case when Upper(Trim(cst_marital_status)) ='M' then 'Married'
									 when Upper(Trim(cst_marital_status)) = 'S' then 'Single'
									 else 'Other' 
								End 'cst_marital_status',
								Case when Upper(Trim(cst_gndr)) ='M' then 'Male'
									 when Upper(Trim(cst_gndr)) = 'F' then 'Female'
									 else 'Other'
								End 'cst_gndr',
								cst_create_DATE
						from ( Select *,ROW_NUMBER() OVER(PARTITION BY cst_id order by cst_create_DATE desc)  'Flag' 
								from bronze_cmr_customer_info where cst_id is not null)t
						where Flag ='1';


				PRINT '=================TRUNCATE silver_cmr_customer_info ======================'
				TRUNCATE TABLE silver_cmr_product_info;

				
				PRINT '=================INSERT INTO silver_cmr_product_info====================='

				If OBJECT_ID ('silver_cmr_product_info','U') is not Null
				  DROP TABLE silver_cmr_product_info;
					CREATE TABLE silver_cmr_product_info (
						 prd_id					INT ,
						 prd_key		VARCHAR (50),
						 prd_nm			VARCHAR (50),
						 prd_key_id		VARCHAR (50),
						 cat_id			VARCHAR (50),
						 prd_cost				INT ,
						 prd_line		VARCHAR (50),
						 prd_start_dt			DATE,
						 prd_end_dt				DATE,
						 dwd_create_dt  DATEtime2 default GETDATE()
					) ;
		
				INSERT INTO silver_cmr_product_info
						select prd_id,
							   prd_key,
							   prd_nm,
							   SUBSTRING(prd_key,7,LEN(prd_key)) 'prd_key_id',
							   Replace(SUBSTRING(prd_key,1,5),'-','_') 'cat_id',
							   ISNULL(prd_cost,0) 'prd_cost',
							   Case UPPER(Trim(prd_line))
									when 'M' then 'Mountains'
									when 'R' then 'Road'
									when 'S' then 'Other Sales'
									when 'T' then 'Touting'
									else 'n/a'
							   end 'prd_line',
							   Cast(prd_start_dt As DATE) 'prd_start_dt',
							   Cast(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY  prd_start_dt)-1 AS DATE) 'prd_end_dt',
							  GETDATE()
						 from bronze_cmr_product_info
						 order by prd_start_dt Asc

	

				PRINT '=================TRUNCATE silver_cmr_sales_info ======================'
				TRUNCATE TABLE silver_cmr_sales_info;

				PRINT '=================INSERT INTO silver_cmr_sales_info===================='

				If OBJECT_ID ('silver_cmr_sales_info','U') is not Null
				  DROP TABLE silver_cmr_sales_info;
					CREATE TABLE silver_cmr_sales_info (
						 sls_ord_num    VARCHAR (50),
						 sls_prd_key    VARCHAR (50),
						 sls_cust_id			INT ,
						 sls_order_dt		   DATE ,
						 sls_ship_dt		   DATE ,
						 sls_due_dt			   DATE ,
						 sls_sales				INT ,
						 sls_quantity			INT ,
						 sls_price				INT ,
						 dwd_create_dt  DATEtime2 default getDATE()

					) ;

				INSERT INTO silver_cmr_sales_info

						select sls_ord_num ,
							   sls_prd_key,
							   sls_cust_id,
							   Case when sls_order_dt =0 or len(sls_order_dt)!=8 then Null
									Else Cast(Cast(sls_order_dt AS VARCHAR) AS DATE)
							   End 'sls_order_dt',
							   Case when sls_ship_dt =0 or len(sls_ship_dt)!=8 then Null
									Else Cast(Cast(sls_ship_dt AS VARCHAR) AS DATE)
							   End 'sls_order_dt',
							   Case when sls_order_dt =0 or len(sls_due_dt)!=8 then Null
									Else Cast(Cast(sls_due_dt AS VARCHAR) AS DATE)
							   End 'sls_due_dt',
							   Case when sls_sales <=0 or sls_sales is null or sls_sales !=sls_quantity * ABS(sls_price)  then sls_quantity * ABS(sls_price)
									else sls_sales
							   End 'sls_sales',
							   sls_quantity,
							   Case when sls_price <=0 or  sls_price is null then sls_sales /Nullif(sls_quantity,0)
									else sls_price
							   End 'sls_price',
							   GETDATE()
						from bronze_cmr_sales_info





				
				PRINT '=================TRUNCATE silver_erp_customer_info ======================'
				TRUNCATE TABLE silver_erp_customer_info;

				PRINT '=================INSERT INTO silver_erp_customer_info====================='

				If OBJECT_ID ('silver_erp_customer_info','U') is not Null
				  DROP TABLE silver_erp_customer_info;
					CREATE TABLE silver_erp_customer_info (
						 cid   VARCHAR (50),
						 bDATE  DATE ,
						 gen   VARCHAR (50) ,
						 dwd_create_dt  DATEtime2 default GETDATE()

					) ;

				INSERT INTO silver_erp_customer_info
						select substring(cid,4,len(cid)) 'cid',
							Case when BDATE < '1924-01-01' or BDATE > GETDATE() then  null
								 else BDATE
							 End 'bDATE',
							Case when gen = 'F' then 'Female'
								 when gen is null then 'Other'
								 when gen = 'M' then 'Male'
							   else gen
							End 'gen',
							GetDATE()
							 from bronze_erp_customer_info



				PRINT '=================TRUNCATE silver_erp_location ======================'
				TRUNCATE TABLE silver_erp_location;

				PRINT '===============INSERT INTO silver_erp_location ======================'

				If OBJECT_ID ('silver_erp_location','U') is not Null
				  DROP TABLE silver_erp_location;
					CREATE TABLE silver_erp_location (
						 cid			VARCHAR (50),
						 cntry			VARCHAR (50),
						 dwd_create_dt  DATEtime2 default getDATE()

					) ;


				
				INSERT INTO silver_erp_location
						select REPLACE(cid,'-','') 'cid',
							Case when CNTRY = 'US' then  'United States'
								 when CNTRY = 'DE' then 'Germany'
								 when CNTRY = 'USA' then 'United States'
								 when CNTRY is null then 'Other'
							else CNTRY
							End 'cntry',
							GETDATE()
						from bronze_erp_location



	
				PRINT '=================TRUNCATE silver_erp_category_info ======================'
				TRUNCATE TABLE silver_erp_category_info;
				PRINT '=================INSERT INTO silver_erp_category_info===================='

				If OBJECT_ID ('silver_erp_category_info','U') is not Null
				  DROP TABLE silver_erp_category_info;
					CREATE TABLE silver_erp_category_info (
						 id				VARCHAR (50),
						 cat			VARCHAR (50),
						 subcat			VARCHAR (50),
						 maINTenance    VARCHAR (50),
						 dwd_create_dt  DATEtime2 default getDATE()

					) ;


				INSERT INTO silver_erp_category_info	
						select *,GETDATE() from bronze_erp_category_info 

	    END TRY

		BEGIN CATCH

			PRINT'=========================================='
			PRINT'AN ERROR OCCURED DURING THE TRUNCATING PERIOD'
			PRINT'=========================================='

		END CATCH

END