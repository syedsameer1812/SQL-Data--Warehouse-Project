---cleaning the data from bronze and insert in to silver layer--
--select * from bronze.crm_cust_info;
create or alter procedure silver.load_silver as
begin
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: sliver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: sliver.crm_cust_info';
		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date)

		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when upper(trim(cst_marital_status))='S' then 'Single'
			 when upper(trim(cst_marital_status))='M' then 'Married'
			 else 'n/a'
		end cst_marital_status,
		case when upper(trim(cst_gndr))='F' then 'Femal'
			 when upper(trim(cst_gndr))='M' then 'Male'
			 else 'n/a'
		end cst_gnfr,
		cst_create_date
		 from(
		 select*,
		 row_number() over (partition by cst_id order by cst_create_date desc)as flag_last
		 from bronze.crm_cust_info
		 where cst_id is not null 
		 )t where flag_last = 1 ;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
  


         SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>> Inserting Data Into: silver.crm_prd_info';
  ----after alter date insert into sliver.crm_prd_info table--
  insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
  Select 
  prd_id,
  REPLACE(substring(prd_key,1,5),'-','_') as cat_id,
  substring(prd_key,7, len(prd_key)) as Prd_key,
  prd_nm,
  isnull(prd_cost,0)as prd_cost,
  case upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'n/a'
end as prd_line,
cast(prd_start_dt as Date)as Prd_start_dt,
cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date)as prd_end_dt
from bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

--select* from silver.crm_prd_info;



        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
  ----after alter date insert into sliver.crm_sales_details--
insert into silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case 
	when sls_order_dt =0 or len(sls_order_dt) !=8 then null
	else cast(cast(sls_order_dt as varchar)as date)
	end as sls_order_dt,
case 
	when sls_ship_dt =0 or len(sls_ship_dt) !=8 then null
	else cast(cast(sls_ship_dt as varchar)as date)
	end as sls_ship_dt,
case
	when sls_due_dt =0 or len(sls_due_dt) !=8 then null
	else cast(cast(sls_due_dt as varchar)as date)
	end as sls_due_dt,
case 
	when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
	then sls_quantity * ABS(sls_price)
	else sls_sales
end as sls_sales,
sls_quantity,
case 
	when sls_price is null or sls_price <=0
	then sls_sales/nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

--select * from silver.crm_sales_details;


		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
 ----after alter date insert into sliver.erp_cust_az12--
insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
select 
case when cid like 'NAS%' then substring(cid,4,len(cid))
	else cid
end as cid ,
case when bdate > getdate() then null
	else bdate
end as bdate,
case when upper(trim(gen)) in ('F' , 'FEMALE') then 'Female'
	 when upper(trim(gen)) in ('M' , 'MALE') then 'Male'
	 else 'n/a'
end as gen
from bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

--select* from silver.erp_cust_az12;


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';

----after alter date insert into sliver.erp_loc_a101--
insert into silver.erp_loc_a101(
	cid,
	cntry
	)
Select 
replace(cid,'-','')cid,
case when trim(cntry) = 'DE' then 'Germany'
	 when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim(cntry)
end cntry
from bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

--select * from silver.erp_loc_a101;



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
----after alter date insert into sliver.erp_px_cat_g1v2--

insert into silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
Select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

--select * from silver.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';

exec silver.load_silver;
