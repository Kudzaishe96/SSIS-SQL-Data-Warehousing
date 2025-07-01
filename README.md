# SSIS-SQL-Data-Warehousing
Use SSIS to create a Data Warehouse with SQL


## Table Of Contents

- [ Project Overview ](#Project-Overview)
- [ Data Source ](#Data-Source)
- [ Tools ](#Tools)
- [ Data Extraction (Bronze Layer) ](#Data-Extraction-(Bronze-Layer))
- [ Data Cleaning (Silver Layer) ](#Data-Cleaning-(Silver-Layer))
- [ Gold Layer](#Gold-Layer)


### Project Overview

This project seek to show the extraction of raw data into meaningfull sql views for data analytics 


### Data Source
EMR and CMR Data csv files uploaded in the Data Source

### Tools
-  Microsoft Visual Studio (SSIS)
- SQL Server Management Studio

### Data Extraction 
1. Create a new Microsoft Microsoft Visual Studio (SSIS) project and create Flat files Datasources for the two Datasets(source_cmr,source_erp)
2. Create a SQL Data Destination connection and tables for storing information from the two extracted Datasets into the bronze layer 
3. Create a pipeline for the extraction process
4. Run the pipeline

   ### *Load Data--step 1*
   ![Data Flow -Control Flow](https://github.com/user-attachments/assets/e5f16e34-cb10-48e2-a1d0-0b95b4c29485)

   ### *Connection Properties--step 1*
   ![image](https://github.com/user-attachments/assets/012c2ee0-8fc6-4faa-8e8a-0bb05babdcc6)




### Data Cleaning 
1. Create an App that you attach a read and write access role using the managed identity
2. Use the app creadentials to attach it to the DataBricks application to connect to the Bronze Layer(pyspark code in Scripts folder)
3. Load Data into the Silver layer by using Pyspark
4. Remove duplicates, null values and create three tables namely dim_products, dim_customer, facts_sales
5. Store tables in the Silver layer as delta format
   

   ### *Silver Layer*
   ![Screenshot 2025-06-24 175611](https://github.com/user-attachments/assets/3e17e064-58cd-4307-aff4-1fe145cc681d)


### Gold Layer (Views)
1. Create a Serveless SQL Server in Azure Sysnapse Analytics.
2. Create a Database called Datawarehouse
3. Extract Delta data from the Silver layer using SQL and create gold_views namely customer ,products and sales gold views

   

 #  The End

