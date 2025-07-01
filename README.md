# SSIS-SQL-Data-Warehousing
Use SSIS to create a Data Warehouse with SQL


## Table Of Contents

- [ Project Overview ](#Project-Overview)
- [ SSIS-SQL-Data-Warehousing Architecture](#SSIS-SQL-Data-Warehousing-Architecture)
- [ Data Source ](#Data-Source)
- [ Tools ](#Tools)
- [ Data Extraction (Bronze Layer) ](#Data-Extraction-(Bronze-Layer))
- [ Data Cleaning (Silver Layer) ](#Data-Cleaning-(Silver-Layer))
- [ Gold Layer](#Gold-Layer)
- [ SSIS Data Flow](#SSIS-Data-Flow)


### Project Overview

This project seek to show the extraction of raw data into meaningfull sql views for data analytics 


## SSIS-SQL-Data-Warehousing Architecture

![SSIS Data Architecture](https://github.com/user-attachments/assets/e3e65033-39f9-4752-b0f9-a801222f286a)


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
1. Check for data quality by using the sql script (Data quality) located in the scripts folder
2. Remove all duplicates and check for Consistancy for all tables
3. Create a stored procedure (Silver Layer) in the scripts folder and execute it to store the new tables in the Silver Layer
   


### Gold Layer (Views)
1. Run data quality checks from the silver layer
2. Use the silver layer tables to create new Gold views namely gold_dim_customer ,gold_dim_products and gold_facts_sales  views


### SSIS Data Flow

 ### *Control Flow--*
 ![Control Flow](https://github.com/user-attachments/assets/862b379e-d152-4a63-a6d0-48cac01c4885)

 ### *Data Flow --*
 ![Data Flow -Control Flow](https://github.com/user-attachments/assets/d7b412d6-3930-40c2-8515-6e0bafad6c1a)


 #  The End

