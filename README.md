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
- SSIS
- SQL Server Management Studio

### Data Extraction 
1. Create A json format file for simulating an api call
2. Use the JSON file to extract the parameter values for the CSV files (Data Source) using an api that we feed into a lookup activity in Synapse Analytics
3. Use the lookup activity output values to feed the For loop activity which contains the Copy activity for our data sources.
4. Use parameters to create the file directory system in a hierachical way.
5. Run the Pipeline and extract the raw data into An Azure Data lake

   ### *Load Data--step 1*
   
  ![Screenshot 2025-06-24 174606](https://github.com/user-attachments/assets/903e5825-fb6a-44d3-bd03-03e6d0687668)

   ### *Load Data--step 2*
  
  ![Screenshot 2025-06-24 175017](https://github.com/user-attachments/assets/e8bdde12-bf57-4a57-acfa-864127d65946)


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

