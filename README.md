# Multinational-retail-data-centralisation

## Overview of the project
    You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location. Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business.

    __Techniques/knowledge__
    AWS RDS database; Python(Pandas, tabula, api, OOP, etc.); PgAdmin; Github; data clean and analysis mind.



## Workflow of the project
    This project will be splitted into three major parts as per the target. 

### Extract and clean data from data sources: extract all data from multiple sources and clean them
    - create all kinds of connections to grasp data from server with and without passcodes and name such a class 'DataExtractor'. Methods included in the class:
        - retrieve_pdf_data: extract pdf content using tabula lib
        - retrieve_stores_data: download the sales store data whose unique APIs are tracked seperately
        - extract_from_s3: download data from AWS S3 bucket
        - extract_datetime_url: download datetime files recorded in json format using request function
    - check each table downloaded and examine the null data or useless data such as the date is random characters, and clean the data with typos. For example, the number of staff is 'J74', you need to remove 'J' from it. Meanwhile, according to the requests, you may classify the data and unify the different value in the same unit. In python, the class named 'DataCleaning' has the following methods:
        - clean_user_data: check the null values and transfer the datatime values to the same format
        - clean_card_details: clean the time related data 
        - clean_store_data: check the null variables and empty proportion of each column, clean the null values and revise the category data, etc.
        - clean_product_data: check null and invalid data, unify the 'weight' record in the same unit 
        - clean_orders_data: check and clean the useless record
        - clean_datetime_date: clean the invalid record from the original record
    - upload cleaned data to database using database_utils.py
    - assemble the above classes, methods into the data_main.py

### Create database schema: develop star-based schema of the database
    - change the type of each column as needed
    - set primary key in dim_tables and foreign key in the orders_table to form a star-based schema database

### Querying the data for business: using sql for analysis
    - navigate and operate the database in pgAdmin4
    - get familar with the sql codes and realize the output as needed
    - upload everything to github
