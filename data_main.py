#%% import all classes/ methods developed for data manipulations
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

#%%                                 milestone2 task3
########################################################################################
###### import table form data from database
get_db = DataExtractor()
#link to the database
engine = get_db.init_db_engine()
#read list in the database
list_table = get_db.list_db_tables()
print(list_table)
#get table and transfer to pandas form
selcted_table = get_db.read_rds_table(list_table[1])
#%%clean table
clean_data = DataCleaning()
cleaned_table = clean_data.clean_user_data(selcted_table)
#%%upload the cleaned table to my sales database
update_db = DatabaseConnector()
update_db.upload_to_db(cleaned_table,'dim_users')


#%%                                 milestone2 task4
########################################################################################
###### import table form data from database
get_db = DataExtractor()
url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
selcted_table = get_db.retrieve_pdf_data(url)
#%%clean table
clean_data = DataCleaning()
cleaned_table = clean_data.clean_card_details(selcted_table)
#%%upload the cleaned table to my sales database
update_db = DatabaseConnector()
update_db.upload_to_db(cleaned_table,'dim_card_details')

#%%                                 milestone2 task5
########################################################################################
###### import table form data from database
###### grasp the data
get_db = DataExtractor()
#get table and transfer to pandas form
selcted_table = get_db.retrieve_stores_data()
#%%clean table
clean_data = DataCleaning()
cleaned_table = clean_data.clean_store_data(selcted_table)
#%%upload the cleaned table to my sales database
update_db = DatabaseConnector()
update_db.upload_to_db(cleaned_table,'dim_store_details')


#%%                                 milestone2 task6
########################################################################################
###### import table form data from database
###### grasp the data
get_db = DataExtractor()
#get table and transfer to pandas form
selcted_table = get_db.extract_from_s3('s3://data-handling-public/products.csv')
#%%clean table
clean_data = DataCleaning()
cleaned_table = clean_data.clean_products_data(selcted_table)
#%%upload the cleaned table to my sales database
update_db = DatabaseConnector()
update_db.upload_to_db(cleaned_table,'dim_products')


#%%                                 milestone2 task7
########################################################################################
#grasp data
get_db = DataExtractor()
engine = get_db.init_db_engine()
list_table = get_db.list_db_tables()
#list_table[2] contains the info regarding orders
selcted_table = get_db.read_rds_table(list_table[2])
#clean table
clean_data = DataCleaning()
cleaned_table = clean_data.clean_orders_data(selcted_table)
#upload
update_db = DatabaseConnector()
update_db.upload_to_db(selcted_table,'orders_table')

#%%                                 milestone2 task8
########################################################################################
#grasp data
get_db = DataExtractor()
selcted_table = get_db.extract_datetime_url()
#clean table
clean_data = DataCleaning()
cleaned_table = clean_data.clean_datetime_date(selcted_table)
#upload
update_db = DatabaseConnector()
update_db.upload_to_db(selcted_table,'dim_date_times')