#%%
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
import pandas as pd
import tabula
import requests
import boto3


class DataExtractor:
    

    def __init__(self) -> None:
        pass
    
    def read_db_creds(self):
        '''
        read credentials of the database
        '''
        with open("db_creds.yaml", "r") as f:
            return yaml.safe_load(f)

    def init_db_engine(self):
        '''
        connect to the database
        '''
        self.creds = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.creds['RDS_HOST']
        USER = self.creds['RDS_USER']
        PASSWORD = self.creds['RDS_PASSWORD']
        DATABASE = self.creds['RDS_DATABASE']
        PORT = 5432
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")        
        return self.engine

    def list_db_tables(self):
        '''
        check the database the name of tables
        '''
        with self.engine.connect() as connection:
            self.inspector = inspect(self.engine)
            self.table_names = self.inspector.get_table_names()
        return self.table_names 
    
    def read_rds_table(self, table_name):
        '''
        read/load tables from the database
        '''
        return pd.read_sql_table(table_name, self.engine)
    

    def retrieve_pdf_data(self, pdf_path):
        '''
        extract info from a pdf file to pd.dataframe
        '''
        self.pdf_info = tabula.read_pdf(pdf_path, pages='all')
        return self.pdf_info
    
    def list_number_of_stores(self, endpoint, header):
        '''
        it returns the number of stores contained
        '''
        reponse = requests.get(endpoint, headers=header)
        if reponse.status_code==200:
            self.total_number = reponse.json()['number_stores']
            return self.total_number
        
    def retrieve_single_stores_data(self, endpoint, header):
        '''
        extract info for a specific store as per the store index provided
        '''
        reponse = requests.get(endpoint, headers=header)
        if reponse.status_code==200:
            store_list = reponse.json()
            return store_list
        else:
            return 'the server does not respond'
    
    def retrieve_stores_data(self):
        '''
        extract all stores infos through api
        '''
        store_info = []
        self.list_number_of_stores(endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header={"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
        API_HEADERS = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        for store_number in range(1, self.total_number+1):
            API_STORE = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            Data_store = self.retrieve_single_stores_data(API_STORE, header=API_HEADERS)
            store_info.append(Data_store)
            print(store_number)
        return store_info
    

    def extract_from_s3(self, s3_address):
        # Split the S3 address into bucket name and file key
        bucket_name, file_key = s3_address.replace("s3://", "").split("/", 1)
        # Create a session using AWS credentials
        session = boto3.Session()
        # Create an S3 client using the session
        s3_client = session.client("s3")
        # Download the file from the S3 bucket
        download_path = "/Users/fanzhiwei/Desktop/Aicore-test/multinational-retail-data-centralisation/file.csv"  # Specify the local path to save the downloaded file
        s3_client.download_file(bucket_name, file_key, download_path)
        # Read the downloaded CSV file into a pandas DataFrame
        df = pd.read_csv(download_path)
        return df
    
    
    def extract_datetime_url(self):
        url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        reponse = requests.get(url)
        df = reponse.json()
        return df
    



#%%



#####obtain database tables
# get_db.init_db_engine()
# tables = get_db.list_db_tables()
# store_detail = get_db.read_rds_table('legacy_store_details')

####obtain pdf data
# pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# pdf_path = '/Users/fanzhiwei/Desktop/Aicore-test/multinational-retail-data-centralisation/card_details.pdf'
# pdf_content = get_db.retrieve_pdf_data(pdf_path)
# print(pdf_content)

####api port  {API_ENDPOINT}?x-api-key={API_KEY}
# #number of stores api input
# API_ENDPOINT = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
# API_HEADERS = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
# number_of_stores = get_db.list_number_of_stores(API_ENDPOINT, API_HEADERS)
# #it returns the whole store info
# store_datas = get_db.retrieve_stores_data()





# # # %%
# import tabula
# pdf_path = "/Users/fanzhiwei/Desktop/Aicore-test/multinational-retail-data-centralisation/card_details.pdf"
# dfs = tabula.read_pdf(pdf_path, pages = 1)
# # read_pdf returns list of DataFrames
# print(len(dfs))
# dfs[0]
# # # %%

# %%
# def main():
#     xxxxxx
# if __name__ == "__main__":
#     main()


# #%%
# import boto3
# # Create a session using AWS credentials
# session = boto3.Session()

# # Create an S3 client using the session
# s3_client = session.client('s3')

# # Specify the S3 bucket and file key
# bucket_name = 'data-handling-public'
# file_key = 'date_details.json'

# # Download the JSON file from S3
# download_path = '/Users/fanzhiwei/Desktop/Aicore-test/multinational-retail-data-centralisation'  # Specify the local path to save the downloaded file
# s3_client.download_file(bucket_name, file_key, download_path)
# %% milestone2 task 8
# import requests
# url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
# reponse = requests.get(url)
# store_list = reponse.json()
# # %%
# import pandas as pd
# selcted_table = pd.DataFrame.from_dict(store_list)
# #%%
# for i in range(len(selcted_table['year'])):
#     x = selcted_table['year'].iloc[i]
#     if len(str(x)) != 4 or x=='NULL':
#         selcted_table['year'].iloc[i]=None
# #%%
# selcted_table.drop(selcted_table[selcted_table['year'].isna()].index, inplace=True)

