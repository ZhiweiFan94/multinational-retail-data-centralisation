import yaml
import tabula
import requests
import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DataExtractor:
    """
    extract dataset from different sources, it contains different methods explained in Readme file.
    """

    def __init__(self):
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
        pdf_info = tabula.read_pdf(pdf_path, pages='all')
        # Convert each list table to a DataFrame
        dataframes = []
        for table in pdf_info:
            dataframe = pd.DataFrame(table)
            dataframes.append(dataframe)
        # Concatenate all DataFrames into a single DataFrame
        combined_dataframe = pd.concat(dataframes)
        return combined_dataframe
    
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
        
    def storeapi_creds(self):
        with open("store_creds.yaml", "r") as f:
           return yaml.safe_load(f)
    
    def retrieve_stores_data(self):
        '''
        extract all stores infos through api
        '''
        self.store_creds = self.storeapi_creds()
        NUM_ENDPOINT = self.store_creds['NUM_ENDPOINT']
        STORE_ENDPOINT = self.store_creds['STORE_ENDPOINT']
        API_KEY =   self.store_creds['API_KEY']
        #extract info using api
        store_info = []
        self.list_number_of_stores(endpoint=f"{NUM_ENDPOINT}", header={"x-api-key": f"{API_KEY}"})
        API_HEADERS = {"x-api-key": f"{API_KEY}"}
        for store_number in range(self.total_number):
            API_STORE = f'{STORE_ENDPOINT}/{store_number}'
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
        download_path = "./multinational-retail-data-centralisation/file.csv"  # Specify the local path to save the downloaded file
        s3_client.download_file(bucket_name, file_key, download_path)
        # Read the downloaded CSV file into a pandas DataFrame
        df = pd.read_csv(download_path)
        return df
    
    
    def extract_datetime_url(self):
        url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        reponse = requests.get(url)
        df = reponse.json()
        return df
    