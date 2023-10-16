import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    
    def read_keys(self):
        with open('sale_db_key.yaml','r') as f:
            return yaml.safe_load(f)
        

    def my_sales_database(self):
        '''
        connect to my sales local database
        '''
        self.creds = self.read_keys()
        DATABASE_TYPE = self.creds['DATABASE_TYPE']
        DBAPI = self.creds['DBAPI']
        HOST = self.creds['HOST']
        USER = self.creds['USER']
        PASSWORD = self.creds['PASSWORD']
        DATABASE = self.creds['DATABASE']
        PORT = self.creds['PORT']
        sales_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return sales_engine
    
    
    def upload_to_db(self, table, table_name):
        # new_table = pd.DataFrame(table)
        sales_engine = self.my_sales_database()
        table.to_sql(table_name, sales_engine, if_exists='replace')

