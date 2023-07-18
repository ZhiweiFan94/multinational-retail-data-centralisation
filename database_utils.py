from sqlalchemy import create_engine

class DatabaseConnector:
    
    def __init__(self) -> None:
        pass

    def my_sales_database(self):
        '''
        connect to my sales local database
        '''
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = '127.0.0.1'
        USER = 'postgres'
        PASSWORD = 'Dylan1994'
        DATABASE = 'sales_data'
        PORT = 5432
        sales_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return sales_engine
    
    def upload_to_db(self, table, table_name):
        # new_table = pd.DataFrame(table)
        sales_engine = self.my_sales_database()
        table.to_sql(table_name, sales_engine, if_exists='replace')

