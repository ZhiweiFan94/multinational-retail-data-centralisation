#%%
from data_extraction import DataExtractor 
from database_utils import DatabaseConnector
import pandas as pd

#%%
class DataCleaning:

    def __init__(self) -> None:
        pass

    def clean_user_data(self, selcted_table):
        """
        Alternatively, use the code
        # Convert 'join_date' and 'date_of_birth' to datetime format
        df_users['join_date'] = pd.to_datetime(df_users['join_date'], errors='coerce')
        df_users['date_of_birth'] = pd.to_datetime(df_users['date_of_birth'], errors='coerce')
        # Drop rows where 'join_date' and 'date_of_birth' cannot be transformed to the desired format
        df_users = df_users.dropna(subset=['join_date', 'date_of_birth'])
        # Check for null values and drop rows with any null values
        df_users = df_users.dropna()
        """
        #%%CLEAN TIME DATA
        time_col = 'date_of_birth' 
        date_list=[]
        for i in range(len(selcted_table[time_col])):
            # should notice that the pd.to_datetime can handle different formats only target at single element, if input the whole colomn, it cannot transfer automatically, which is tricky!
            # The '.date()' method is taken to extract the date from the TimeStamp time format
            # errors='coerce': for those dates cannot be transfered are set to 'NaT' which can be tracked by isna() method later
            date = pd.to_datetime(selcted_table[time_col].iloc[i],errors='coerce').date()
            date_list.append(date)
        selcted_table[time_col] = date_list
        selcted_table.drop(selcted_table[selcted_table[time_col].isna()].index, inplace=True)
        #examine the another column about datetime data
        date_list=[]
        time_col = 'join_date'
        for i in range(len(selcted_table[time_col])):
            date = pd.to_datetime(selcted_table[time_col].iloc[i],errors='coerce').date()
            date_list.append(date)
        selcted_table[time_col] = date_list
        selcted_table.drop(selcted_table[selcted_table[time_col].isna()].index, inplace=True)


        return selcted_table
    

    def clean_card_details(self,selcted_table):
        # Remove rows with null values
        df_cleaned = selcted_table.dropna()
        # Convert 'expiry_date' and 'date_payment_confirmed' to datetime format
        df_cleaned['expiry_date'] = pd.to_datetime(df_cleaned['expiry_date'], format='%m/%y', errors='coerce')
        df_cleaned['date_payment_confirmed'] = pd.to_datetime(df_cleaned['date_payment_confirmed'], errors='coerce')
        return df_cleaned


    def clean_store_data(self,selcted_table):
        #%%DATA CLEAN for stores
        # selcted_table = pd.read_csv('store.csv')
        read_colomn = list(selcted_table.columns)
        # Drop the first column from the DataFrame
        # selcted_table = selcted_table.iloc[:,1:]
        selcted_table = selcted_table.drop(columns=read_colomn[0], axis=1)
        selcted_table = selcted_table.drop(columns='lat', axis=1)
        selcted_table.dropna(inplace=True)
        #%%CLEAN TIME DATA
        time_col = 'opening_date' 
        date_list=[]
        for i in range(len(selcted_table[time_col])):
            date = pd.to_datetime(selcted_table[time_col].iloc[i],errors='coerce').date()
            date_list.append(date)
        selcted_table[time_col] = date_list
        selcted_table.drop(selcted_table[selcted_table[time_col].isna()].index, inplace=True)
        #%%CLEAN continent duplications
        selcted_table['continent'].unique()
        mapping_dict = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
        # Use the map() function to replace values in the column, fillna part ensures the element does not occur in the dict remains the same
        selcted_table['continent'] = selcted_table['continent'].map(mapping_dict).fillna(selcted_table['continent'])
        return selcted_table
    

    def clean_products_data(self,selected_table):
        #%% convert all weights in kg
        #filter all values that does not cotain 'g,kg,ml'
        for i in range(len(selected_table['weight'])):
            x = str(selected_table['weight'].iloc[i])
            if ('g' or 'kg' or 'ml') not in x:
                selected_table['weight'].iloc[i] = None
            elif 'x' in x:
                selected_table['weight'].iloc[i] = None
            elif x[-1]=='.' or x[-1]==' ':
                selected_table['weight'].iloc[i] = x[:-1]
                x = selected_table['weight'].iloc[i]
                if x[-1]=='.' or x[-1]==' ':
                    selected_table['weight'].iloc[i] = x[:-1]
        #drop the empty raws
        selected_table.drop(selected_table[selected_table['weight'].isna()].index, inplace=True)
        #%%convert g and ml to kg unit
        for i in range(len(selected_table['weight'])):
            try:
                if ('g' or 'ml' in selected_table['weight'].iloc[i]) and ('kg' not in selected_table['weight'].iloc[i]):
                    unit_cell = selected_table['weight'].iloc[i]
                    if 'g' in unit_cell:
                        value = float(unit_cell[:-1])/1000
                        selected_table['weight'].iloc[i] = str(value)+'kg'
                    elif 'ml' in unit_cell:
                        value = float(unit_cell[:-2])/1000
                        selected_table['weight'].iloc[i] = str(value)+'kg'
            except:
                print(selected_table.iloc[i])
        # %% convert to float type without showing unit of kg
        selected_table['weight'] = selected_table['weight'].str.strip('kg')
        selected_table['weight'] = selected_table['weight'].astype(float)
        return selected_table
    

    def clean_orders_data(self,selcted_table):
        selcted_table.drop(columns=['first_name','last_name','1'],inplace=True)
        return selcted_table
    

    def clean_datetime_date(self,selcted_table):
        for i in range(len(selcted_table['year'])):
            x = selcted_table['year'].iloc[i]
            if len(str(x)) != 4 or x=='NULL':
                selcted_table['year'].iloc[i]=None
        selcted_table.drop(selcted_table[selcted_table['year'].isna()].index, inplace=True)



# ###### grasp the data
# get_db = DataExtractor()
# #link to the database
# engine = get_db.init_db_engine()
# #read list in the database
# list_table = get_db.list_db_tables()
# #get table and transfer to pandas form
# selcted_table = get_db.read_rds_table(list_table[1])

# #%%DATA CLEAN
# #look at basic info
# selcted_table.info()
# selcted_table.head(10)
# #%%CLEAN THE MISSING PART
# #get variable from column names and check missing values
# table_col = list(selcted_table.columns)
# selcted_table.isnull().sum()

# #%%
# ###### grasp the data
# get_db = DataExtractor()
# #get table and transfer to pandas form
# selcted_table = get_db.retrieve_stores_data()

# #%%orignial table for milestone2task5 backup
# store_table = selcted_table

# #%%DATA CLEAN for stores
# selcted_table = pd.read_csv('store.csv')
# read_colomn = list(selcted_table.columns)
# # Drop the first column from the DataFrame
# # selcted_table = selcted_table.iloc[:,1:]
# selcted_table = selcted_table.drop(columns=read_colomn[0], axis=1)
# selcted_table = selcted_table.drop(columns='lat', axis=1)
# selcted_table.dropna(inplace=True)
# #%%CLEAN TIME DATA
# time_col = 'opening_date' 
# date_list=[]
# for i in range(len(selcted_table[time_col])):
#     # should notice that the pd.to_datetime can handle different formats only target at single element, if input the whole colomn, it cannot transfer automatically, which is tricky!
#     # The '.date()' method is taken to extract the date from the TimeStamp time format
#     # errors='coerce': for those dates cannot be transfered are set to 'NaT' which can be tracked by isna() method later
#     date = pd.to_datetime(selcted_table[time_col].iloc[i],errors='coerce').date()
#     date_list.append(date)
# selcted_table[time_col] = date_list
# selcted_table.drop(selcted_table[selcted_table[time_col].isna()].index, inplace=True)
# #%%CLEAN continent duplications
# selcted_table['continent'].unique()
# mapping_dict = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
# # Use the map() function to replace values in the column, fillna part ensures the element does not occur in the dict remains the same
# selcted_table['continent'] = selcted_table['continent'].map(mapping_dict).fillna(selcted_table['continent'])

# #%%
# update_db = DatabaseConnector()
# update_db.upload_to_db(selcted_table,'dim_store_details')


# %%
# #%%
# selected_table = pd.read_csv('products.csv')
# #%% convert all weights in kg
# #filter all values that does not cotain 'g,kg,ml'
# for i in range(len(selected_table['weight'])):
#     x = str(selected_table['weight'].iloc[i])
#     if ('g' or 'kg' or 'ml') not in x:
#         selected_table['weight'].iloc[i] = None
#     elif 'x' in x:
#         selected_table['weight'].iloc[i] = None
#     elif x[-1]=='.' or x[-1]==' ':
#         selected_table['weight'].iloc[i] = x[:-1]
#         x = selected_table['weight'].iloc[i]
#         if x[-1]=='.' or x[-1]==' ':
#             selected_table['weight'].iloc[i] = x[:-1]
# #drop the empty raws
# selected_table.drop(selected_table[selected_table['weight'].isna()].index, inplace=True)
# #%%convert g and ml to kg unit
# for i in range(len(selected_table['weight'])):
#     try:
#         if ('g' or 'ml' in selected_table['weight'].iloc[i]) and ('kg' not in selected_table['weight'].iloc[i]):
#             unit_cell = selected_table['weight'].iloc[i]
#             if 'g' in unit_cell:
#                 value = float(unit_cell[:-1])/1000
#                 selected_table['weight'].iloc[i] = str(value)+'kg'
#             elif 'ml' in unit_cell:
#                 value = float(unit_cell[:-2])/1000
#                 selected_table['weight'].iloc[i] = str(value)+'kg'
#     except:
#         print(selected_table.iloc[i])
# # %% convert to float type without showing unit of kg
# selected_table['weight'] = selected_table['weight'].str.strip('kg')
# selected_table['weight'] = selected_table['weight'].astype(float)
# # #%%
# update_db = DatabaseConnector()
# update_db.upload_to_db(selected_table,'dim_products')
# %%milestone2 task7
# ###### grasp the data
# get_db = DataExtractor()
# #link to the database
# engine = get_db.init_db_engine()
# #read list in the database
# list_table = get_db.list_db_tables()
# #get table and transfer to pandas form
# selcted_table = get_db.read_rds_table(list_table[2])
# # # %%save the download sheet
# # import pandas as pd
# # selcted_table.to_csv('orders.csv',index=False)
# # # %%
# # selcted_table=pd.read_csv('orders.csv')
# #%%
# selcted_table.drop(columns=['first_name','last_name','1'],inplace=True)
# #%%
# update_db = DatabaseConnector()
# update_db.upload_to_db(selcted_table,'orders_table')

# # %%milestone 2 task 8
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

# # #%%
# update_db = DatabaseConnector()
# update_db.upload_to_db(selcted_table,'dim_date_times')

# # %%
# #%%
# import pandas as pd
# df = pd.read_csv('card_details.csv')
# # Check for null values
# null_values = df.isnull().sum()

# # Analyze data types of each column
# data_types = df.dtypes

# null_values, data_types

# # Remove rows with null values
# df_cleaned = df.dropna()

# # Convert 'expiry_date' and 'date_payment_confirmed' to datetime format
# df_cleaned['expiry_date'] = pd.to_datetime(df_cleaned['expiry_date'], format='%m/%y', errors='coerce')
# df_cleaned['date_payment_confirmed'] = pd.to_datetime(df_cleaned['date_payment_confirmed'], errors='coerce')

# # Check the first few rows of the cleaned DataFrame and its data types
# df_cleaned.head(), df_cleaned.dtypes

# # %%
# update_db = DatabaseConnector()
# update_db.upload_to_db(df_cleaned,'dim_card_details')
# # %%
