import pandas as pd

class DataCleaning:

    def __init__(self) -> None:
        pass

    def clean_user_data(self, selcted_table):
        """
        unify datetime record 
        """
        #%%CLEAN TIME DATA
        time_col = 'date_of_birth' 
        date_list=[]
        for i in range(len(selcted_table[time_col])):
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
        """
        transfer datetime data to the same format
        """
        # Remove rows with null values
        df_cleaned = selcted_table.dropna()
        # Convert 'expiry_date' and 'date_payment_confirmed' to datetime format
        df_cleaned['expiry_date'] = pd.to_datetime(df_cleaned['expiry_date'], format='%m/%y', errors='coerce')
        df_cleaned['date_payment_confirmed'] = pd.to_datetime(df_cleaned['date_payment_confirmed'], errors='coerce')
        return df_cleaned


    def clean_store_data(self,selcted_table):
        """
        unify the categories and drop invalid record
        """
        #%%DATA CLEAN for stores
        read_colomn = list(selcted_table.columns)
        selcted_table = selcted_table.drop(columns=read_colomn[0], axis=1)
        selcted_table = selcted_table.drop(columns='lat', axis=1)
        selcted_table.drop(selcted_table[selcted_table['store_type'].isna()].index,inplace=True)
        selcted_table.drop(selcted_table[selcted_table['country_code'].apply(lambda x: x not in ['GB','DE','US'])].index, inplace = True)
        selcted_table['staff_numbers'] = selcted_table['staff_numbers'].str.extract(r'(\d+)', expand=False).astype(float)
        #%%CLEAN TIME DATA
        time_col = 'opening_date' 
        date_list=[]
        for i in range(len(selcted_table[time_col])):
            date = pd.to_datetime(selcted_table[time_col].iloc[i],errors='coerce').date()
            date_list.append(date)
        selcted_table[time_col] = date_list
        #%%CLEAN continent duplications
        selcted_table['continent'].unique()
        mapping_dict = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
        selcted_table['continent'] = selcted_table['continent'].map(mapping_dict).fillna(selcted_table['continent'])
        return selcted_table
    

    def clean_products_data(self,df):
        """
        clean the categories and unify the weights measured in various units
        """
        scope = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty',
       'food-and-drink', 'diy']
        df.drop(df[df['category'].apply(lambda x: x not in scope)].index, inplace=True)
        #%%deal with the values contains multuple symbol
        df_multi = df[df['weight'].str.contains('x')]
        df_multi['weight'] = df_multi['weight'].str.strip('g')
        for i in range(len(df_multi['weight'])):
            df_multi['weight'].iloc[i] = df_multi['weight'].iloc[i].replace(' ','')
            numbers = [int(num) for num in df_multi['weight'].iloc[i].split('x')]
            result = numbers[0] * numbers[1]
            df_multi['weight'].iloc[i] = str(result) + 'g'
        # update the old with the revised one with index matched
        df.update(df_multi)
        #%%deal with the values contains ['kg', 'g', 'ml', 'oz~28.35g']
        df['weight'] = df['weight'].str.replace(' ','')
        df['weight'] = df['weight'].str.strip('.')
        df['ori_unit'] = df['weight'].str.strip('0123456789.')
        for i in range(len(df['weight'])):
            if df['ori_unit'].iloc[i] == 'kg':
                df['weight'].iloc[i] = df['weight'].iloc[i].strip('kg')
            elif df['ori_unit'].iloc[i] == 'g':
                df['weight'].iloc[i] = df['weight'].iloc[i].strip('g')
                df['weight'].iloc[i] = str(float(df['weight'].iloc[i])/1000)
            elif df['ori_unit'].iloc[i] == 'ml':
                df['weight'].iloc[i] = df['weight'].iloc[i].strip('ml')
                df['weight'].iloc[i] = str(float(df['weight'].iloc[i])/1000)
            elif df['ori_unit'].iloc[i] == 'oz':
                df['weight'].iloc[i] = df['weight'].iloc[i].strip('oz')
                df['weight'].iloc[i] = str(float(df['weight'].iloc[i])*28.35/1000)
        df.drop(columns='ori_unit',inplace=True)
        df['weight']=df['weight'].astype(float)
        return df
    

    def clean_orders_data(self,selcted_table):
        """
        remove the useless record
        """
        selcted_table.drop(columns=['first_name','last_name','1'],inplace=True)
        return selcted_table
    

    def clean_datetime_date(self,df):
        """
        filter the valid time
        """
        #%% delete all raws where the [year] column is not a valid year
        valid_year = ['2012', '1997', '1994', '2001', '2015', '2002', '1993', '2006',
            '2004', '2008', '2021', '2018', '2009', '2020', '2017', '2019',
            '2000', '2007', '2013', '2010', '1995', '2005', '1999', '2003',
            '1996', '2014', '2022', '2016', '1998', '2011', '1992']
        for i in range(len(df['year'])):
            if df['year'].iloc[i] not in valid_year:
                df['year'].iloc[i] = None
        df.drop(df[df['year'].isna()].index, inplace=True)
        return df
