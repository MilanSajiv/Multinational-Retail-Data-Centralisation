import pandas as pd
from data_extraction import DataExtractor
import numpy as np
import yaml

class DataCleaning:

    def clean_user_data(self): 
        df = DataExtractor().read_rds_table('legacy_users')
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format=True, errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True, errors='coerce')
        df.dropna(subset=['date_of_birth','join_date'], inplace=True)
        return df

    def clean_card_data(self):
        data_frame = DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        data_frame['date_payment_confirmed'] = pd.to_datetime(data_frame['date_payment_confirmed'], infer_datetime_format=True, errors='coerce')
        data_frame['card_number'] = data_frame['card_number'].astype(str).str.replace('\W', '', regex=True)
        data_frame['card_number'] = data_frame['card_number'].apply(lambda x: np.nan if x=='NULL' else x)
        data_frame.dropna(subset=['card_number', 'date_payment_confirmed'], inplace=True)
        card_data = pd.DataFrame(data_frame)
        return card_data

    def clean_store_data(self):
        store_data = DataExtractor().retrieve_stores_data()
        store_data.replace({'continent': ['eeEurope', 'eeAmerica']}, {'continent': ['Europe', 'America']}, inplace=True)
        store_data.drop(columns='lat', inplace=True)
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], infer_datetime_format=True, errors='coerce')
        store_data['store_type'] = store_data['store_type'].astype(str).apply(lambda x: np.nan if x=='NULL' else x)
        store_data.dropna(subset=['opening_date', 'store_type'], inplace=True)
        store_data['staff_numbers'] = store_data['staff_numbers'].str.replace(r'\D', '')
        return store_data

    def convert_product_weights(self, products_dataframe):
        products_dataframe['weight'] = products_dataframe['weight'].astype(str)
        products_dataframe.replace({'weight':['12 x 100g', '8 x 150g']}, {'weight':['1200g', '1200g']}, inplace=True)
        filter_letters = lambda x: ''.join(y for y in x if not y.isdigit())
        products_dataframe['units'] = products_dataframe['weight'].apply(filter_letters)
        products_dataframe['weight'] = products_dataframe['weight'].str.extract('([\d.]+)').astype(float)
        products_dataframe['weight'] = products_dataframe.apply(lambda x: x['weight']/1000 if x['units']=='g' or x['units']=='ml' else x['weight'], axis=1)
        products_dataframe.drop(columns='units', inplace=True)
        return products_dataframe

    def clean_products_data(self, products_dataframe):
        products_df = self.convert_product_weights(products_dataframe)
        products_df.dropna(subset=['uuid', 'product_code'], inplace=True)
        products_df['date_added'] = pd.to_datetime(products_df['date_added'], format='%Y-%m-%d', errors='coerce')
        drop_prod_list = ['S1YB74MLMJ', 'C3NCA2CL35', 'WVPMHZP59U']
        products_df.drop(products_df[products_df['category'].isin(drop_prod_list)].index, inplace=True)
        return products_df

    def clean_orders_data(self):
        orders_dataframe = DataExtractor().read_rds_table('orders_table')
        orders_dataframe.drop(columns=['1', 'first_name', 'last_name', 'level_0'], inplace=True)
        return orders_dataframe

    def clean_date_times(self):
        date_time_dataframe = DataExtractor().extract_from_s3_json()
        date_time_dataframe['day'] = pd.to_numeric(date_time_dataframe['day'], errors='coerce')
        date_time_dataframe.dropna(subset=['day', 'year', 'month'], inplace=True)
        date_time_dataframe['timestamp'] = pd.to_datetime(date_time_dataframe['timestamp'], format='%H:%M:%S', errors='coerce')
        return date_time_dataframe
