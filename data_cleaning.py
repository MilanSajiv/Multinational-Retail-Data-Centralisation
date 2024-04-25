import tabula
import pandas as pd

class DataCleaning:
    def clean_card_data(self, df):
        # Method to clean card data
        # You can implement your cleaning logic here
        cleaned_df = df  # Placeholder for actual cleaning logic
        return cleaned_df

    def clean_user_data(self, df):
        # Method to clean user data
        # You can implement your cleaning logic here
        cleaned_df = df  # Placeholder for actual cleaning logic
        return cleaned_df

    def convert_product_weights(self, df):
        # Method to convert product weights to kg
        df['weight'] = df['weight'].str.replace(r'[^\d\.\,]', '', regex=True)  # Remove non-numeric characters
        df['weight'] = df['weight'].str.replace(',', '.')  # Replace comma with dot for decimal
        df['weight'] = df['weight'].astype(float)  # Convert to float
        # Convert ml to kg using a rough estimate
        df.loc[df['weight_unit'] == 'ml', 'weight'] *= 0.001
        df.drop(columns=['weight_unit'], inplace=True)  # Drop weight_unit column
        return df

    def clean_products_data(self, df):
        # Method to clean product data
        # You can implement your cleaning logic here
        cleaned_df = df  # Placeholder for actual cleaning logic
        return cleaned_df
    
    def clean_orders_data(self, df):
        # Method to clean orders data
        cleaned_df = df.drop(columns=['first_name', 'last_name', '1'])  # Remove specified columns
        return cleaned_df

