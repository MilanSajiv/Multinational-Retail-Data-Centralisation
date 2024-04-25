import boto3
import pandas as pd
import requests
import yaml
import tabula

class DataExtractor:
    def extract_from_s3(self, s3_address):
        try:
            # Splitting S3 address to bucket name and file name
            bucket_name, file_key = s3_address.split('s3://')[1].split('/')
            # Downloading file from S3 bucket
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            # Reading CSV data into DataFrame
            df = pd.read_csv(response['Body'])
            return df
        except Exception as e:
            print("Error:", e)
            return None

    def retrieve_pdf_data(self, link):
        try:
            dfs = tabula.read_pdf(link, pages='all', multiple_tables=True)
            df = pd.concat(dfs)
            return df
        except Exception as e:
            print("Error extracting data from PDF:", e)
            return None

    def list_number_of_stores(self, endpoint, headers):
        try:
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                return response.json()['number_of_stores']
            else:
                print("Failed to retrieve number of stores from API.")
                return None
        except Exception as e:
            print("Error:", e)
            return None

    def retrieve_stores_data(self, endpoint, headers):
        try:
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                data = response.json()['stores']
                df = pd.DataFrame(data)
                return df
            else:
                print("Failed to retrieve store data from API.")
                return None
        except Exception as e:
            print("Error:", e)
            return None
    