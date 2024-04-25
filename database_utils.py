import yaml
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import inspect

class DatabaseConnector:
    def read_db_creds(self, file_path):
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def init_db_engine(self, creds):
        connection_string = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(connection_string)
        return engine
    
    def upload_to_db(self, df, table_name, engine):
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)

    def read_from_db(self, engine, query):
        with engine.connect() as connection:
            result = connection.execute(query)
            data = result.fetchall()
        return data
    
    def read_db_creds(self, file_path):
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def init_db_engine(self, creds):
        connection_string = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(connection_string)
        return engine
    
    def upload_to_db(self, df, table_name, engine):
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)

    def read_from_db(self, engine, query):
        with engine.connect() as connection:
            result = connection.execute(query)
            data = result.fetchall()
        return data

    def list_db_tables(self, engine):
        # Method to list all tables in the database
        tables = engine.table_names()
        return tables

    def read_rds_table(self, engine, table_name):
        # Method to read data from a specific table in the database
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)
        return df
    
    def list_db_tables(self, engine):
        # Method to list all tables in the database
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables


