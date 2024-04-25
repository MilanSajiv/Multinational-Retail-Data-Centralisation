import yaml
from sqlalchemy import create_engine
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

# Instantiate DatabaseConnector and DataCleaning classes
connector = DatabaseConnector()
cleaner = DataCleaning()

# Retrieve database credentials
db_creds = connector.read_db_creds(r'C:\Milan\Career\Self-Study\Ai Core\Multinational Retail Data Centralisation\db_creds.yaml')  # Assuming you have a YAML file with credentials

# Initialize database engine
engine = connector.init_db_engine(db_creds)

# Step 1: List all tables in the database
tables = connector.list_db_tables(engine)

# Step 2: Extract orders data
orders_table_name = 'orders_table'  # Assuming you know the name of the table containing orders
orders_data = connector.read_rds_table(engine, orders_table_name)

# Step 3: Clean orders data
cleaned_orders_data = cleaner.clean_orders_data(orders_data)

# Step 4: Upload cleaned orders data to the database
connector.upload_to_db(cleaned_orders_data, orders_table_name, engine)