# Insert data into table that doesn't have foreign key

import pandas as pd
import mysql.connector
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor

def create_db_connection(host_name, user_name, user_password, db_name):

    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        return connection
    except Error as err:
        print(f"Error: '{err}'")
        return None

def execute_batch_insert_with_offset(host, username, password, database, query, data, offset, batch_size):
    """
    """
    connection = create_db_connection(host, username, password, database)
    if connection:
        cursor = connection.cursor()
        try:
            # Calculate the actual batch based on offset and batch_size
            batch_data = data[offset:offset + batch_size]
            cursor.executemany(query, batch_data)
            connection.commit()
            print(f"Batch insert successful for offset {offset}")
        except Error as err:
            print(f"Error: '{err}'")
        finally:
            cursor.close()
            connection.close()

# Database Configuration
host = 'your_database_host'  # Replace with your database host
username = 'your_username'   # Replace with your database username
password = 'your_password'   # Replace with your database password
database = 'your_database'   # Replace with your database name

# File Path Configuration
file_path = 'Profession.csv' # Adjust it accordingly

# Read data from CSV file
data_df = pd.read_csv(file_path)

# Convert DataFrame to a list of tuples for insertion
data_tuples = list(data_df.itertuples(index=False, name=None))

# SQL Insert Query
insert_query = "INSERT INTO Profession (professionId, profession) VALUES (%s, %s);"

# Batch Insert Parameters
batch_size = 10  # Number of records to insert per batch
total_rows = len(data_tuples)
offsets = range(0, total_rows, batch_size)

# Execute batch inserts in parallel
with ThreadPoolExecutor() as executor:
    for offset in offsets:
        executor.submit(execute_batch_insert_with_offset, host, username, password, database, insert_query, data_tuples,offsets, batch_size)