#  Insert data into table that has foreign key

import pandas as pd
import mysql.connector
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor

def create_db_connection(host_name, user_name, user_password, db_name):
    """
    Establishes a database connection.

    Args:
    host_name (str): Host name of the database server.
    user_name (str): Username for the database.
    user_password (str): Password for the database.
    db_name (str): Name of the database.

    Returns:
    MySQLConnection: A connection object if successful, None otherwise.
    """
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

def check_foreign_keys_exist(connection, movie_id, person_id):
    """
    Checks if the foreign key constraints are satisfied for given IDs.

    Args:
    connection (MySQLConnection): The database connection.
    Below is an example of Movie and person tables
    movie_id (int/str): The movie ID to check in the Movies table.
    person_id (int/str): The person ID to check in the Person table.

    Returns:
    bool: True if both foreign keys exist, False otherwise.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM Movies WHERE movie_id = %s)", (movie_id,))
    movie_exists = cursor.fetchone()[0]

    cursor.execute("SELECT EXISTS(SELECT 1 FROM Person WHERE person_id = %s)", (person_id,))
    person_exists = cursor.fetchone()[0]

    cursor.close()
    return movie_exists and person_exists

def execute_batch_insert_with_offset(host, username, password, database, query, data, offset, batch_size):
    """
    Executes a batch insert operation for a specified range of data.

    Args:
    host (str): Database server host.
    username (str): Database username.
    password (str): Database password.
    database (str): Database name.
    query (str): SQL query for insertion.
    data (list of tuples): Data to be inserted.
    offset (int): Starting index for the batch.
    batch_size (int): Number of records in each batch.
    """
    connection = create_db_connection(host, username, password, database)
    if connection:
        cursor = connection.cursor()
        try:
            batch_data = data[offset:offset + batch_size]
            for movie_id, person_id in batch_data:
                if check_foreign_keys_exist(connection, movie_id, person_id):
                    cursor.execute(query, (movie_id, person_id))
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
file_path = 'Profession.csv' # Replace with the corresponding CSV file path

# Read data from CSV file
data_df = pd.read_csv(file_path)

# Convert DataFrame to a list of tuples for insertion
data_tuples = list(data_df.itertuples(index=False, name=None))

# SQL Insert Query
insert_query = "INSERT INTO Profession (personid, professionid) VALUES (%s, %s);"

# Parameters for Batch Insert
batch_size = 1000  # Define the number of records per batch
total_rows = len(data_tuples)
offsets = range(0, total_rows, batch_size)

# Execute batch inserts in parallel
with ThreadPoolExecutor() as executor:
    for offset in offsets:
        executor.submit(execute_batch_insert_with_offset, host, username, password, database, insert_query, data_tuples, offset, batch_size)