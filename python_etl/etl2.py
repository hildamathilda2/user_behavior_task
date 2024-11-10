import pandas as pd
import numpy as np
import psycopg2

# Define the paths and database parameters
csv_file_path = 'C:/Users/NDS/user_behavior_task/airflowtask2/airflow/sample_files/dataset_user_behavior_for_test.csv'
db_params = {
    "host": "localhost",
    "dbname": "user_behavior",
    "user": "postgres",
    "password": "admin",
    "port": "5432"
}

# Define initial column mapping
column_mapping = {
    'Iduser': 'user_id',
    'start watching': 'start_watching',
    'Device Id': 'session_id',
    'Content Name': 'event_type',
    'Province': 'province',
    'City': 'city',
    'Content Type': 'content_type',
    'Playing Time Millisecond': 'play_time_ms',
    'Device Type': 'device_type'
}

# Load the CSV file
data = pd.read_csv(csv_file_path)

# Define a function to prepare and clean data
def process_data(data, column_mapping):
    """
    Renames columns, performs data quality checks, and transforms data.
    
    Parameters:
    - data: DataFrame to process.
    - column_mapping: Dictionary mapping original column names to desired column names.
    
    Returns:
    - Processed DataFrame with necessary transformations applied.
    """
    # Rename columns based on the provided mapping
    data = data.rename(columns=column_mapping)

    # Fill missing values based on column type
    data = data.apply(lambda col: col.fillna('unknown') if col.dtype == 'object' else col)
    data = data.apply(lambda col: col.fillna(0) if np.issubdtype(col.dtype, np.number) else col)
    data = data.apply(lambda col: col.fillna(pd.Timestamp('1970-01-01')) if np.issubdtype(col.dtype, np.datetime64) else col)

    # Validate and format datetime in 'start_watching' column
    if 'start_watching' in data.columns:
        data['start_watching'] = pd.to_datetime(data['start_watching'], errors='coerce')
        if data['start_watching'].isnull().any():
            print("Warning: Invalid date formats detected in 'start_watching' column. Proceeding with NaT values.")

    # Combine 'province' and 'city' columns into 'location', capitalizing each word
    if 'province' in data.columns and 'city' in data.columns:
        data['location'] = data['province'].str.title() + ", " + data['city'].str.title()

    # Ensure 'user_id' and 'play_time_ms' values are non-negative
    if 'user_id' in data.columns:
        data['user_id'] = data['user_id'].clip(lower=0)
    if 'play_time_ms' in data.columns:
        data['play_time_ms'] = data['play_time_ms'].clip(lower=0)

    # Replace remaining NaN values with None for database compatibility
    return data.where(pd.notnull(data), None)

# Apply data preparation function
cleaned_data = process_data(data, column_mapping)

# Define ETL function to load cleaned data into PostgreSQL
def etl(data, db_params):
    """
    ETL function to load cleaned data into PostgreSQL database.
    """
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Drop table if exists and create new one
        drop_table_sql = "DROP TABLE IF EXISTS usb1;"
        create_table_sql = """
        CREATE TABLE usb1 (
            id SERIAL PRIMARY KEY,
            user_id INT,
            session_id VARCHAR(255),
            event_type VARCHAR(255),
            event_time TIMESTAMP,
            content_type VARCHAR(255),
            device_type VARCHAR(255),
            location VARCHAR(255),
            play_time_ms INT
        );
        """
        
        cur.execute(drop_table_sql)
        cur.execute(create_table_sql)
        conn.commit()
        
        # Insert data into temporary table
        create_temp_table_sql = """
        CREATE TEMPORARY TABLE user_behavior_temp (
            user_id INT,
            session_id VARCHAR(255),
            event_type VARCHAR(255),
            event_time TIMESTAMP,
            content_type VARCHAR(255),
            device_type VARCHAR(255),
            location VARCHAR(255),
            play_time_ms INT
        );
        """
        cur.execute(create_temp_table_sql)
        
        for index, row in data.iterrows():
            cur.execute(
                "INSERT INTO user_behavior_temp (user_id, session_id, event_type, event_time, content_type, device_type, location, play_time_ms) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (row['user_id'], row['session_id'], row['event_type'], row['start_watching'], row['content_type'], row['device_type'], row['location'], row['play_time_ms'])
            )
        conn.commit()

        # Insert only unique rows into the main table using CTE and ROW_NUMBER
        dedup_insert_sql = """
        WITH ranked_data AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, session_id, event_type ORDER BY event_time DESC) AS row_num
            FROM user_behavior_temp
        )
        INSERT INTO usb1 (user_id, session_id, event_type, event_time, content_type, device_type, location, play_time_ms)
        SELECT user_id, session_id, event_type, event_time, content_type, device_type, location, play_time_ms
        FROM ranked_data
        WHERE row_num = 1;
        """
        
        cur.execute(dedup_insert_sql)
        conn.commit()
        
        # Confirm number of rows inserted
        cur.execute("SELECT COUNT(*) FROM usb1")
        row_count = cur.fetchone()[0]
        
        print(f"Number of unique records inserted: {row_count}")
        return row_count

    except Exception as e:
        print(f"Error during ETL process: {e}")
    finally:
        # Clean up
        if cur:
            cur.close()
        if conn:
            conn.close()

# Run the ETL process
unique_rows = etl(cleaned_data, db_params)
print(f"Number of unique records inserted: {unique_rows}")
