import pandas as pd
import psycopg2
# from sqlalchemy import create_engine, text

#db credential
# db_user = 'postgres'
# db_password = 'admin'
# db_host = 'localhost'
# db_port = '5432'
# db_name = 'user_behavior'
# table_name = 'usb1'

# csv_file_path = 'C:/Users/NDS/user_behavior_task/airflowtask2/airflow/sample_files/dataset_user_behavior_for_test.csv'
# data = pd.read_csv(csv_file_path)

def data_quality_checks(data):
    """
    Perform data quality checks and validation on the CSV data.
    Returns the cleaned DataFrame if all checks pass, else raises an error.
    """
    # Check 1: Ensure the data is not empty
    if data.empty:
        raise ValueError("DataFrame is empty. The CSV file may be missing data.")
    
    # Check 2: Check for duplicates and remove them
    initial_row_count = len(data)
    data.drop_duplicates(inplace=True)
    if len(data) < initial_row_count:
        print(f"Removed {initial_row_count - len(data)} duplicate rows.")
    
    # Check 3: Check for missing values and fill/drop as necessary
    missing_values = data.isnull().sum().sum()
    if missing_values > 0:
        print(f"Found {missing_values} missing values. Filling or dropping as per rules.")
        data.fillna(0, inplace=True)  # Example: Replace missing values with 0
    
    # Check 4: Validate data types (e.g., dates, numbers)
    if 'start watching' in data.columns:
        data['start watching'] = pd.to_datetime(data['start watching'], errors='coerce')
        invalid_dates = data['start watching'].isnull().sum()
        if invalid_dates > 0:
            print(f"Found {invalid_dates} invalid dates. Dropping these rows.")
            data = data.dropna(subset=['start watching'])
    
    # Check 5: Check for data ranges/values within expected thresholds
    if 'user_age' in data.columns:
        if not data['user_age'].between(0, 120).all():
            print("Found user ages outside of expected range. Adjusting data.")
            data['user_age'] = data['user_age'].clip(lower=0, upper=120)
    
    # All checks passed, return cleaned data
    return data

def etl(csv_file, db_params):
    """
    ETL function to load data from csv_file into PostgreSQL database at db_url.
    This function performs:
    1. Dropping the table if it exists.
    2. Creating a new table.
    3. Loading CSV data.
    4. Running data quality checks and validation.
    5. Using a CTE and ROW_NUMBER() to insert only unique rows into the final table.
    6. Returning the number of rows inserted.
    """
    # Step 1: Load CSV data
    data = pd.read_csv(csv_file)
    
    # Step 2: Data Quality Checks and Validation
    try:
        data = data_quality_checks(data)
        print("Data quality checks passed successfully.")
    except ValueError as e:
        print(e)
        return
    
    # Step 3: Connect to PostgreSQL database using psycopg2
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Step 4: Define SQL queries
        drop_table_sql = "DROP TABLE IF EXISTS usb1;"
        create_table_sql = """
        CREATE TABLE usb1 (
            id SERIAL PRIMARY KEY,
            user_id INT,
            session_id VARCHAR(50),
            event_type VARCHAR(50),
            event_time TIMESTAMP
        );
        """
        
        # Step 5: Drop table if exists and create a new one
        cur.execute(drop_table_sql)
        cur.execute(create_table_sql)
        conn.commit()
        
        # Step 6: Insert cleaned data into a temporary table in PostgreSQL
        temp_table = 'user_behavior_temp'
        
        # Create the temporary table (ensure it matches the columns in the DataFrame)
        create_temp_table_sql = """
        CREATE TEMPORARY TABLE user_behavior_temp (
            user_id INT,
            session_id VARCHAR(50),
            event_type VARCHAR(50),
            event_time TIMESTAMP
        );
        """
        cur.execute(create_temp_table_sql)
        
        # Insert the CSV data into the temp table
        for index, row in data.iterrows():
            cur.execute(
                "INSERT INTO user_behavior_temp (user_id, session_id, event_type, event_time) VALUES (%s, %s, %s, %s)",
                (row['user_id'], row['session_id'], row['event_type'], row['start watching'])
            )
        conn.commit()

        # Step 7: Use CTE and ROW_NUMBER() to insert only unique rows into the main table
        dedup_insert_sql = """
        WITH ranked_data AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY user_id, session_id, event_type ORDER BY event_time) AS row_num
            FROM user_behavior_temp
        )
        INSERT INTO usb1 (user_id, session_id, event_type, event_time)
        SELECT user_id, session_id, event_type, event_time
        FROM ranked_data
        WHERE row_num = 1;
        """
        
        # Execute deduplication insertion
        cur.execute(dedup_insert_sql)
        conn.commit()
        
        # Step 8: Data Quality Checks in PostgreSQL
        # Confirm number of rows inserted
        cur.execute("SELECT COUNT(*) FROM usb1")
        row_count = cur.fetchone()[0]
        
        # Check for NULLs in critical fields
        cur.execute("""
            SELECT COUNT(*) FROM usb1
            WHERE user_id IS NULL OR session_id IS NULL OR event_type IS NULL OR event_time IS NULL
        """)
        null_check = cur.fetchone()[0]
        
        if null_check > 0:
            raise ValueError("Data quality check failed in PostgreSQL: NULL values found in critical columns.")
        
        # Ensure all rows are unique after deduplication
        print(f"Number of unique records after deduplication: {row_count}")
        return row_count

    except Exception as e:
        print(f"Error during ETL process: {e}")
    finally:
        # Clean up
        if cur:
            cur.close()
        if conn:
            conn.close()

# Usage example
csv_file_path = 'C:/Users/NDS/user_behavior_task/airflowtask2/airflow/sample_files/dataset_user_behavior_for_test.csv'
db_params = {
    "host": "localhost",
    "dbname": "user_behavior",
    "user": "postgres",
    "password": "admin",
    "port": "5432"
}

# Run the ETL process
unique_rows = etl(csv_file_path, db_params)
print(f"Number of unique records inserted: {unique_rows}")