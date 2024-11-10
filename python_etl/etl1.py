import pandas as pd
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

# Define initial column names in the CSV file
original_columns = {
    'Iduser': 'user_id',          # User ID
    'start watching': 'event_time',  # Event timestamp
    'Device Id': 'session_id',     # Device/session ID
    'Content Name': 'event_type'   # Event type/content name
}

# Load the CSV file and rename columns
data = pd.read_csv(csv_file_path)
data.rename(columns=original_columns, inplace=True)

# Define data quality checks function
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
    if 'event_time' in data.columns:
        data['event_time'] = pd.to_datetime(data['event_time'], errors='coerce')
        invalid_dates = data['event_time'].isnull().sum()
        if invalid_dates > 0:
            print(f"Found {invalid_dates} invalid dates. Dropping these rows.")
            data = data.dropna(subset=['event_time'])
    
    # Check 5: Check for data ranges/values within expected thresholds
    if 'user_id' in data.columns:
        if not data['user_id'].between(0, 1200000000).all():  # Assuming user_id range for simplicity
            print("Found user IDs outside of expected range. Adjusting data.")
            data['user_id'] = data['user_id'].clip(lower=0, upper=1200000000)
    
    # All checks passed, return cleaned data
    return data

# Define ETL function to load cleaned data into PostgreSQL
def etl(data, db_params):
    """
    ETL function to load cleaned data into PostgreSQL database.
    """
    try:
        # Step 3: Connect to PostgreSQL database using psycopg2
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Step 4: Define SQL queries
        drop_table_sql = "DROP TABLE IF EXISTS usb1;"
        create_table_sql = """
        CREATE TABLE usb1 (
            id SERIAL PRIMARY KEY,
            user_id INT,
            session_id VARCHAR(255),
            event_type VARCHAR(255),
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
            session_id VARCHAR(255),
            event_type VARCHAR(255),
            event_time TIMESTAMP
        );
        """
        cur.execute(create_temp_table_sql)
        
        # Insert the CSV data into the temp table
        for index, row in data.iterrows():
            cur.execute(
                "INSERT INTO user_behavior_temp (user_id, session_id, event_type, event_time) VALUES (%s, %s, %s, %s)",
                (row['user_id'], row['session_id'], row['event_type'], row['event_time'])
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

# Apply data quality checks to the cleaned data
cleaned_data = data_quality_checks(data)

# Run the ETL process
unique_rows = etl(cleaned_data, db_params)
print(f"Number of unique records inserted: {unique_rows}")
