import pandas as pd
import numpy as np
import re
import psycopg2

# Define the path to the uploaded CSV file and database parameters
csv_file_path = 'C:/Users/NDS/user_behavior_task/airflowtask2/airflow/sample_files/dataset_user_behavior_for_test_3.csv'
db_params = {
    "host": "localhost",
    "dbname": "user_behavior_3",
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
data = pd.read_csv(csv_file_path, quotechar='"', escapechar='\\')
print(data.dtypes)

# Define a function to prepare and clean data
def prepare_data(data, column_mapping):
    """
    Renames columns, performs data quality checks, and transforms data.
    """
    # Rename columns based on the provided mapping
    data = data.rename(columns=column_mapping)
    print(data['start_watching'].head())
    print(data.isnull().sum())
    print(data[data.isnull().any(axis=1)])
    contains_quotes_event_type = data['user_id'].str.contains('"', na=False)
    print(data[contains_quotes_event_type])

    # Fill missing start_watching values based on column type
    # data = data.apply(lambda col: col.fillna('unknown') if col.dtype == 'object' else col)
    # data = data.apply(lambda col: col.fillna(0) if np.issubdtype(col.dtype, np.number) else col)

    # Ensure 'user_id' is a string for proper processing
    data['user_id'] = data['user_id'].astype(str)

    def split_row(row):
        try:
        # Regular expression to match fields
        # - Match any sequence of non-comma characters ([^,]+)
        # - Match quoted content (which may contain commas), like "LHEGANGSTER,THECOP,THEDEVI"
           pattern = r'"([^"]*)"|([^",]+)'

        # Find all matches using the regex pattern
           parts = []
           for match in re.finditer(pattern, row):
            # If we have a quoted field, use it
             if match.group(1):
                parts.append(f'"{match.group(1)}"')  # Content inside quotes
             elif match.group(2):
                parts.append(match.group(2))  # Non-quoted content
        
        # Ensure we have the expected number of parts (8 expected, based on the example you gave)
           if len(parts) < 9:
             raise ValueError(f"Unexpected row format: {row}")

        # Assign values based on the expected positions of the split data
           return {
            'user_id': parts[0],  # First part is user_id
            'start_watching': parts[1],  # Second part is start_watching
            'session_id': parts[2],  # Third part is session_id
            'province': parts[3],  # Fourth part is province
            'city': parts[4],  # Fifth part is city
            'event_type': parts[5],  # Sixth part is event_type
            'content_name': parts[6],  # Seventh part is content_name (may contain commas)
            'content_type': parts[7],  # Eighth part is content_type
            'device_type': parts[8],  # Ninth part is device_type
           }
        except Exception as e:
           print(f"Error processing row: {row}")
           print(f"Error message: {str(e)}")
           return None  # Return None or some other fallback in case of error

# Step 1: Rename the columns to avoid overlap
    data = data.rename(columns={
    'user_id': 'user_id_old',
    'start_watching': 'start_watching_old',
    'session_id': 'session_id_old',
    'province': 'province_old',
    'city': 'city_old',
    'event_type': 'event_type_old',
    'device_type': 'device_type_old',
    'content_type': 'content_type_old',
    })

# Step 2: Apply the split_row function to 'user_id_old' (or whatever column contains raw data)
    data['split_data'] = data['user_id_old'].apply(split_row)

# Step 3: Expand the resulting dictionary into separate columns
    split_columns = data['split_data'].apply(pd.Series)

# Step 4: Add suffix to the new columns to avoid conflict with old column names
    split_columns = split_columns.add_suffix('_new')

# Step 5: Join the new columns with the original dataframe
    data = data.join(split_columns)

# Optionally, drop the 'split_data' column if it's no longer needed
    data.drop('split_data', axis=1, inplace=True)

# Step 6: Optionally, you can rename back the '_old' columns to the original column names if necessary
    data = data.rename(columns={
    'user_id_old': 'user_id',
    'start_watching_old': 'start_watching',
    'session_id_old': 'session_id',
    'province_old': 'province',
    'city_old': 'city',
    'event_type_old': 'event_type',
    'device_type_old': 'device_type',
    'content_type_old': 'content_type',
    })
    print(data)


    data['user_id'] = pd.to_numeric(data['user_id'], errors='coerce').fillna(0).astype(int)
    data['play_time_ms'] = pd.to_numeric(data['play_time_ms'], errors='coerce').fillna(0).astype(int)

    data = data.apply(
        lambda col: pd.to_datetime(col, format="%m/%d/%Y %H:%M", errors='coerce').fillna(pd.Timestamp('1970-01-01 00:00:00')) 
        if col.name == 'start_watching' else col
    )

    # Validate and format datetime in 'start_watching' column
    # if 'start_watching' in data.columns:
       # data['start_watching'] = pd.to_datetime(data['start_watching'], errors='coerce')
         
       # data['start_watching'].fillna(pd.Timestamp('1970-01-01 00:00:00'), inplace=True)
        # if data['start_watching'].isnull().any():
            # print("Warning: Invalid date formats detected in 'start_watching' column. Proceeding with NaT values.")
            # data.apply(lambda col: col.fillna(pd.Timestamp('1970-01-01')) if np.issubdtype(col.dtype, np.datetime64) else col)
    
    # Convert 'user_id' to numeric, setting errors='coerce' to convert non-numeric values to NaN
    # if 'user_id' in data.columns:
    #     data['user_id'] = pd.to_numeric(data['user_id'], errors='coerce').fillna(0).astype(int)

    # Convert 'play_time_ms' to numeric, setting errors='coerce' to handle non-numeric values
    # if 'play_time_ms' in data.columns:
    #     data['play_time_ms'] = pd.to_numeric(data['play_time_ms'], errors='coerce').fillna(0).astype(int)

    # Validate and format datetime in 'start_watching' column, filling missing values if needed
    # if 'start_watching' in data.columns:
    # data['start_watching'] = pd.to_datetime(data['start_watching'], errors='coerce')
    # data['start_watching'].fillna(pd.Timestamp('1970-01-01 00:00:00'), inplace=True)

        # Menggunakan format khusus saat mengonversi menjadi string
        # data['start_watching'] = data['start_watching'].dt.strftime('%m/%d/%Y %H:%M')
        # data['start_watching'] = data['start_watching'].astype('object')
    
        # data['start_watching'] = pd.to_datetime(data['start_watching'], errors='coerce')
        # Fill missing 'start_watching' values with a placeholder timestamp
        # data['start_watching'].fillna(pd.Timestamp('1970-01-01 00:00:00'), inplace=True)
        # data['start_watching'].fillna('1970-01-01')

    # Combine 'province' and 'city' columns into 'location', capitalizing each word
    if 'province' in data.columns and 'city' in data.columns:
        data['location'] = data['province'].str.title() + ", " + data['city'].str.title()
        data['province'] = data['province'].str.title()
        data['city'] = data['city'].str.title()

    # Ensure 'user_id' and 'play_time_ms' values are non-negative
    # if 'user_id' in data.columns:
    #     data['user_id'] = pd.to_numeric(data['user_id'], errors='coerce').fillna(0).astype(int)
    #     data['user_id'] = data['user_id'].clip(lower=0)
    # if 'play_time_ms' in data.columns:
    #     data['play_time_ms'] = pd.to_numeric(data['play_time_ms'], errors='coerce').fillna(0).astype(int)
    #     data['play_time_ms'] = data['play_time_ms'].clip(lower=0)

    # Replace remaining NaN values with None for database compatibility
    # return data.where(pd.notnull(data), None)
    return data

# Apply data preparation function
cleaned_data = prepare_data(data, column_mapping)
print(f"Number of rows in cleaned data: {cleaned_data.shape[0]}")
print(cleaned_data[cleaned_data.isna().any(axis=1)])
print(cleaned_data.iloc[3821])

# Define ETL function to load cleaned data into PostgreSQL
def etl(data, db_params):
    """
    ETL function to load cleaned data into PostgreSQL database.
    Creates additional summary tables for users by province and content type.
    """
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Drop main table if exists and create new one with province and city columns
        drop_main_table_sql = "DROP TABLE IF EXISTS usb3;"
        create_main_table_sql = """
        CREATE TABLE usb3 (
            id SERIAL PRIMARY KEY,
            user_id INT,
            session_id VARCHAR(255),
            event_type VARCHAR(255),
            event_time TIMESTAMP,
            content_type VARCHAR(255),
            device_type VARCHAR(255),
            province VARCHAR(255),
            city VARCHAR(255),
            location VARCHAR(255),
            play_time_ms INT
        );
        """
        
        cur.execute(drop_main_table_sql)
        cur.execute(create_main_table_sql)
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
            province VARCHAR(255),
            city VARCHAR(255),
            location VARCHAR(255),
            play_time_ms INT
        );
        """
        cur.execute(create_temp_table_sql)
        
        # Populate temporary table with the cleaned data
        for _, row in data.iterrows():
            cur.execute(
                """
                INSERT INTO user_behavior_temp (user_id, session_id, event_type, event_time, content_type, device_type, province, city, location, play_time_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['user_id'], row['session_id'], row['event_type'], row['start_watching'], row['content_type'], row['device_type'], row['province'], row['city'], row['location'], row['play_time_ms'])
            )
        conn.commit()

        # Insert only unique rows into the main table using CTE and ROW_NUMBER
        dedup_insert_sql = """
        WITH ranked_data AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, session_id, event_type ORDER BY event_time DESC) AS row_num
            FROM user_behavior_temp
        )
        INSERT INTO usb3 (user_id, session_id, event_type, event_time, content_type, device_type, province, city, location, play_time_ms)
        SELECT user_id, session_id, event_type, event_time, content_type, device_type, province, city, location, play_time_ms
        FROM ranked_data
        WHERE row_num >= 1;
        """
        
        cur.execute(dedup_insert_sql)
        conn.commit()
        
        # Confirm number of rows inserted
        cur.execute("SELECT COUNT(*) FROM usb3")
        row_count = cur.fetchone()[0]
        
        print(f"Number of unique records inserted: {row_count}")
        
        # Create summary tables
        # Table for the count of users from each province
        drop_province_count_table = "DROP TABLE IF EXISTS users_by_province;"
        create_province_count_table = """
        CREATE TABLE users_by_province AS
        SELECT province, COUNT(DISTINCT user_id) AS user_count
        FROM usb3
        GROUP BY province;
        """
        cur.execute(drop_province_count_table)
        cur.execute(create_province_count_table)
        conn.commit()
        print("Created table 'users_by_province'.")

        # Table for the count of users by content type
        drop_content_type_count_table = "DROP TABLE IF EXISTS users_by_content_type;"
        create_content_type_count_table = """
        CREATE TABLE users_by_content_type AS
        SELECT content_type, COUNT(DISTINCT user_id) AS user_count
        FROM usb3
        GROUP BY content_type;
        """
        cur.execute(drop_content_type_count_table)
        cur.execute(create_content_type_count_table)
        conn.commit()
        print("Created table 'users_by_content_type'.")

        # Retrieve results for display
        print("\nResults:")
        cur.execute("SELECT * FROM users_by_province")
        province_results = cur.fetchall()
        print("\nUsers by Province:")
        for row in province_results:
            print(row)

        cur.execute("SELECT * FROM users_by_content_type")
        content_type_results = cur.fetchall()
        print("\nUsers by Content Type:")
        for row in content_type_results:
            print(row)

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
