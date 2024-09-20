import json
import pandas as pd
import pymysql.cursors
import s3_file_operations as s3_ops

# RDS configurations
rds_host = "****************"  # RDS endpoint
rds_username = "*******"
rds_user_pwd = "*******"  # RDS password
rds_db_name = "Rick_And_Morty"
bucket_name = "de-masterclass-shisia"   # S3 bucket name


def lambda_handler(event, context):
    try:
        # Read transformed data from S3
        print("Reading transformed data from S3...")
        characters_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/Character.csv')
        episodes_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/Episode.csv')
        appearance_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/Appearance.csv')
        location_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/Location.csv')

        # Check if data is loaded successfully
        if characters_df is None or episodes_df is None or appearance_df is None or location_df is None:
            print("Error in loading data from S3")
            return {
                'statusCode': 500,
                'body': json.dumps('Error in loading data from S3')
            }
        print("Data loaded successfully from S3")

        # Establish RDS connection
        conn = pymysql.connect(host=rds_host,
                               user=rds_username,
                               password=rds_user_pwd,
                               port=3306,
                               database=rds_db_name,
                               cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()

        # Create tables
        create_tables(cursor)

        # Insert data into tables
        bulk_insert_data(cursor, conn, characters_df, "Character_Table")
        bulk_insert_data(cursor, conn, episodes_df, "Episode_Table")
        bulk_insert_data(cursor, conn, appearance_df, "Appearance_Table")
        bulk_insert_data(cursor, conn, location_df, "Location_Table")

        print("Data insertion completed successfully")
        return {
            'statusCode': 200,
            'body': json.dumps('Data transformation and upload successful')
        }

    except Exception as e:
        print("Exception: ", e)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

def create_tables(cursor):
    # SQL create table scripts
    create_character_table = """
        CREATE TABLE IF NOT EXISTS Character_Table (
            character_id INT NOT NULL PRIMARY KEY,
            name VARCHAR(255),
            status VARCHAR(255),
            species VARCHAR(255),
            type VARCHAR(255),
            gender VARCHAR(255),
            origin_id VARCHAR(255),
            location_id VARCHAR(255),
            image VARCHAR(255),
            url VARCHAR(255),
            created TIMESTAMP
        ) ENGINE=INNODB;
    """

    create_episode_table = """
        CREATE TABLE IF NOT EXISTS Episode_Table (
            episode_id INT NOT NULL PRIMARY KEY,
            name VARCHAR(255),
            air_date VARCHAR(255),
            episode VARCHAR(255),
            url VARCHAR(255),
            created TIMESTAMP
        ) ENGINE=INNODB;
    """

    create_appearance_table = """
        CREATE TABLE IF NOT EXISTS Appearance_Table (
            appearance_id INT NOT NULL PRIMARY KEY,
            episode_id INT,
            character_id INT
        ) ENGINE=INNODB;
    """

    create_location_table = """
        CREATE TABLE IF NOT EXISTS Location_Table (
            location_id INT NOT NULL PRIMARY KEY,
            name VARCHAR(255),
            type VARCHAR(255),
            dimension VARCHAR(255),
            url VARCHAR(255),
            created TIMESTAMP
        ) ENGINE=INNODB;
    """

    # Execute create table queries
    cursor.execute(create_character_table)
    cursor.execute(create_episode_table)
    cursor.execute(create_appearance_table)
    cursor.execute(create_location_table)

def bulk_insert_data(cursor, conn, df, table_name):
    column_names = list(df.columns)
    placeholders = ','.join(['%s'] * len(column_names))
    sql_insert = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({placeholders});"

    # Convert dataframe to list of tuples for batch insert
    data = [tuple(row[column] if pd.notna(row[column]) else None for column in column_names) for _, row in df.iterrows()]

    try:
        # Batch insert data using executemany for better performance
        cursor.executemany(sql_insert, data)
        conn.commit()
        print(f"Data inserted into {table_name}")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
        conn.rollback()
