import json
import pandas as pd
import boto3
import ast
from io import StringIO
import s3_file_operations as s3_ops

def lambda_handler(event, context):
    bucket = "de-masterclass-shisia"  # S3 bucket name

    # Read data from S3
    print("Reading Character data from S3...")
    characters_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/Character.csv')
    print(f"Characters DataFrame shape: {characters_df.shape}")
    print("Reading Episode data from S3...")
    episodes_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/Episode.csv')
    print(f"Episodes DataFrame shape: {episodes_df.shape}")
    print("Reading Location data from S3...")
    location_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/Location.csv')
    print(f"Locations DataFrame shape: {location_df.shape}")

    # Check if data is loaded successfully
    if characters_df is None or episodes_df is None or location_df is None:
        print("Error in loading data from S3")
        return {
            'statusCode': 500,
            'body': json.dumps('Error in loading data from S3')
        }

    print("Data loaded successfully from S3")

    # Characters DataFrame transformation
    extract_id = lambda x: x.split('/')[-1] if x else None

    characters_df['origin_id'] = [
        extract_id(ast.literal_eval(record)['url']) if isinstance(record, str) else None
        for record in characters_df['origin']
    ]

    characters_df['location_id'] = [
        extract_id(ast.literal_eval(record)['url']) if isinstance(record, str) else None
        for record in characters_df['location']
    ]

    # Rename the primary key in characters_df to 'character_id'
    characters_df = characters_df.rename(columns={'id': 'character_id'})

    # Drop unnecessary columns
    characters_df = characters_df.drop(columns=['origin', 'location', 'episode'])

    # Replace NaN values with None (interpreted as NULL in SQL)
    characters_df = characters_df.where(pd.notnull(characters_df), None)
    location_df = location_df.where(pd.notnull(location_df), None)

    print("Checking for NaN in Characters DataFrame...")
    print(characters_df.isna().sum())
    
    print("Checking for NaN in Locations DataFrame...")
    print(location_df.isna().sum())

    # Appearance DataFrame transformation
    appearance_df = episodes_df.copy()

    # Function to extract the ID from a URL
    character_func = lambda x: [url.split('/')[-1] for url in ast.literal_eval(x)] if isinstance(x, str) else None
    
    # Using list comprehension to extract character_ids
    appearance_df['character_ids'] = [
        character_func(record) if record else None
        for record in appearance_df['characters']
    ]
    
    # Explode the 'character_ids' column to create a row for each character ID
    expanded_df = appearance_df.explode('character_ids').reset_index(drop=True).reset_index()
    
    # Rename columns to match the desired output
    expanded_df = expanded_df.rename(columns={'index': 'appearance_id', 'id': 'episode_id', 'character_ids': 'character_id'})
    
    # Select only the relevant columns
    expanded_df = expanded_df[['appearance_id', 'episode_id', 'character_id']]

    # Replace NaN values with None in the expanded DataFrame
    expanded_df = expanded_df.where(pd.notnull(expanded_df), None)

    # Episodes DataFrame transformation
    episodes_df = episodes_df.drop("characters", axis=1)

    # Rename the primary key in episodes_df to 'episode_id'
    episodes_df = episodes_df.rename(columns={'id': 'episode_id'})

    # Locations DataFrame transformation
    location_df = location_df.drop('residents', axis=1)

    # Rename the primary key in location_df to 'location_id'
    location_df = location_df.rename(columns={'id': 'location_id'})

    # Save final DataFrames to S3
    s3_ops.write_data_to_s3(characters_df, bucket, 'Rick&Morty/Transformed/Character.csv')
    s3_ops.write_data_to_s3(episodes_df, bucket, 'Rick&Morty/Transformed/Episode.csv')
    s3_ops.write_data_to_s3(expanded_df, bucket, 'Rick&Morty/Transformed/Appearance.csv')
    s3_ops.write_data_to_s3(location_df, bucket, 'Rick&Morty/Transformed/Location.csv')

    return {
        'statusCode': 200,
        'body': json.dumps('Data transformation and upload successful')
    }
