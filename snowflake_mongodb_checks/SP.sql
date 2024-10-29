-- Procedure to check the differences between MongoDB and Snowflake

CREATE OR REPLACE PROCEDURE XXX.XXX.SNOWFLAKE_MONGO_CHECKS(
    MONGO_DATABASE STRING,
    MONGO_COLLECTION STRING,
    SNOWFLAKE_TABLE STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','dnspython','pandas')
IMPORTS = ('@packages/pymongo.zip')
EXTERNAL_ACCESS_INTEGRATIONS = (mongodb_access_integration)
HANDLER = 'retrieve_snowflake_mongo_checks'
AS
$$
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as F
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from bson import ObjectId

# Utility functions for value comparison
def are_values_equivalent_vectorized(series1, series2):
    str_series1 = series1.astype(str).str.lower().str.strip()
    str_series2 = series2.astype(str).str.lower().str.strip()
    null_like_mask = str_series1.isin(['nan', 'none', '', 'null']) & str_series2.isin(['nan', 'none', '', 'null'])
    bool_like_mask = str_series1.isin(['true', 'false']) & str_series2.isin(['true', 'false'])
    return null_like_mask | (bool_like_mask & (str_series1 == str_series2)) | (str_series1 == str_series2)

def retrieve_snowflake_mongo_checks(session: snowpark.Session, MONGO_DATABASE: str, MONGO_COLLECTION: str, SNOWFLAKE_TABLE: str) -> str:
    # Get the column mappings from the config table
    mappings_df = session.table("XXX.XXX.COLLECTION_COLUMN_MAPPINGS") \
                        .filter(F.col("COLLECTION_NAME") == MONGO_COLLECTION) \
                        .select("MONGO_COLUMN", "SNOWFLAKE_COLUMN") \
                        .collect()
    
    # Convert to dictionary
    COLUMN_MAPPING = {row['MONGO_COLUMN']: row['SNOWFLAKE_COLUMN'] for row in mappings_df}
    
    if not COLUMN_MAPPING:
        return f"Error: No column mappings found for collection {MONGO_COLLECTION}"
    
    # Query the CREDENTIALS table for MongoDB credentials
    credentials_df = session.table("XXX.XXX.CREDENTIALS").filter(F.col("KEY").isin([
        "MONGO_USER", 
        "MONGO_PASSWORD", 
        "MONGO_CLUSTER",
        "MONGO_SHARD_00",
        "MONGO_SHARD_01",
        "MONGO_SHARD_02",
        "MONGO_SHARD_03",
        "MONGO_SHARD_04"
    ]))
    credentials = {row['KEY']: row['VALUE'] for row in credentials_df.collect()}
    
    # Basic required credentials (always needed)
    base_required_keys = ['MONGO_USER', 'MONGO_PASSWORD', 'MONGO_CLUSTER']
    if not all(key in credentials for key in base_required_keys):
        return f"Error: Missing one or more basic MongoDB credentials: {', '.join(base_required_keys)}"

    # Find available shards
    shard_keys = [key for key in credentials.keys() if key.startswith('MONGO_SHARD_')]
    if not shard_keys:
        return "Error: No MongoDB shards found in credentials"

    # MongoDB connection string
    mongo_user = credentials['MONGO_USER']
    mongo_password = credentials['MONGO_PASSWORD']
    mongo_cluster = credentials['MONGO_CLUSTER']
    mongo_shards = ",".join(credentials[shard] for shard in sorted(shard_keys))
    
    mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_shards}/?ssl=true&retryWrites=true&w=majority&socketTimeoutMS=60000&connectTimeoutMS=60000"
    
    # Connect to MongoDB
    try:
        client = MongoClient(mongo_uri)
        db = client[MONGO_DATABASE]  # Use the passed database name
        collection = db[MONGO_COLLECTION]  # Use the passed collection name
        
        # Retrieve documents from the 'vouchers' collection
        mongo_columns = ['_id'] + list(COLUMN_MAPPING.keys())
        data = collection.find({}, {col: 1 for col in mongo_columns})
        
        # Convert to Pandas DataFrame
        df_mongo = pd.DataFrame(data)
        df_mongo['OBJECT_ID'] = df_mongo['_id'].apply(lambda x: str(x) if isinstance(x, ObjectId) else x)
        
        # Rename and filter columns based on the COLUMN_MAPPING
        df_mongo = df_mongo.rename(columns=COLUMN_MAPPING)
        columns_to_keep = ['OBJECT_ID'] + list(COLUMN_MAPPING.values())
        df_mongo = df_mongo[columns_to_keep]

        # Retrieve data from Snowflake using the passed table name
        df_snowflake = session.table(SNOWFLAKE_TABLE).select(*columns_to_keep).to_pandas()

        # Ensure both DataFrames have the same data types
        for col in columns_to_keep:
            df_mongo[col] = df_mongo[col].astype(str)
            df_snowflake[col] = df_snowflake[col].astype(str)

        # Compare the DataFrames and find differences
        df_merged = pd.merge(df_mongo, df_snowflake, on='OBJECT_ID', suffixes=('_mongo', '_snowflake'))

        differences = []
        for col in columns_to_keep[1:]:  # Skip 'OBJECT_ID'
            mask = ~are_values_equivalent_vectorized(df_merged[f'{col}_mongo'], df_merged[f'{col}_snowflake'])
            if mask.any():
                diff_df = df_merged[mask][['OBJECT_ID', f'{col}_mongo', f'{col}_snowflake']]
                diff_df.columns = ['OBJECT_ID', 'Value_Mongo', 'Value_Snowflake']
                diff_df['Column_Difference'] = col
                differences.append(diff_df)

        # Concatenate the differences
        if differences:
            df_differences = pd.concat(differences, ignore_index=True).drop_duplicates()
        else:
            df_differences = pd.DataFrame(columns=['OBJECT_ID', 'Value_Mongo', 'Value_Snowflake', 'Column_Difference'])

        # Log and return results
        if df_differences.empty:
            return f"Great news! No differences found in the data between MongoDB ({MONGO_COLLECTION}) and Snowflake ({SNOWFLAKE_TABLE})."
        else:
            # Capture the current timestamp
            current_timestamp = datetime.now()
            df_differences['TIMESTAMP_CHECKED'] = pd.Timestamp.now()
            df_differences['SOURCE'] = MONGO_COLLECTION
            insert_query = """
            INSERT INTO XXX.XXX.SNOWFLAKE_MONGO_DIFFERENCES
            (OBJECT_ID, Value_Mongo, Value_Snowflake, Column_Difference, TIMESTAMP_CHECKED, SOURCE)
            VALUES (?, ?, ?, ?, ?, ?)
            """
            
            # Insert the data using parameterized queries
            for row in df_differences.itertuples(index=False):
                session.sql(insert_query, [row.OBJECT_ID, row.Value_Mongo, row.Value_Snowflake, row.Column_Difference, current_timestamp, row.SOURCE]).collect()
            
            return f"{len(df_differences)} differences found and inserted into Snowflake for {MONGO_COLLECTION}."
        
    except Exception as e:
        return f"Error processing {MONGO_COLLECTION}: {e}"
$$;
