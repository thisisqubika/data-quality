import pandas as pd
import json
import snowflake.connector
from configs.config import snowflake_conn_prop_local as snowflake_conn_prop
from snowflake.snowpark.session import Session
from configs.config import snowflake_conn_prop_local as snowflake_conn_prop
from snowflake.snowpark.types import StringType, TimestampType, BooleanType, StructType, StructField


def process_and_store_validation_results(session, source_table: str, destination_table: str):
    # Read data from the specified Snowflake table into a DataFrame
    df_sql = session.table(source_table)
    data = df_sql.collect()
    df = pd.DataFrame(data) 

    json_object = json.loads(df['RUNVALIDATION'].iloc[-1])

    # Initialize a list to store the extracted information
    extracted_data = []

    # Initialize a dictionary to store information for different expectation types
    expectation_data = {}

    # Assuming json_object contains the provided JSON data
    for item in json_object:
        results = item.get("results", [])  # Get the results list or an empty list if not present
        for result in results:
            expectation_type = result.get("expectation_config", {}).get("expectation_type")
            
            # Extract expectation_config and remove batch_id if it exists
            expectation_config = result.get("expectation_config", {}).get("kwargs", {})
            expectation_config.pop("batch_id", None)  # Remove batch_id

            observed_values = result.get("result", {}).get("observed_value")
            success_status = result.get("success", False)
            runtime = item.get("meta", {}).get("run_id", {}).get("run_time")
                
            # Append the extracted information to the list
            extracted_data.append({
                "EXPECTATION_TYPE": expectation_type,
                "RUNTIME": runtime,
                "EXPECTATION_CONFIG": expectation_config,
                "OBSERVED_VALUE": observed_values,
                "SUCCESS_STATUS": success_status,
                "RUNTIME": runtime
            })
                
            # Store the information in a dictionary using the expectation_type as the key
            expectation_data.setdefault(expectation_type, []).append({
                "EXPECTATION_TYPE": expectation_type,
                "RUNTIME": runtime,
                "EXPECTATION_CONFIG": expectation_config,
                "OBSERVED_VALUE": observed_values,
                "SUCCESS_STATUS": success_status,
            })

    df = pd.DataFrame(extracted_data)

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("EXPECTATION_TYPE", StringType()),
        StructField("RUNTIME", TimestampType()),  # Define RUNTIME as TimestampType
        StructField("EXPECTATION_CONFIG", StringType()),
        StructField("OBSERVED_VALUE", StringType()),
        StructField("SUCCESS_STATUS", BooleanType())
])

    # Process each row and create a list of dictionaries representing each row
    data_to_insert = []

    for index, row in df.iterrows():
        expectation_type = row['EXPECTATION_TYPE']
        runtime = row['RUNTIME']
        expectation_config = json.dumps(row['EXPECTATION_CONFIG'])
        observed_values = json.dumps(row['OBSERVED_VALUE'])
        success_status = row['SUCCESS_STATUS']

        data_to_insert.append({
            "EXPECTATION_TYPE": expectation_type,
            "RUNTIME": runtime,
            "EXPECTATION_CONFIG": expectation_config,
            "OBSERVED_VALUE": observed_values,
            "SUCCESS_STATUS": success_status
        })


    # Create a DataFrame from the list of dictionaries
    df_to_insert = session.create_dataframe(data_to_insert, schema=schema)


    # Write the DataFrame to the Snowflake table
    df_to_insert.write.mode("append").save_as_table(destination_table)

    # Print the number of rows inserted
    print(f"Number of rows inserted: {len(data_to_insert)}")
