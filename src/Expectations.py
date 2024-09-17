import pandas as pd
import sys
import json
import platform
import os,requests
from pathlib import Path
import glob
from configs.config import snowflake_conn_prop_local as snowflake_conn_prop
from src.DataValidationContext import GEDataValidationContext
from src.BatchRequest import getBatchRequest 
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest


def createExpectationSuite(context,suitename):
    context.create_expectation_suite(
    expectation_suite_name=suitename, overwrite_existing=True)
        

def createExpectations(session, context, suitename, local_batch_request, pandasdataframe, db_name,schema_name,table):
    from snowflake.snowpark.functions import col
    
    # Creating the validator
    validator = context.get_validator(
        batch_request=local_batch_request, expectation_suite_name=suitename
    )

    # Retrieve the expectations from the table
    #df_sql = session.table("CITIBIKE_2.VALIDATION.EXPECTATIONS").filter(col("TABLE_NAME") == table.upper())
    df_sql = session.table(f'{db_name}.VALIDATION.{schema_name}_EXPECTATIONS').filter(col("TABLE_NAME") == table.upper())
    data = df_sql.collect()
    expectations_df = pd.DataFrame(data)

    if not expectations_df.empty:
        for index, row in expectations_df.iterrows():
            column_name = row['COLUMN_NAME']
            expectation_method_name = row['EXPECTATION']
            parameters = json.loads(row['PARAMETERS'])

            # Dynamically get the expectation method from the validator
            expectation_method = getattr(validator, expectation_method_name, None)

            if expectation_method:
                try:
                    expected_values = parameters.get('expectedValues', {})
                    args = []

                    # Add column_name to args if it's not "NONE"
                    if column_name != "NONE":
                        args.append(column_name)
                    # Process the parameters based on their type
                    if 'value' in expected_values:
                        # Handle single or list values
                        value = expected_values['value']
                        if isinstance(value, list):
                            args.extend(value)
                        else:
                            args.append(value)
                    elif 'between' in expected_values:
                        # Handle range values
                        args.extend(expected_values['between'])
                    elif 'list' in expected_values:
                        # For expectations that require a list as a single argument, pass the whole list
                        list_values = expected_values['list']
                        if list_values:
                            args.append(list_values)  # Append the entire list as a single argument

                    # Call the expectation method with the arguments
                    expectation_method(*args)
                    print(f"Successfully processed {expectation_method_name} for column {column_name}")

                except Exception as e:
                    print(f"Failed to process {expectation_method_name} for column {column_name}: {e}")
    else:
        print(f"No expectations found for the table '{table}'.")

    # Saving the expectation suite
    validator.save_expectation_suite(discard_failed_expectations=False)
