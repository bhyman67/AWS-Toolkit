import os
import boto3
import time
import json
from botocore.config import Config

# Load configuration from config.json
config_file = os.path.expanduser("~/aws/AWS Python Script Config.json")
with open(config_file, 'r') as file:
    config = json.load(file)

# Extract configuration values
aws_profile = config["aws_profile"]
athena_workgroup = config["athena_workgroup"]
region_name = config["region_name"]
output_location = config["output_location"]

# Initialize a session with the specified profile
session = boto3.Session(profile_name=aws_profile)

# AWS Athena client
athena_client = session.client('athena', region_name=region_name)

def get_table_ddl(database, table_name, output_location):
    """
    Retrieves the DDL statement for a given table in Athena.
    """
    query = f"SHOW CREATE TABLE {database}.{table_name};"
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location},
        WorkGroup=athena_workgroup  # Use the global workgroup variable
    )
    query_execution_id = response['QueryExecutionId']
    
    # Wait for the query to complete
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if status != 'SUCCEEDED':
        raise Exception(f"Query failed with status: {status}")
    
    # Fetch the results
    result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    rows = result['ResultSet']['Rows']
    
    # Combine all rows (skip the header row)
    ddl_statement = "\n".join(row['Data'][0]['VarCharValue'] for row in rows[:])
    
    # Return only the DDL statement (without including the query)
    return ddl_statement

def retrieve_ddls(tables, output_location):
    """
    Retrieves DDL statements for a list of tables.
    """
    ddls = {}
    for db_table in tables:
        database, table_name = db_table.split('.')
        try:
            ddl = get_table_ddl(database, table_name, output_location)
            ddls[db_table] = ddl
        except Exception as e:
            ddls[db_table] = f"Error: {str(e)}"
    return ddls

def write_ddls_to_file(ddls, output_file):
    """
    Writes DDL statements to a file with one empty line between each statement.
    """
    with open(output_file, 'w') as file:
        for table, ddl in ddls.items():
            file.write(f"-- DDL for {table} --\n")
            file.write(ddl)
            file.write("\n\n")  # Add one empty line between DDLs

if __name__ == "__main__":
    # List of db.table_name pairs
    tables = [
        "prod_mca.wifi_cm_devices_agg",
        "prod_wifi_lkp.ip_ap_mac",
        "prod_wifi.wifi_consolidated_equipment",
        "prod_mob.mvno_bi_acctlines_enriched",
        "prod_wifi.wifi_radius_event_usage_v2",
        "tmp_prod_mob.isaac_adhoc_wifi_type_lkp"
    ]
    
    # Directory to save the output file
    output_dir = "/Users/P3193404/.aws/DDL Statements/"
    os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist
    
    # Output file to write DDLs
    output_file = os.path.join(output_dir, "ddl_statements.txt")
    
    # Retrieve DDLs
    ddls = retrieve_ddls(tables, output_location)
    
    # Write DDLs to file
    write_ddls_to_file(ddls, output_file)
    
    print(f"DDL statements written to {output_file}")