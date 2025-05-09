import awswrangler as wr
import boto3
import json
import os

# Load configuration from config.json
config_file = os.path.expanduser("~/aws/AWS Python Script Config.json")
with open(config_file, 'r') as file:
    config = json.load(file)

# Extract configuration values
aws_profile = config["aws_profile"]
region_name = config["region_name"]
athena_workgroup = config["athena_workgroup"]

# Initialize a session with the correct AWS profile and region
session = boto3.Session(profile_name=aws_profile, region_name=region_name)

# Define Athena DB and workgroup
database = config.get("database", "")  # Optional: Add "database" to config.json if needed
workgroup = athena_workgroup

# Define the query
query = """
    select *
    from my_db.my_table
"""

# Execute the query and load the results into a DataFrame
df = wr.athena.read_sql_query(
    sql=query,
    database=database,
    boto3_session=session,
    workgroup=workgroup
)

# Display the DataFrame
print(df.head())