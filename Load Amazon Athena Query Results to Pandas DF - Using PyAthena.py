from pyathena import connect
import pandas as pd
from pyathena.pandas.util import as_pandas
import json
import os

# Load configuration from config.json
config_file = os.path.expanduser("~/aws/AWS Python Script Config.json")
with open(config_file, 'r') as file:
    config = json.load(file)

# Extract configuration values
aws_profile = config["aws_profile"]
athena_workgroup = config["athena_workgroup"]
region_name = config["region_name"]

# Initialize the connection using the configuration values
conn = connect(
    profile_name=aws_profile,
    work_group=athena_workgroup,
    region_name=region_name
)

# Define the query
query = '''
    select *
    from my_db.my_table
'''

# Execute the query and load the results into a DataFrame
cursor = conn.cursor()
cursor.execute(query)
df = as_pandas(cursor)

# Display the DataFrame
print(df.describe())

print("Done")