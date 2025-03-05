import awswrangler as wr
import boto3

# wondering if you'll see this in the Athena hist tab

# Initialize a session with the correct AWS profile
session = boto3.Session(profile_name='', region_name='us-east-1')
 
# Define Athena DB and workgroup
database = ''  # Athena database name
workgroup = ''

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