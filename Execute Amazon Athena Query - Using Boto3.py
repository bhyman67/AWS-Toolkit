import boto3
import time
 
# Initialize the Athena and S3 clients
session = boto3.Session(profile_name='')
athena_client = session.client('athena', region_name='us-east-1')  # Update your region as needed
 
# Define parameters
s3_output = ''  # Replace with your S3 bucket for Athena results
database = ''  # Replace with your Athena database
workgroup = ''
query = """
    select * 
    from my_db.my_table
"""  # Replace with your SQL query
 
def run_athena_query(query, database, s3_output, workgroup):
    # Start query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output},
        WorkGroup = workgroup
    )
    query_execution_id = response['QueryExecutionId']
    print(f"Started query with execution ID: {query_execution_id}")
   
    # Wait for the query to complete
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
       
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        print("Query is still running...")
        time.sleep(2)  # Wait before checking again
 
    if status == 'SUCCEEDED':
        print("Query succeeded!")
        return query_execution_id
    else:
        print(f"Query failed or was cancelled with status: {status}")
        return None
 
# Run the function
execution_id = run_athena_query(query, database, s3_output, workgroup)
 
if execution_id:
    print(f"Results are available in S3 at: {s3_output}{execution_id}.csv")