import boto3
import time

athena_client = boto3.client('athena')

def check_query_status(query_execution_id):
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED':
            print("Query succeeded")
            return True
        elif status == 'FAILED':
            print("Query failed")
            return False
        elif status == 'CANCELLED':
            print("Query was cancelled")
            return False
        time.sleep(2)

def run_query(query, database, output_location):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    return response['QueryExecutionId']

def run_all_sql_statements(file_path, database, output_location):
    with open(file_path, 'r') as file:
        sql_statements = file.read().split(';')
        for statement in sql_statements:
            if statement.strip():
                query_execution_id = run_query(statement.strip(), database, output_location)
                if not check_query_status(query_execution_id):
                    print(f"Query failed: {statement.strip()}")
                    break
                else:
                    print(f"Query succeeded: {statement.strip()}")

if __name__ == "__main__":
    file_path = 'path/to/your/sqlfile.sql'
    database = 'your_database_name'
    output_location = 's3://your-output-bucket/'
    
    # Run queries from file
    run_all_sql_statements(file_path, database, output_location)
    
    # Define additional queries and their sequence
    queries = [
        "CREATE TABLE IF NOT EXISTS example_table AS SELECT * FROM source_table WHERE condition;",
        "SELECT * FROM example_table WHERE another_condition;",
        # Add more queries as needed
    ]
    
    # Run additional queries sequentially
    for query in queries:
        query_execution_id = run_query(query, database, output_location)
        if not check_query_status(query_execution_id):
            print("Exiting due to query failure.")
            break