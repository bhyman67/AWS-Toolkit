import os
import boto3
import json

# Load configuration from config.json
config_file = os.path.expanduser("~/.aws/AWS Python Script Config.json")
with open(config_file, 'r') as file:
    config = json.load(file)

# Extract configuration values
aws_profile = config["aws_profile"]
region_name = config["region_name"]
catalog_name = config.get("catalog_name", "AwsDataCatalog")

# List of tables to retrieve metadata for (format: ("database_name", "table_name"))
tables = [
    ("prod_mca", "wifi_cm_devices_agg"),
    ("prod_wifi_lkp", "ip_ap_mac"),
    ("prod_wifi", "wifi_consolidated_equipment"),
    ("prod_mob", "mvno_bi_acctlines_enriched"),
    ("prod_wifi", "wifi_radius_event_usage_v2"),
    ("tmp_prod_mob", "isaac_adhoc_wifi_type_lkp"),
    ("prod_mob", "rpt_nba_os_version_trend")
]

# Initialize a session with the specified profile
session = boto3.Session(profile_name=aws_profile)
client = session.client('athena', region_name=region_name)

metadata = {}

for database_name, table_name in tables:
    try:
        response = client.get_table_metadata(
            CatalogName=catalog_name,
            DatabaseName=database_name,
            TableName=table_name
        )
        table_key = f"{database_name}.{table_name}"
        metadata[table_key] = {
            "Columns": response['TableMetadata']['Columns'],
            "PartitionKeys": response['TableMetadata'].get('PartitionKeys', []),
            "TableType": response['TableMetadata'].get('TableType', ''),
            "Parameters": response['TableMetadata'].get('Parameters', {})
        }
    except Exception as e:
        metadata[f"{database_name}.{table_name}"] = {"Error": str(e)}

# Save all metadata to a single JSON file
output_file = os.path.expanduser("~/.aws/athena_multiple_tables_metadata.json")
with open(output_file, "w") as f:
    json.dump(metadata, f, indent=2)

print(f"Metadata for all tables saved to {output_file}")