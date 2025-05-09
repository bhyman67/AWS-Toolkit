echo -n "Enter the EMR Primary Node ID: "
read emr_primary_node_id
echo "The EMR Primary Node ID is: $emr_primary_node_id"
aws --profile impulse-dev --region us-east-1 ssm start-session --target $emr_primary_node_id