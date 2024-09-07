import os
import csv
import json
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError

# Azure Storage configuration
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
container_names = [
    'insights-logs-appserviceapplogs', 
    'insights-logs-appservicehttplogs', 
    'insights-logs-appserviceplatformlogs', 
    'insights-metrics-pt1m',
    'insights-logs-appserviceauditlogs'
]
upload_container_name = 'logs'
upload_blob_name = 'build_data.csv'

# Ensure the connection string is set
if not connection_string:
    raise ValueError("Please set the AZURE_STORAGE_CONNECTION_STRING environment variable")

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

def collect_and_upload_storage_data():
    try:
        # Create an in-memory buffer
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'Blob Name', 'Blob Size', 'Content'
        ])
        
        for container_name in container_names:
            container_client = blob_service_client.get_container_client(container_name)
            blobs = list(container_client.list_blobs())
            for blob in blobs:
                blob_client = container_client.get_blob_client(blob)
                blob_data = blob_client.download_blob().readall().decode('utf-8')
                if blob_data:
                    try:
                        parsed_data = json.loads(blob_data)
                        if isinstance(parsed_data, dict) and all(parsed_data.values()):  # Ensure no missing values
                            writer.writerow([
                                blob.name, blob.size, json.dumps(parsed_data)
                            ])
                    except json.JSONDecodeError:
                        pass
        
        # Upload the CSV data from the in-memory buffer to Azure Blob Storage
        blob_client = blob_service_client.get_blob_client(container=upload_container_name, blob=upload_blob_name)
        blob_client.upload_blob(output.getvalue(), overwrite=True)
    except ResourceNotFoundError:
        pass
    except Exception:
        pass

# Collect storage data and upload to blob
collect_and_upload_storage_data()