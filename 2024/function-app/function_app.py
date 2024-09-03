import os
import json
import pandas as pd
import logging
from azure.storage.blob import BlobServiceClient
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="TimerTrigger")
@app.timer_trigger(schedule="0 */10 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    # Retrieve the connection string from the environment variable
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_names = ['insights-logs-appservicehttplogs']

    # Initialize the BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    logs = []

    # Iterate over each container and each blob in the container
    for container_name in container_names:
        container_client = blob_service_client.get_container_client(container_name)
        for blob in container_client.list_blobs():
            blob_data = container_client.get_blob_client(blob).download_blob().readall()
            logs.extend(json.loads(blob_data))

    # Convert the aggregated logs to a DataFrame
    df = pd.DataFrame(logs)

    # Example preprocessing steps
    df['Timestamp'] = pd.to_datetime(df['time'])
    df.dropna(inplace=True)

    # Extract relevant features for machine learning
    features = df[['Timestamp', 'HttpMethod', 'StatusCode', 'TimeTaken']]
    labels = df['SomeLabel']  # Replace with your actual label column

    # Save the preprocessed data to a CSV file in the storage account
    csv_data = df.to_csv(index=False)
    output_blob_name = 'preprocessed_logs.csv'  # Replace with your desired blob name
    output_blob_client = blob_service_client.get_blob_client(container_names[0], output_blob_name)
    output_blob_client.upload_blob(csv_data, overwrite=True)

    logging.info("Logs preprocessed and uploaded to the storage account as 'preprocessed_logs.csv'")