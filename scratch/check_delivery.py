import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

print("Listing blobs in 'client-delivery' container:")
container_client = blob_service_client.get_container_client("client-delivery")

blobs = list(container_client.list_blobs())
for blob in blobs:
    print(f"Name: {blob.name}, Last Modified: {blob.last_modified}")
