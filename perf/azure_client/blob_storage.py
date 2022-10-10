import io
import os

from azure.storage.blob import BlobServiceClient

from constants import TOTAL

CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
CONTAINER = os.getenv('AZURE_CONTAINER')


def get_azure_message_counts(directory):
    """
    Method to get counts of messages in azure (partition/folder wise)
    :param directory:
    :return:
    """
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container=CONTAINER)
    counts = {}
    blob_list = container_client.list_blobs(name_starts_with=directory)
    for blob in blob_list:
        partition = blob.name.rsplit('/', 2)[-2].upper()
        blob_stream = container_client.download_blob(blob.name)
        for chunk in blob_stream.chunks():
            file_chunk = io.BytesIO(chunk)
            count = len([1 for _ in file_chunk])
            counts[partition] = counts.get(partition, 0) + count
    counts[TOTAL] = sum(counts.values())
    return counts
