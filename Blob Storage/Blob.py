'''
@Author: Santanu Mohapatra
@Date: 09/07/2021
@Last Modified by: Santanu Mohapatra
@Last Modified Time: 09:05 AM
@Title: Python program for Blob Storage.
'''

import os, uuid, sys
from azure.storage.blob import BlobServiceClient

def blob_storage_sample():
    try:
        block_blob_service = BlobServiceClient(account_name='blobstoragedemo', account_key='')

        container_name ='letsstartblob'
        block_blob_service.create_container(container_name)

        
        local_path=os.path.abspath(os.path.curdir)
        local_file_name = input("Enter file name to upload : ")
        full_path_to_file = os.path.join(local_path, local_file_name)

        print("Temp file = " + full_path_to_file)
        print("\nUploading to Blob storage as blob" + local_file_name)

        block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)

        print("\nList blobs in the container")
        generator = block_blob_service.list_blobs(container_name)
        for blob in generator:
            print("\t Blob name: " + blob.name)


        full_path_to_file2 = os.path.join(local_path, str.replace(local_file_name , '.txt', '_DOMNLOADED.txt'))
        print("\nDownloading blob to " + full_path_to_file2)
        block_blob_service.get_blob_to_path(container_name, local_file_name, full_path_to_file2)

        sys.stdout.write("Sample finished running. when you hit <any key>, the sample will be deleted and the sample "
                            "application will exit.")
        sys.stdout.flush()
        input()


        block_blob_service.delete_container(container_name)
        os.remove(full_path_to_file)
        os.remove(full_path_to_file2)
    except Exception as e:
        print(e)

if __name__=='__main__':
   blob_storage_sample()