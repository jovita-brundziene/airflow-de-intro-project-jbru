
##############################################################################
# development ELT script for dummy pipeline
##############################################################################
"""
To do:
- Go through repo steps
- Include dev/prod environment parameters
- add parameters to config file
- create a docker image
- create a github action to run pipeline automatically
- create unit tests
- modularise code into at least config, functions and run
- Update requirements file and build it into the script
- Requirements lint?
- Nice to have: package it up as a python package?
"""

import boto3
import os
import logging
from botocore.exceptions import ClientError

import s3fs
from arrow_pd_parser import reader
import pandas as pd

##############################################################################
# extract data from local to S3
##############################################################################

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# function for uploading parquet files to S3
# -----> Improvement: can I separate out logging from core function? <-----
def upload_parquet_files_to_s3(bucket_name, local_directory, s3_prefix, dry_run=True):
    """
    Uploads .parquet files from a local directory to an S3 bucket under a specified prefix.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        local_directory (str): Path to the local directory containing .parquet files.
        s3_prefix (str): Path prefix within the S3 bucket.
        dry_run (bool): If True, simulates the upload without actually uploading files.
    """
    # Create an S3 client using boto3
    s3 = boto3.client('s3')

    # Loop through all files in the specified local directory
    for file in os.listdir(local_directory):
        # Only process files with a .parquet extension
        if file.endswith('.parquet'):
            # Construct the full local file path
            local_path = os.path.join(local_directory, file)
            # Define the S3 object key (i.e., path within the bucket)
            s3_key = f'{s3_prefix}/{file}'

            try:
                # Check if the file already exists in the S3 bucket
                s3.head_object(Bucket=bucket_name, Key=s3_key)
                logging.info(f"File already exists in S3: s3://{bucket_name}/{s3_key} — skipping upload.")
            except ClientError as e:
                # If the error code is 404, the file does not exist — proceed with upload
                if e.response['Error']['Code'] == '404':
                    if dry_run:
                        # Simulate the upload in dry run mode
                        print(f"[DRY RUN] Would upload: {local_path} to s3://{bucket_name}/{s3_key}")
                    else:
                        # Attempt to upload the file to S3
                        try:
                            s3.upload_file(local_path, bucket_name, s3_key)
                            logging.info(f"Successfully uploaded: {local_path} to s3://{bucket_name}/{s3_key}")
                        except Exception as upload_error:
                            logging.error(f"Failed to upload: {local_path}. Error: {upload_error}")
                else:
                    # Log unexpected errors during head_object check
                    logging.error(f"Error checking existence of {s3_key}: {e}")

#upload data to S3
#turn this into a config file
upload_parquet_files_to_s3(
    bucket_name='alpha-hmcts-de-testing-sandbox',
    local_directory='data/example-data',
    s3_prefix='de-intro-project-jb/path',
    dry_run=True
)

##############################################################################
# Load data
##############################################################################

# list parquet files in S3 bucket
def list_parquet_files_from_s3(bucket_name: str, s3_prefix: str) -> list:
    """
    Lists all Parquet files in a given S3 prefix using s3fs.

    Parameters:
        bucket_name (str): S3 bucket name.
        s3_prefix (str): Prefix (folder path) in the bucket.

    Returns:
        list: List of full S3 paths to Parquet files.
    """
    fs = s3fs.S3FileSystem()
    s3_path = f"s3://{bucket_name}/{s3_prefix}"
    files = fs.ls(s3_path)
    return [f for f in files if f.endswith('.parquet')]

# load parquet files from S3 into memory
def load_parquet_files_from_s3(bucket_name, s3_prefix):
    """
    Loads and parses Parquet files from S3 using PyArrow and a custom parser.

    Parameters:
        bucket_name (str): S3 bucket name.
        s3_prefix (str): Prefix (folder path) in the bucket.

    Returns:
        pd.DataFrame: Combined DataFrame from all Parquet files.
    """
    #from your_module import list_parquet_files_from_s3  # adjust import as needed

    parquet_files = list_parquet_files_from_s3(bucket_name, s3_prefix)
    all_dfs = []

    for file_path in parquet_files:
        df = reader.read(file_path)  # reader handles S3 paths directly
        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


