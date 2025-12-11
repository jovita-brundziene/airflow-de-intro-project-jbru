
##############################################################################
# development ELT script for dummy pipeline
##############################################################################
"""
To do:
- Go through repo steps
    - add mojap columns
    Write table to S£
    Move files to raw hist
    Apply scd2
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

import json
import os
from mojap_metadata import Metadata

##############################################################################
# Extract data from local to S3
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
    s3_prefix='de-intro-project-jb/dev',
    dry_run=False
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
        s3_uri = f"s3://{file_path}"  # prepend s3://
        df = reader.read(s3_uri)      # now reader sees full S3 URI
        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

#load data
bucket = "alpha-hmcts-de-testing-sandbox"
prefix = "de-intro-project-jb/dev"

df = load_parquet_files_from_s3(bucket, prefix)
print(df.head())

##############################################################################
# Enforce Metadata types
##############################################################################

# load metadata function
def load_metadata(filename: str) -> dict:
    """
    Load metadata from a JSON file located in the data/metadata folder.
    """
    metadata_path = os.path.join("data", "metadata", filename)
    with open(metadata_path, "r", encoding="utf-8") as f:
        return json.load(f)

# load metadata
metadata = load_metadata("intro-project-metadata.json")
metadata

# create Metadata object from JSON
metadata_obj = Metadata.from_dict(metadata)

#validate metadata against schema
metadata_obj.validate()

# function to normalise column names to lowercase and replace spaces with underscores
def normalize_column_names(df):
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    return df

# apply normalisation
df = normalize_column_names(df)

# convert 'Date of birth' from 'YYYY-MM-DD' to ISO timestamp format 'YYYY-MM-DDTHH:MM:SS'
def convert_to_iso_timestamp(date_str):
    if pd.isna(date_str):
        return None
    try:
        # Parse date string and format as ISO timestamp
        return pd.to_datetime(date_str, format='%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%S')
    except Exception as e:
        print(f'Error converting {date_str}: {e}')
        return None

# apply dob conversion
df['date_of_birth'] = df['date_of_birth'].apply(convert_to_iso_timestamp)

df.head()
    
# function for metadata enforcement
def enforce_metadata_types(df, metadata_obj):
    """
    Enforce column data types in a DataFrame based on mojap-metadata schema.
    Handles both dict-based and object-based columns.
    """
    for col in metadata_obj.columns:
        # Detect if col is dict or object
        if isinstance(col, dict):
            col_name = col["name"].lower().replace(" ", "_")
            col_type = col["type"]
            fmt = col.get("datetime_format", None)
        else:  # Column-like object
            col_name = col.name.lower().replace(" ", "_")
            col_type = col.type
            fmt = getattr(col, "datetime_format", None)

        if col_name not in df.columns:
            print(f"⚠️ Column '{col_name}' not found in DataFrame.")
            continue

        # Apply type casting
        if col_type == "string":
            df[col_name] = df[col_name].astype("string")
        elif col_type.startswith("timestamp"):
            df[col_name] = pd.to_datetime(df[col_name], format=fmt, errors="coerce")
        elif col_type in ["int", "integer"]:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
        elif col_type in ["float", "double"]:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

    return df

#enforce metadata types
df = enforce_metadata_types(df, metadata_obj)

print(df.dtypes)
df.head()



