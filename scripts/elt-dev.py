##############################################################################
# development ELT script for dummy pipeline
##############################################################################
"""
To discuss:
- to call as modules in .ipynb?
- debugging/ running code in chunks?
- Visualisation of new repo structure
- python entry point

Done:
- modularised refactored code to split into constants, functions and run scripts
- use logging consistently
- added data types to dockstrings
- use dockstrings consistently
- split monolithic functions
- close files after transform to avoid overloading cache

To do:
- improvements:
    check path in extract
- Go through repo steps
    add mojap columns
    Write table to S3
    Move files to raw hist
    Apply scd2
- Include dev/prod environment parameters
- create a docker image
- create a github action to run pipeline automatically
- create unit tests
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

# Refactored functions for listing, checking, and uploading parquet files to S3 with logging

def list_parquet_files(local_directory):
    """
    List all .parquet files in a local directory.

    Parameters:
        local_directory (str): Path to the local directory.

    Returns:
        list: List of .parquet file paths.
    """
    return [
        os.path.join(local_directory, file)
        for file in os.listdir(local_directory)
        if file.endswith('.parquet')
    ]

def file_exists_in_s3(bucket_name, s3_key):
    """
    Check if a file exists in an S3 bucket.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        s3_key (str): Path of the file in the S3 bucket.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        logging.info(f"File exists in S3: s3://{bucket_name}/{s3_key}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logging.info(f"File does not exist in S3: s3://{bucket_name}/{s3_key}")
            return False
        logging.error(f"Error checking existence of {s3_key}: {e}")
        raise

def upload_parquet_files_to_s3(bucket_name, local_directory, s3_prefix, dry_run=True):
    """
    Upload .parquet files from a local directory to an S3 bucket under a specified prefix.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        local_directory (str): Path to the local directory containing .parquet files.
        s3_prefix (str): Path prefix within the S3 bucket.
        dry_run (bool): If True, simulates the upload without actually uploading files.

    Returns:
        list: List of successfully uploaded file paths.
    """
    parquet_files = list_parquet_files(local_directory)
    uploaded_files = []

    for local_path in parquet_files:
        file_name = os.path.basename(local_path)
        s3_key = f"{s3_prefix}/{file_name}"

        if file_exists_in_s3(bucket_name, s3_key):
            logging.info(f"Skipping upload for existing file: {local_path}")
            continue

        if dry_run:
            logging.info(f"[DRY RUN] Would upload: {local_path} to s3://{bucket_name}/{s3_key}")
        else:
            try:
                s3 = boto3.client('s3')
                s3.upload_file(local_path, bucket_name, s3_key)
                logging.info(f"Successfully uploaded: {local_path} to s3://{bucket_name}/{s3_key}")
                uploaded_files.append(local_path)
            except Exception as e:
                logging.error(f"Failed to upload: {local_path}. Error: {e}")

    return uploaded_files

# Example usage
#upload_parquet_files_to_s3(
#    bucket_name='alpha-hmcts-de-testing-sandbox',
#    local_directory='data/example-data',
#    s3_prefix='de-intro-project-jb/dev',
#    dry_run=False
#)

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
print(metadata)

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

print(df.head())
    
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
print(df.head())



