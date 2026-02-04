import boto3
import os
import logging
from botocore.exceptions import ClientError
import s3fs
from arrow_pd_parser import reader
import pandas as pd
import json
from mojap_metadata import Metadata
from pandas import DataFrame
from typing import List, Dict

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def list_parquet_files(local_directory: str) -> List[str]:
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

def file_exists_in_s3(bucket_name: str, s3_key: str) -> bool:
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

def upload_parquet_files_to_s3(bucket_name: str, local_directory: str, s3_prefix: str, dry_run: bool = True) -> List[str]:
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

def list_parquet_files_from_s3(bucket_name: str, s3_prefix: str) -> List[str]:
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

def load_parquet_files_from_s3(bucket_name: str, s3_prefix: str) -> DataFrame:
    """
    Loads and parses Parquet files from S3 using PyArrow and a custom parser.

    Parameters:
        bucket_name (str): S3 bucket name.
        s3_prefix (str): Prefix (folder path) in the bucket.

    Returns:
        pd.DataFrame: Combined DataFrame from all Parquet files.
    """
    parquet_files = list_parquet_files_from_s3(bucket_name, s3_prefix)
    all_dfs = []

    for file_path in parquet_files:
        s3_uri = f"s3://{file_path}"
        try:
            with reader.open(s3_uri) as f:
                df = reader.read(f)
            all_dfs.append(df)
        except Exception as e:
            logging.error(f"Failed to read Parquet file {s3_uri}. Error: {e}")

    combined_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    logging.info(f"Loaded DataFrame with {len(combined_df)} rows from S3")
    return combined_df

def load_metadata(filename: str) -> Dict:
    """
    Load metadata from a JSON file located in the data/metadata folder.

    Parameters:
        filename (str): Name of the JSON file.

    Returns:
        dict: Parsed metadata as a dictionary.
    """
    metadata_path = os.path.join("data", "metadata", filename)
    with open(metadata_path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Normalize column names in a DataFrame.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with normalized column names.
    """
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    return df

def enforce_metadata_types(df: DataFrame, metadata_obj: Metadata) -> DataFrame:
    """
    Enforce column data types in a DataFrame based on mojap-metadata schema.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        metadata_obj (Metadata): Metadata object defining column types.

    Returns:
        pd.DataFrame: DataFrame with enforced column types.
    """
    for col in metadata_obj.columns:
        if isinstance(col, dict):
            col_name = col["name"].lower().replace(" ", "_")
            col_type = col["type"]
            fmt = col.get("datetime_format", None)
        else:
            col_name = col.name.lower().replace(" ", "_")
            col_type = col.type
            fmt = getattr(col, "datetime_format", None)

        if col_name not in df.columns:
            logging.warning(f"Column '{col_name}' not found in DataFrame.")
            continue

        if col_type == "string":
            df[col_name] = df[col_name].astype("string")
        elif col_type.startswith("timestamp"):
            df[col_name] = pd.to_datetime(df[col_name], format=fmt, errors="coerce")
        elif col_type in ["int", "integer"]:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
        elif col_type in ["float", "double"]:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

    return df
