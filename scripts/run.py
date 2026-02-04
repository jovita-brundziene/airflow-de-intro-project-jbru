import logging
from constants import (
    S3_BUCKET,
    S3_PREFIX,
    LOCAL_DIRECTORY,
    METADATA_FILE,
    DRY_RUN,
    RUN_MODE,
    DATA_PATH,
    OUTPUT_PATH
)

from functions import (
    upload_parquet_files_to_s3,
    load_parquet_files_from_s3,
    load_metadata,
    normalize_column_names,
    enforce_metadata_types
)

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Upload parquet files to S3
uploaded_files = upload_parquet_files_to_s3(
    bucket_name=S3_BUCKET,
    local_directory=LOCAL_DIRECTORY,
    s3_prefix=S3_PREFIX,
    dry_run=DRY_RUN
)
logging.info(f"Uploaded files: {uploaded_files}")

# Load parquet files from S3
df = load_parquet_files_from_s3(S3_BUCKET, S3_PREFIX)
logging.info(f"Loaded DataFrame head:\n{df.head()}")

# Load metadata from JSON file
metadata = load_metadata(METADATA_FILE)
logging.info(f"Loaded metadata: {metadata}")

# Normalize column names in the dataframe
df = normalize_column_names(df)

# Enforce metadata types on the dataframe
from mojap_metadata import Metadata
metadata_obj = Metadata.from_dict(metadata)
df = enforce_metadata_types(df, metadata_obj)

logging.info(f"DataFrame dtypes:\n{df.dtypes}")
logging.info(f"DataFrame head after enforcing metadata types:\n{df.head()}")
