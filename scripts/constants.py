# S3 Configuration
S3_BUCKET = "alpha-hmcts-de-testing-sandbox"
S3_PREFIX = "de-intro-project-jb/dev"

# Local Directory
LOCAL_DIRECTORY = "data/example-data"

# Metadata File
METADATA_FILE = "intro-project-metadata.json"

# Other Configurations
DRY_RUN = False
RUN_MODE = "write"
DATA_PATH = "data/input-data"
OUTPUT_PATH = "data/output-data"

"""
To do:
- Outstanding repo steps
    add mojap columns
    Write table to S3
    Move files to raw hist
    Apply scd2
- Include dev/prod environment parameters?
- create a docker image
- create a github action to run pipeline automatically
- create unit tests
- Integration/ end to end testing
- Update requirements file and build it into the script
- Requirements lint
- Nice to have: package it up as a python package?
"""