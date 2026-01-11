import psycopg2
import os
import logging
from typing import Dict, List

import boto3
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")

# SETTINGS
AWS_REGION = "eu-north-1"   # Stockholm Region
LOCAL_TMP = "./tmp"

# S3 PATHS
CUSTOMERS_S3_PATH = "s3://core-telecoms-data-lake/customers/"
CALL_LOGS_PREFIX = "s3://core-telecoms-data-lake/call logs/"  # contains daily CSV
SOCIAL_MEDIA_PREFIX = "s3://core-telecoms-data-lake/social_medias/"   # contains daily JSON

# DAILY POSTGRES TABLES
POSTGRES_TABLES = [
    "Web_form_request_2025_11_20",
    "Web_form_request_2025_11_21",
    "Web_form_request_2025_11_22",
    "Web_form_request_2025_11_23",
]
POSTGRES_SSM_PARAMETER = "/coretelecomms/database/"  # JSON stored here

# LOGGING SETUP
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger()


# HELPERS
def ensure_tmp_folder():
    if not os.path.exists(LOCAL_TMP):
        os.makedirs(LOCAL_TMP)
        logging.info("Created local tmp folder.")
    else:
        logging.info("Local tmp folder already exists.")

def download_s3_file(s3_path, local_path):
    """Downloads a single object from S3."""
    s3 = boto3.client("s3", region_name=AWS_REGION,\
                    aws_access_key_id=ACCESS_KEY_ID,\
                    aws_secret_access_key=SECRET_ACCESS_KEY)

    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    s3.download_file(bucket, key, local_path)

    logging.info(f"Downloaded S3 file → {local_path}")


def download_s3_prefix(prefix, local_dir, file_type="csv"):
    """Downloads all objects under a folder/prefix."""
    s3 = boto3.client("s3", region_name=AWS_REGION,\
                    aws_access_key_id=ACCESS_KEY_ID,\
                    aws_secret_access_key=SECRET_ACCESS_KEY)
    bucket, key_prefix = prefix.replace("s3://", "").split("/", 1)

    response = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix)

    if "Contents" not in response:
        logging.warning(f"No files found under prefix {prefix}")
        return []

    downloaded_files = []

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(file_type):
            continue

        filename = key.split("/")[-1]
        local_path = os.path.join(local_dir, filename)
        s3.download_file(bucket, key, local_path)
        downloaded_files.append(local_path)

        logging.info(f"Downloaded {file_type.upper()} from → {local_path}")

    return downloaded_files


def get_ssm_parameter(name: str, decrypt: bool = True, region: str = "eu-north-1"):
    
    ssm = boto3.client("ssm", region_name=AWS_REGION,\
                        aws_access_key_id=ACCESS_KEY_ID,\
                    aws_secret_access_key=SECRET_ACCESS_KEY)

    try:
        response = ssm.get_parameter(
            Name=name,
            WithDecryption=decrypt
        )
        return response["Parameter"]["Value"]

    except ClientError as e:
        raise Exception(f"SSM error retrieving '{name}': {e}")


# POSTGRES EXTRACTION
def extract_postgres_table(table_name, credentials):
    """Extracts a single Postgres table into a pandas DataFrame."""
    conn = psycopg2.connect(
        host=credentials["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=credentials["database"],
        port=credentials["port"]
    )

    query = f"SELECT * FROM customer_complaints.{table_name} LIMIT 5;"
    df = pd.read_sql(query, conn)
    conn.close()

    logging.info(f"Extracted Postgres table → {table_name}")
    return df


def download_customer_csvs(
    bucket: str = "core-telecoms-data-lake",
    prefix: str = "customers/",
    local_dir: str = LOCAL_TMP
):
    """
    Downloads all CSV files from a specific S3 prefix into a local directory
    without listing the entire bucket (only objects under the prefix).
    """

    # Ensure local folder exists
    os.makedirs(local_dir, exist_ok=True)

    s3 = boto3.client("s3", region_name=AWS_REGION,\
                        aws_access_key_id=ACCESS_KEY_ID,\
                    aws_secret_access_key=SECRET_ACCESS_KEY)

    # List *only* objects under the prefix
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        print("No files found under the prefix.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]

        # Only download CSV files
        if key.lower().endswith(".csv"):
            filename = os.path.basename(key)
            local_path = os.path.join(local_dir, filename)

            print(f"Downloading {key} → {local_path}")
            s3.download_file(bucket, key, local_path)

    print("All CSV files downloaded successfully!")
