import psycopg2
import os
import re
import io
import json
import hashlib
import logging
import tempfile
import datetime
from load import remove_files
from load import init_supabase
from load import dataframe_to_supabase
from load import upload_parquet_files
from typing import Dict, List, Tuple

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from extract import get_ssm_parameter
from extract import ensure_tmp_folder
from extract import download_customer_csvs
from extract import extract_postgres_table
from extract import download_s3_prefix

from transform import transform_media_complaint_jsons
from transform import transform_web_form_csvs
from transform import transform_call_logs_csv
from transform import transform_customers_csv

load_dotenv()
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")

# SETTINGS
AWS_REGION = "eu-north-1"   # Stockholm Region
LOCAL_TMP = "./tmp"

# check and create folder
ensure_tmp_folder()

# EXTRACT

# DAILY POSTGRES TABLES
POSTGRES_TABLES = [
    "Web_form_request_2025_11_20",
    "Web_form_request_2025_11_21",
    "Web_form_request_2025_11_22",
    "Web_form_request_2025_11_23",
]
POSTGRES_SSM_PARAMETER = "/coretelecomms/database/"  # JSON stored here
CALL_LOGS_PREFIX = "s3://core-telecoms-data-lake/call logs/"  # contains daily CSV
SOCIAL_MEDIA_PREFIX = "s3://core-telecoms-data-lake/social_medias/"

# get parameters from parameter store
db_host = get_ssm_parameter("/coretelecomms/database/db_host")
db_name = get_ssm_parameter("/coretelecomms/database/db_name")
db_user = get_ssm_parameter("/coretelecomms/database/db_username")
db_pass = get_ssm_parameter("/coretelecomms/database/db_password")
db_port = get_ssm_parameter("/coretelecomms/database/db_port")


# Fetch database credentials from SSM
db_credentials = {
    "host": get_ssm_parameter("/coretelecomms/database/db_host"),
    "database": get_ssm_parameter("/coretelecomms/database/db_name"),
    "user": get_ssm_parameter("/coretelecomms/database/db_username"),
    "password": get_ssm_parameter("/coretelecomms/database/db_password"),
    "port": get_ssm_parameter("/coretelecomms/database/db_port"),
}


# Extract Website Complaints (Daily, from Postgres)
for table in POSTGRES_TABLES:
    df = extract_postgres_table(table, db_credentials)
    df.to_csv(os.path.join(LOCAL_TMP, f"{table}.csv"), index=False)

logging.info("All extractions completed successfully!")
 
# extract customer complains
cust = download_customer_csvs()
# Extract Social Media Complaints (Daily JSON)
social_files = download_s3_prefix(SOCIAL_MEDIA_PREFIX, LOCAL_TMP, file_type="json")
# Extract Call Center Logs (Daily CSV)
call_logs = download_s3_prefix(CALL_LOGS_PREFIX, LOCAL_TMP, file_type="csv")


# TRANSFORMATION
# transform media complain data
media_complains = transform_media_complaint_jsons()
# transform web form data
web_form = transform_web_form_csvs()
# transform call logs data
call_logs = transform_call_logs_csv()
# transform customer data
customers = transform_customers_csv()

# SAVE transformed dataframe as parquet
media_complains.to_parquet("tmp/media_complains.parquet", compression="snappy")
web_form.to_parquet("tmp/web_form.parquet", compression="snappy")
call_logs.to_parquet("tmp/call_logs.parquet", compression="snappy")
customers.to_parquet("tmp/customers.parquet", compression="snappy")

# LOAD
dataframes = {
    "call_logs": call_logs,
    "media_complaints": media_complains,
    "web_form": web_form,
    "customers":customers
}

# initialize supabase
supabase = init_supabase()
# Ingest to Superbase Database
for table_name, df in dataframes.items():
    dataframe_to_supabase(df, table_name, supabase)

upload_parquet_files(
        local_dir=LOCAL_TMP,
        bucket_name="supabase-bucket-2025",
        s3_prefix="raw")

# remove raw files
remove_files()