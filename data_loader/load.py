import os
import boto3
from dotenv import load_dotenv
from supabase import create_client, Client
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")

# SETTINGS
AWS_REGION = "eu-north-1"   # Stockholm Region
LOCAL_TMP = "./tmp"

def init_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise Exception("Supabase credentials missing in environment variables")

    return create_client(url, key)


def dataframe_to_supabase(df, table_name, supabase: Client, chunk_size=500):
    """
    Insert a dataframe into a Supabase table in chunks.
    Converts datetime columns to ISO format automatically.
    """

    if df.empty:
        print(f"Skipped: DataFrame for '{table_name}' is empty.")
        return

    # Convert all datetime columns to ISO strings
    for col in df.columns:
        if str(df[col].dtype).startswith("datetime"):
            df[col] = df[col].astype(str)

    # Convert df to list of dicts
    records = df.to_dict(orient="records")

    print(f"Uploading {len(records)} rows to table '{table_name}'...")

    # Insert in chunks (Supabase recommends batching)
    for i in range(0, len(records), chunk_size):
        batch = records[i:i+chunk_size]

        response = supabase.table(table_name).insert(batch).execute()

        if response.get("error"):
            print(f"Error inserting batch {i}: {response['error']}")
        else:
            print(f"Batch {i} inserted successfully.")

    print(f"Done inserting into '{table_name}'.")

def ensure_bucket_exists(bucket_name, region=AWS_REGION):
    s3 = boto3.client("s3", region_name=AWS_REGION,\
        aws_access_key_id=ACCESS_KEY_ID,\
        aws_secret_access_key=SECRET_ACCESS_KEY)

    # Check if bucket exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
        return
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code != 404:
            raise e

    # If not exists → create it
    print(f"Creating bucket '{bucket_name}'...")
    if region == "us-east-1":
        s3.create_bucket(Bucket=bucket_name)
    else:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region}
        )
    print("Bucket created.")


def upload_parquet_files(local_dir, bucket_name, s3_prefix=""):
    s3 = boto3.client("s3", region_name=AWS_REGION,\
        aws_access_key_id=ACCESS_KEY_ID,\
        aws_secret_access_key=SECRET_ACCESS_KEY)

    # Ensure the bucket exists
    ensure_bucket_exists(bucket_name)

    # Loop through local parquet files
    for file_name in os.listdir(local_dir):
        if file_name.endswith(".parquet"):
            local_path = os.path.join(local_dir, file_name)
            s3_path = f"{s3_prefix}/{file_name}" if s3_prefix else file_name

            try:
                print(f"Uploading {local_path} -> s3://{bucket_name}/{s3_path}")
                s3.upload_file(local_path, bucket_name, s3_path)
            except Exception as e:
                print(f"ERROR uploading {file_name}: {e}")

    print("Upload complete!")


def remove_files(tmp_dir = LOCAL_TMP):
    for entry in os.listdir(tmp_dir):
        full_path = os.path.join(tmp_dir, entry)
        if os.path.isfile(full_path):
            os.remove(full_path)
        # to delete folders too:
        # elif os.path.isdir(full_path):
        #     shutil.rmtree(full_path)
