import os
from typing import Dict, List
import pandas as pd


# SETTINGS

LOCAL_TMP = "./tmp"


TMP_FOLDER = LOCAL_TMP

def transform_call_logs_csv(tmp_folder: str = TMP_FOLDER):
    """
    Reads all CSV files starting with 'call_logs' from tmp folder, 
    performs required transformations and returns ONE combined DataFrame.
    """

    csv_files = [
        f for f in os.listdir(tmp_folder)
        if f.startswith("call_logs") and f.endswith(".csv")
    ]

    if not csv_files:
        print("No call_logs CSV files found.")
        return pd.DataFrame()

    df_list = []

    for file in csv_files:
        file_path = os.path.join(tmp_folder, file)

        # Load CSV
        df = pd.read_csv(file_path)

        # Fix broken column name 
        # Handles cases like "COMPLAINT_catego ry" with space in between
        bad_cols = ["COMPLAINT_catego ry", "COMPLAINT_catego  ry", "COMPLAINT_category "]
        for bad_col in bad_cols:
            if bad_col in df.columns:
                df = df.rename(columns={bad_col: "complaint_category"})

        #  Generic column name cleaning 
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        #  Convert date-like columns to datetime 
        for col in df.columns:
            if "date" in col:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        df_list.append(df)

    # Combine all transformed dfs into one
    final_df = pd.concat(df_list, ignore_index=True)

    return final_df

def transform_web_form_csvs(tmp_folder: str = TMP_FOLDER):
    """
    Reads all CSV files starting with 'web_form' from the tmp folder,
    transforms column names (lowercase, underscores, no spaces),
    converts date columns to datetime format,
    and returns ONE combined DataFrame.
    """

    csv_files = [
        f for f in os.listdir(tmp_folder)
        if f.startswith("Web_form") and f.endswith(".csv")
    ]

    if not csv_files:
        print("No web_form CSV files found.")
        return pd.DataFrame()

    df_list = []

    for file in csv_files:
        file_path = os.path.join(tmp_folder, file)

        # Load CSV
        df = pd.read_csv(file_path)

        #  Transform Column Names 
        df.columns = (
            df.columns
            .str.lower()
            .str.strip()
            .str.replace(" ", "_")
        )

        #  Convert Date Columns 
        for col in df.columns:
            if "date" in col:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        df_list.append(df)

    # Combine all transformed CSV files
    final_df = pd.concat(df_list, ignore_index=True)

    return final_df


def transform_media_complaint_jsons(tmp_folder: str = TMP_FOLDER):
    """
    Reads all JSON files that start with 'media_complaint' from the tmp folder,
    transforms column names (lowercase, replace spaces with underscore),
    converts date columns to datetime, and returns ONE combined DataFrame.
    """

    json_files = [
        f for f in os.listdir(tmp_folder)
        if f.startswith("media_complaint") and f.endswith(".json")
    ]

    if not json_files:
        print("No media_complaint JSON files found.")
        return pd.DataFrame()  # return empty dataframe

    df_list = []

    for file in json_files:
        file_path = os.path.join(tmp_folder, file)

        # Load JSON
        df = pd.read_json(file_path)

        #  Transform Column Names 
        df.columns = (
            df.columns
            .str.lower()
            .str.strip()
            .str.replace(" ", "_")
        )

        #  Convert All Date Columns to datetime 
        for col in df.columns:
            if "date" in col:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        df_list.append(df)

    # Combine all dataframes into one
    final_df = pd.concat(df_list, ignore_index=True)

    return final_df

def transform_customers_csv(tmp_folder: str = TMP_FOLDER):
    """
    Reads all CSV files starting with 'customer details' from tmp folder, 
    performs required transformations and returns ONE combined DataFrame.
    """

    csv_files = [
        f for f in os.listdir(tmp_folder)
        if f.startswith("customers") and f.endswith(".csv")
    ]

    if not csv_files:
        print("No call_logs CSV files found.")
        return pd.DataFrame()

    df_list = []

    for file in csv_files:
        file_path = os.path.join(tmp_folder, file)

        # Load CSV
        df = pd.read_csv(file_path)

        # Fix broken column name 
        # Handles cases like "COMPLAINT_catego ry" with space in between
        bad_cols = ["COMPLAINT_catego ry", "COMPLAINT_catego  ry", "COMPLAINT_category "]
        for bad_col in bad_cols:
            if bad_col in df.columns:
                df = df.rename(columns={bad_col: "complaint_category"})

        #  Generic column name cleaning 
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        # Convert date-like columns to datetime 
        for col in df.columns:
            if "date" in col:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        df_list.append(df)

    # Combine all transformed dfs into one
    final_df = pd.concat(df_list, ignore_index=True)

    return final_df