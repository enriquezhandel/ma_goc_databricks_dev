# Databricks notebook source
# MAGIC %pip install beautifulsoup4

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import requests, json
import pandas as pd
import warnings
from io import StringIO
from bs4 import BeautifulSoup
from requests.sessions import Session
import datetime
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, IntegerType,LongType, StructType, StructField, TimestampType, StringType, BooleanType, DoubleType

# Suppress warnings
warnings.filterwarnings('ignore')

# API key and headers
KEY = dbutils.secrets.get(scope="goc_secrets", key="ODMToken")

HEADERS = {'Content-Type': 'application/json', 'Authorization': KEY,
           'Cookie': 'JSESSIONID=14e76e60-41e4-4a7e-af76-dc70a52ffb89'}

# Dictionary with dataset names and view IDs
collibra_dict = {
    "IP_Datasets": "429317f3-a0a9-49ce-a6f4-ab08d4c3dd8f",
    "IP": "473b782a-c863-49a3-8315-74a45834e876",
    "Orbis_Domains": "01924dde-1369-7b94-9a9e-15dd7a4f161e",
    "Data_Quality_Tests": "018f6172-fa1d-711d-929d-db01ab352257",
    "Data_Quality_Rules": "ea07eb48-fc28-448b-b43c-bf46aea73773",
    "Orbis_4_1": "0191bc4f-ba72-7c3d-a2f5-d2d5cf389d28",
    "Countries_and_Continents": "37504a5b-8a4c-4569-a58b-35711a3b736b",
    "States": "0192f66f-e9d1-7a60-b087-fd7ecfe8d876",
    "IP_Reference_Codes": "dc23b7c0-4915-4398-a230-faac3e91d87a",
    "Source_Codes_for_Datasets": "fd24fd39-eebf-4259-81c5-aa8692da0fca",
    "IP_Original_Source_By_Data_Type_Fourth_Party_Datasets": "0192f692-54fa-79b4-be53-743ab4c0714d",
    "Sources_used_by_IPs": "34d74655-4b26-42b2-a4a5-5405342eb10b"
}

# Create a session
session = Session()

def get_yesterday_start_end_unix_timestamps_milliseconds():
    # Get yesterday's date
    yesterday = datetime.now()
    date_str = yesterday.strftime('%Y-%m-%d')

    # Get the start of yesterday with time set to 00:00:00
    start_of_day = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    # Get the end of yesterday by setting time to 23:59:59 for the same day
    end_of_day = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Convert both to Unix timestamps in milliseconds
    start_of_day_unix_milliseconds = int(start_of_day.timestamp() * 1000)
    end_of_day_unix_milliseconds = int(end_of_day.timestamp() * 1000)

    return date_str, start_of_day_unix_milliseconds, end_of_day_unix_milliseconds

# Function to make API calls
def call_api(url_api, headers_api):
    """
    The aim of this function is to make API calls
    """
    response = session.get(url_api, headers=headers_api, verify=False)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

date_str, start_of_today, end_of_today = get_yesterday_start_end_unix_timestamps_milliseconds()
print(date_str)

# Iterate over the dictionary and process each dataset
for dataset_name, view_id in collibra_dict.items():
    print(f"Processing dataset: {dataset_name}")
    outputDir = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/{}/{}".format(dataset_name,date_str)
    try:
        os.mkdir(outputDir)
    except:
        pass
    # URLs for configuration and CSV data
    url_config = f'https://moodys.collibra.com/rest/2.0/outputModule/tableViewConfigs/viewId/{view_id}'
    url_view = f'https://moodys.collibra.com/rest/2.0/outputModule/export/csv?separator=%7C&viewId={view_id}'

    # Fetch and update configuration
    conf_table = call_api(url_config, HEADERS)
    if conf_table:
        conf_table['TableViewConfig']['displayLength'] = -1  # Unlimit the number of rows

        # Fetch CSV data
        resp_csv = requests.post(url_view, headers=HEADERS, verify=False, data=json.dumps(conf_table))
        if resp_csv.status_code == 200:
            # Fix encoding issues
            try:
                # Decode text correctly, removing problematic characters
                resp_csv_text = (
                    BeautifulSoup(resp_csv.content, 'html.parser')  # Parse raw content
                    .get_text()                                     # Get plain text
                    .replace("Â", "")                              # Remove 'Â'
                )
                
                # Load data into DataFrame
                df = pd.read_csv(
                    StringIO(resp_csv_text),
                    sep='|',
                    on_bad_lines='skip',   # Skip bad lines
                    encoding='utf-8',      # Change to 'utf-8' for proper encoding
                    engine='python'        # Use Python engine for robustness
                )
                
                # Clean and update DataFrame columns
                df.rename(columns=lambda x: x.strip().replace(" ", "_"), inplace=True)  # Strip and format column names
                df['dataset_Name'] = dataset_name
                df['snapshot_date'] = date_str
                
                # Convert to Spark DataFrame and write to Parquet
                sparkDF = spark.createDataFrame(df)
                
                # Handle specific dataset schema
                if dataset_name == "IP_Original_Source_By_Data_Type_Fourth_Party_Datasets":
                    sparkDF = sparkDF.withColumn("Asset_Type_IconCode", col("Asset_Type_IconCode").cast(StringType())) \
                                     .withColumn("Asset_Type_AcronymCode", col("Asset_Type_AcronymCode").cast(StringType())) \
                                     .withColumn("Type", col("Type").cast(StringType())) \
                                     .withColumn("Type_Id", col("Type_Id").cast(StringType()))
                
                sparkDF.write.mode("overwrite").parquet(outputDir)
            
            except (pd.errors.ParserError, ValueError) as e:
                print(f"Error parsing or processing CSV data for {dataset_name}: {e}")
            except Exception as e:
                print(f"Unexpected error for {dataset_name}: {e}")
        else:
            print(f"Failed to fetch CSV data for {dataset_name}: {resp_csv.status_code}")
    else:
        print(f"Failed to fetch configuration for {dataset_name}")
