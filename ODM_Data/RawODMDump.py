# Databricks notebook source
import requests, json
import warnings
from pyspark.sql.functions import col, explode, size, trim, current_date
import logging
import datetime
from datetime import datetime, timedelta
import os

# Suppress warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API key and headers
KEY = dbutils.secrets.get(scope="goc_secrets", key="ODMToken")

HEADERS = {'Content-Type': 'application/json', 'Authorization': KEY,
           'Cookie': 'JSESSIONID=14e76e60-41e4-4a7e-af76-dc70a52ffb89'}

# Dictionary with dataset names and view IDs
collibra_dict = {
    "rawContinents_and_Countries": "019519ae-7f7e-70ed-a0aa-ea4377997cab",
    "rawExternalMarketDataVendorList": "019519bb-a70e-7139-99d5-1189c9b895ea",
    "rawIP_Original_Source_By_Data_Type_Fourth_Party_Datasets": "019519bc-788b-7b26-9cba-8ac34c3c68fd",
    "rawIP_Reference_Codes": "019519bc-ff49-7531-bc9b-565377865aaa",
    "rawListBvDDatasetsProvidedVendors": "019519bd-6d51-7998-a6f8-0d458e946d1d",
    "rawOrbisDataQualityRulebook": "019519be-275f-71f1-a5fa-4ac43bed4a1c",
    "rawOrbisData_Quality_Tests": "019519be-b2eb-7201-99c3-57d6af9b46a6",
    "rawOrbisDomains": "019519bf-0b9f-73d7-88b3-b4bb4d3110a8",
    "rawSources_used_by_IPs": "019519c0-3e99-7ff0-ad32-ace8e7468b24",
    "rawOrbis_4_1": "019519bf-6b32-770e-a334-2e92d1b55379",
    "rawSource_Codes_for_Datasets": "019519bf-d757-717a-a0ca-f9e5094e2d5d",
    "rawStates": "019519c0-a347-7aaf-8bff-5eef896aa1d9"
}

# Create a session
session = requests.Session()

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
    try:
        response = session.get(url_api, headers=headers_api, verify=False)
        response.raise_for_status()
        return json.loads(response.text)
    except requests.exceptions.RequestException as e:
        logging.error(f"API call failed: {e}")
        return None

def explode_array_columns(df):
    final_df = df
    exploded_df = []  
    for colName, colType in df.dtypes:
        if colType.startswith("array"):
            colReview = df.select("*", colName)
            colReview = colReview.withColumn("is_empty", size(col(colName)) == 0)

            if colReview.filter(col("is_empty") == False).count() > 0:
                logging.info(f"Exploding column: {colName}")
                df_exploded = colReview.select("Id", explode(col(colName)).alias(f"{colName}_exploded"))
                dfFix = df_exploded.select("*", f"{colName}_exploded.*").drop(f"{colName}_exploded")
                exploded_df.append(dfFix)
            else:
                logging.info(f"Skipping empty column: {colName}")
                continue

    return final_df, exploded_df

def join_exploded_data(final_df, exploded_df):
    # Initialize idAll with the "Id" column
    idAll = final_df.select("Id").distinct()  # Ensure unique IDs

    # Iterate and join while renaming columns uniquely
    for idx, df in enumerate(exploded_df):
        # Ensure `df` has distinct "Id" values to avoid duplication issues
        df = df.dropDuplicates(["Id"])

        # Rename columns to avoid overwriting
        renamed_df = df.select([col("Id")] + [col(c).alias(f"{c}_{idx}") for c in df.columns if c != "Id"])

        # Perform a LEFT join to keep existing data while adding new columns
        idAll = idAll.join(renamed_df, "Id", "left")

    return idAll

date_str, start_of_today, end_of_today = get_yesterday_start_end_unix_timestamps_milliseconds()
print(date_str)

# Process each dataset in the dictionary
for dataset_name, view_id in collibra_dict.items():
    logging.info(f"Processing dataset: {dataset_name}")

    outputDir = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/{}/{}".format(dataset_name,date_str)
    try:
        os.mkdir(outputDir)
    except:
        pass

    # URLs for configuration and JSON data
    url_config = f'https://moodys.collibra.com/rest/2.0/outputModule/tableViewConfigs/viewId/{view_id}'
    url_view = f'https://moodys.collibra.com/rest/2.0/outputModule/export/json?separator&viewId={view_id}'

    # Fetch and update configuration
    conf_table = call_api(url_config, HEADERS)
    if conf_table:
        conf_table['TableViewConfig']['displayLength'] = -1  # Unlimit the number of rows

        # Fetch JSON data
        try:
            resp_json = requests.post(url_view, headers=HEADERS, verify=False, data=json.dumps(conf_table))
            resp_json.raise_for_status()
            json_content = resp_json.content.decode('utf-8')
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch JSON data: {e}")
            json_content = None

        if json_content:
            # Load JSON data into Spark DataFrame
            df = spark.read.json(spark.sparkContext.parallelize([json_content]))

            # Explode and flatten the JSON data
            exploded_df = df.select(explode(col("aaData")).alias("Data"))
            flattened_df = exploded_df.select("Data.*")
            flattened_df = flattened_df.withColumn("Id", trim(col("Id")))
            flattened_df.createOrReplaceTempView("dfView")

            final_df, cleanExploded_df = explode_array_columns(flattened_df)
            final_df = final_df.withColumn("Id", trim(col("Id")))
            cleanExploded_df = [df.withColumn("Id", trim(col("Id"))) for df in cleanExploded_df]
            idAllColumns = join_exploded_data(final_df, cleanExploded_df)
            idAllColumns.createOrReplaceTempView("dfExploded")

            sql ="""
            SELECT 
                 dv.* 
                ,de.*
            FROM dfView dv
                LEFT JOIN dfExploded de 
                    ON dv.Id = de.Id
            """
            finalDFALL = spark.sql(sql)

            # Check for missing IDs after the join
            null_check = finalDFALL.filter(col("dv.Id").isNull()).count()
            logging.info(f"Number of null IDs in final result: {null_check}")

            # Display the final DataFrame
            finalDFALL = finalDFALL.drop(col("de.Id"))
            # display(finalDFALL)

            # Check the row count after the join
            row_count = finalDFALL.count()
            logging.info(f"Final Row Count after SQL Join: {row_count}")
            # finalDFALL.printSchema()
            # Add a column for the current date named snapshot_date
            finalDFALL = finalDFALL.withColumn("snapshot_date", current_date())
            
            # Writing dataset to Parquet
            finalDFALL.write.mode("overwrite").parquet(outputDir)

        else:
            logging.error("No JSON content to process.")
    else:
        logging.error("Failed to fetch configuration.")
