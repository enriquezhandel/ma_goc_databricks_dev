# Databricks notebook source
import datetime
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F

# COMMAND ----------

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

# COMMAND ----------

date_str, start_of_today, end_of_today = get_yesterday_start_end_unix_timestamps_milliseconds()
print(date_str)
try:
    os.mkdir("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawStatusPageSubscriptions/{}".format(date_str))
except:
    pass

# COMMAND ----------

# Retrieve secrets on SQL Server
sqlServer = dbutils.secrets.get(scope="goc_secrets", key="sqlFINServer")
jdbc_username = dbutils.secrets.get(scope="goc_secrets", key="userFinStats")
jdbc_password = dbutils.secrets.get(scope="goc_secrets", key="passFinStats")

# MVC CONNECTION
sqlDB = dbutils.secrets.get(scope="goc_secrets", key="sqlFinDB")
tableView = dbutils.secrets.get(scope="goc_secrets", key="sqlFinView")

# COMMAND ----------

def JDBC_Query_connection(server, db, username, password, query):
    # Set up the JDBC connection properties
    jdbc_url = (f"jdbc:{server};"
                f"databaseName={db};"
                "encrypt=true;"
                "trustServerCertificate=true")
    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    # Read data query from SQL Server view
    df = spark.read.format("jdbc").option("url", jdbc_url).option("query", query).options(**connection_properties).load()
    return df

# COMMAND ----------

query = f"""
SELECT
       [UserName]
      ,[Email]
      ,[FirstName]
      ,[LastName]
      ,[active user]
      ,[CreatedDate]
      ,[Expires]
      ,[LastLogin]
      ,[Company ID Source]
      ,[Company Name Source]
      ,[UserId]
      ,[UserType]
      ,[prodSubscrId]
      ,[prodStatsId]
      ,[productName]
      ,[prodExpires]
      ,[ProductLastLogin]
      ,[AccountNoPromo]
      ,[UserNoPromo]
      ,[fDashBoard]
      ,[Region]
      ,[tag]
      ,[dtExtract]
      ,CONVERT(date, GETDATE()) as snapshotdate_db
  FROM {tableView}
"""
FinDf = JDBC_Query_connection(sqlServer, sqlDB, jdbc_username, jdbc_password, query)
display(FinDf.limit(10))

# COMMAND ----------

dfCSV = spark.read.csv("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/auxiliar_docs/StatusPage_ProductsNames.csv", header=True, inferSchema=True)
display(dfCSV)

# COMMAND ----------

CleanFinDF = FinDf.join(dfCSV, FinDf.productName == dfCSV.productName, "left") \
                  .select(FinDf.Email.alias("Email"),
                          FinDf.FirstName.alias("FirstName"),
                          FinDf.LastName.alias("LastName"),
                          FinDf.LastLogin.alias("LastLogin"),
                          FinDf.prodSubscrId.alias("prodSubscrId"),
                          FinDf.prodStatsId.alias("prodStatsId"),
                          FinDf.productName.alias("productName"),
                          FinDf.ProductLastLogin.alias("ProductLastLogin"),
                          FinDf.fDashBoard.alias("fDashBoard"),
                          FinDf.snapshotdate_db.alias("snapshot_date"),
                          dfCSV.StatusPageName.alias("StatusPageName")) \
                  .orderBy("Email")

## Generate logic for outputting dataset 
# display(CleanFinDF.limit(1000))
outputAlerts = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawStatusPageSubscriptions/{}/".format(date_str)
CleanFinDF.write.mode("overwrite").parquet(outputAlerts)
