# Databricks notebook source

import datetime
from datetime import datetime, timedelta
import os


# COMMAND ----------

def get_yesterday_start_end_unix_timestamps_milliseconds():
    # Get yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
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
    os.mkdir("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2Alerts/{}".format(date_str))
except:
    pass
try:
    os.mkdir("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2RiskAssesment/{}".format(date_str))
except:
    pass
try:
    os.mkdir("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2ModuleTracking/{}".format(date_str))
except:
    pass
try:
    os.mkdir("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawOrbisAWSModuleTracking/{}".format(date_str))
except:
    pass

# COMMAND ----------

# Retrieve secrets on SQL Server
sqlServer = dbutils.secrets.get(scope="goc_secrets", key="sqlQIFServer")
jdbc_username = dbutils.secrets.get(scope="goc_secrets", key="sqlQIFUser")
jdbc_password = dbutils.secrets.get(scope="goc_secrets", key="sqlQIFPass")

# MVC CONNECTION
sqlDBCatalystMVC = dbutils.secrets.get(scope="goc_secrets", key="sqlDBCatalystMVC")
tableRA = dbutils.secrets.get(scope="goc_secrets", key="sqlTableCatalystRA")
tableAlerts = dbutils.secrets.get(scope="goc_secrets", key="sqlTableCatalystAlerts")
tableTBLTracks = dbutils.secrets.get(scope="goc_secrets", key="sqlTableTBLTracks")
tableTBLModules = dbutils.secrets.get(scope="goc_secrets", key="sqlTableTBLModules")
tableTBLUsers = dbutils.secrets.get(scope="goc_secrets", key="sqlTableTBLUsers")


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

queryModuleTracking =f"""
SELECT
    t.TrackId,
    t.CreationDate,
    t.StartDate,
    t.EndDate,
    t.UserId,
	m.ModuleId,
	m.ProductId,
	u.UserName,
    u.UserClientName,
    DATEDIFF(second, t.CreationDate, t.StartDate) AS WaitTime,
    DATEDIFF(second, t.StartDate, t.EndDate) AS ExecutionTime 
FROM
    {tableTBLTracks} t
    JOIN {tableTBLModules} m
		ON t.InstanceId = m.InstanceId
	LEFT JOIN {tableTBLUsers} u
		ON t.UserId= u.UserId
WHERE
    t.Status = 0
    AND m.ProductId IN ('ComplianceCatalyst4','CreditCatalyst4','ProcurementCatalyst4')
    AND m.ModuleId = 'DELAYEDEXPORTS'
   	AND t.EndDate >= CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE())-1, 0) AS DATETIME)
    AND t.EndDate < CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE()), 0) AS DATETIME)
"""
dfModuleTracking = JDBC_Query_connection(sqlServer, sqlDBCatalystMVC, jdbc_username, jdbc_password, queryModuleTracking)
outputModuleTracking = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2ModuleTracking/{}/".format(date_str)
dfModuleTracking.write.mode("overwrite").parquet(outputModuleTracking)
# display(dfModuleTracking.limit(10))

# COMMAND ----------

queryOrbisModuleTracking =f"""
SELECT
    t.TrackId,
    t.CreationDate,
    t.StartDate,
    t.EndDate,
    t.UserId,
	m.ModuleId,
	m.ProductId,
	u.UserName,
    u.UserClientName,
    DATEDIFF(second, t.CreationDate, t.StartDate) AS WaitTime,
    DATEDIFF(second, t.StartDate, t.EndDate) AS ExecutionTime 
FROM
    {tableTBLTracks} t
    JOIN {tableTBLModules} m
		ON t.InstanceId = m.InstanceId
	LEFT JOIN {tableTBLUsers} u
		ON t.UserId= u.UserId
WHERE
    t.Status = 0
    AND m.ProductId IN ('Orbis')
    AND m.ModuleId = 'DELAYEDEXPORTS'
   	AND t.EndDate >= CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE())-1, 0) AS DATETIME)
    AND t.EndDate < CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE()), 0) AS DATETIME)
"""
dfOrbisModuleTracking = JDBC_Query_connection(sqlServer, sqlDBCatalystMVC, jdbc_username, jdbc_password, queryOrbisModuleTracking)
outputModuleTracking = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawOrbisAWSModuleTracking/{}/".format(date_str)
dfOrbisModuleTracking.write.mode("overwrite").parquet(outputModuleTracking)
# display(dfModuleTracking.limit(10))

# COMMAND ----------

queryRA =f""" 
SELECT DISTINCT
	 [Id]
    ,[UserId]
    ,[ProcessId]
    ,[SyncStatus]
    ,[ServerName]
    ,[DebugServerName]
    ,[StartedOn]
    ,[ItemType]
    ,[ParentProcessId]
    ,[FinishedOn]
    ,[ProductId]
    ,[Priority]
    ,[TaskType]
    ,[EntityType]
    ,[CarryOver]
    ,[RequestedOn]
    ,[Context]
    ,[NextCheckDate]
    ,[RemainingAttempt]
    ,[DuplicateCheck]
    ,[SharingId]
    ,[BulkId]
	,CAST(DATEDIFF(second, startedOn, finishedon) as bigint) as 'execution_seconds'
	,CAST(DATEDIFF(second, RequestedOn, startedOn) as bigint) as 'wait_seconds'
FROM {tableRA} with (nolock)
	WHERE 
		productid=171
	and tasktype = 0
	AND SharingId IS NOT NULL
	AND startedOn IS NOT NULL
	AND startedOn >= CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE())-1, 0) AS DATETIME)
    AND startedOn < CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE()), 0) AS DATETIME)
"""
dfRA = JDBC_Query_connection(sqlServer, sqlDBCatalystMVC, jdbc_username, jdbc_password, queryRA)
outputRA = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2RiskAssesment/{}/".format(date_str)
dfRA.write.mode("overwrite").parquet(outputRA)
# display(dfRA.limit(10))

# COMMAND ----------

## Alert Query ##  
queryAlerts =f"""
SELECT
	 SharingId
	, AlertCriteria
	, count(*) as Count
	, AlertDate
	, CONVERT(date, GETDATE()) as snapshotdate
FROM {tableAlerts} with (nolock)
WHERE
	ProductId = 171
AND Status in (1,2,9)
AND AlertDate >= CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE())-1, 0) AS DATETIME)
AND AlertDate < CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE()), 0) AS DATETIME)
GROUP BY
  SharingId
, AlertCriteria
, AlertDate
"""
dfAlerts = JDBC_Query_connection(sqlServer, sqlDBCatalystMVC, jdbc_username, jdbc_password, queryAlerts)
outputAlerts = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2Alerts/{}/".format(date_str)
dfAlerts.write.mode("overwrite").parquet(outputAlerts)
# display(dfAlerts.limit(10))
