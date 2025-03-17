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
try:
    os.mkdir("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2RAModel/{}".format(date_str))
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
tableCustomFields = dbutils.secrets.get(scope="goc_secrets", key="sqlTableCustomFields")
tableCustomEntitiesType = dbutils.secrets.get(scope="goc_secrets", key="sqlTableCustomEntities")
tableCustomEntities = dbutils.secrets.get(scope="goc_secrets", key="sqlTableCustom_Entities")
tableCustomString = dbutils.secrets.get(scope="goc_secrets", key="sqlTableCustomString")
tableFinUsers = dbutils.secrets.get(scope="goc_secrets", key="sqlTableFinUsers")
tableFinAccounts = dbutils.secrets.get(scope="goc_secrets", key="sqlTableFinAccounts")

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

# COMMAND ----------

from pyspark.sql.functions import broadcast, when, expr, current_date, lower, col
from pyspark.sql import SparkSession

class DataProcessor:
    def __init__(self, sql_server, sql_db, username, password):
        self.sql_server = sql_server
        self.sql_db = sql_db
        self.username = username
        self.password = password
        self.spark = SparkSession.builder.appName("DataProcessor").getOrCreate()

    def execute_jdbc_query(self, query):
        return JDBC_Query_connection(self.sql_server, self.sql_db, self.username, self.password, query)

    def create_temp_view(self, df, view_name):
        df.createOrReplaceTempView(view_name)

    def add_custom_entity_type(self):
        query = f"""
        SELECT * FROM {tableCustomEntitiesType} with (nolock)
        """
        df = self.execute_jdbc_query(query)
        self.create_temp_view(df, "customEntityTypes")

    def add_custom_fields(self):
        query = f"""
        SELECT [Id], [EntityType], [Name], [DataType], [LookupType], [Attributes], [ForeignEntityType]
        FROM {tableCustomFields} with (nolock)
        """
        df = self.execute_jdbc_query(query)
        self.create_temp_view(df, "customFields")

    def add_custom_entities(self):
        query = f"""
            SELECT 
                *
            FROM {tableCustomEntities} with (nolock)
        """
        df = self.execute_jdbc_query(query)
        self.create_temp_view(df, "customEntities")


    def add_ra_model(self):
        query = f"""
        SELECT 
            ra.ProcessId, ra.RequestedOn, ra.UserId, ra.SharingId, ra.ProductId,
            CASE ra.TaskType 
                WHEN 0 THEN 'Score' 
                WHEN 2 THEN 'RecalculateScore' 
                WHEN 3 THEN 'FullScore' 
                WHEN 4 THEN 'FullRecalculateScore' 
                WHEN 5 THEN 'FullScoreQueueing' 
                WHEN 6 THEN 'FullRecalculateScoreQueueing' 
                WHEN 7 THEN 'StartOrRefresh' 
                WHEN 8 THEN 'SingleFullScore' 
                ELSE convert(varchar(max), ra.TaskType) 
            END AS TaskType,
            datediff(ms, ra.RequestedOn, ra.StartedOn) as QueueDurationInMS,
            datediff(ms, ra.StartedOn, ra.FinishedOn) as ComputeDurationInMS,
            datediff(ms, ra.RequestedOn, ra.FinishedOn) as TotalDurationInMS
        FROM {tableRA} ra with (nolock)
        WHERE ra.itemtype = 1 AND ra.SyncStatus = 3 AND ra.startedOn >= CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE())-1, 0) AS DATETIME)
            AND ra.startedOn < CAST(DATEADD(DAY, DATEDIFF(DAY, 0, GETUTCDATE()), 0) AS DATETIME)
        """
        df = self.execute_jdbc_query(query)
        self.create_temp_view(df, "dfRAModel")
    
    def add_custom_strings(self):
        query = f"""
            SELECT 
                 [Id]
                ,[EntityId]
                ,[FieldId]
                ,[Value]
            FROM {tableCustomString} cv with (nolock)
        """
        df = self.execute_jdbc_query(query)
        self.create_temp_view(df, "dfCustomString")

    def modelIDRA(self):
        custom_fields_df = self.spark.table("customFields").cache()
        custom_entities_df = self.spark.table("customEntities").cache()
        custom_string_values_df = self.spark.table("dfCustomString").cache()
        risk_assessments_df = self.spark.table("dfRAModel").cache()
        custom_entity_types_df = self.spark.table("customEntityTypes").cache()

        # Create baseRAID DataFrame
        base_raid_df = custom_fields_df.join(
            custom_entity_types_df,
            lower(custom_fields_df.EntityType) == lower(custom_entity_types_df.Id)
        ).filter(
            (custom_fields_df.Name == 'CF_RA_Id') & 
            (custom_entity_types_df.Name == 'CFE_RA') & 
            (custom_entity_types_df.DatabaseContext == 'Companies')
        ).select(custom_fields_df.Id)

        # Collect baseRAID IDs to use in the join condition
        base_raid_ids = [row.Id for row in base_raid_df.collect()]

        # Perform the main query with broadcast joins
        result_df = risk_assessments_df.join(
            broadcast(custom_string_values_df),
            (custom_string_values_df.FieldId.isin(base_raid_ids)) & 
            (lower(custom_string_values_df.Value) == lower(risk_assessments_df.ProcessId))
        ).join(
            broadcast(custom_entities_df.alias("ae")),
            lower(custom_string_values_df.EntityId) == lower(col("ae.Id"))
        ).join(
            broadcast(custom_entities_df.alias("me")),
            lower(col("ae.ParentId")) == lower(col("me.Id"))
        ).join(
            broadcast(custom_string_values_df.alias("mv")),
            lower(col("me.Id")) == lower(col("mv.EntityId"))
        ).select(
            risk_assessments_df.ProcessId.alias("RAid"),
            risk_assessments_df.SharingId,
            col("mv.Value").alias("modelID"),
            col("mv.Id"),
            col("mv.EntityId"),
            col("mv.FieldId"),
            col("mv.Value")
        ).orderBy(risk_assessments_df.ProcessId.desc())
        
        self.create_temp_view(result_df, "dfModelRA") 

    def add_fin_users(processor):
        query = f"""
        SELECT DISTINCT [Id], [AccountId], [UserName]
        FROM {tableFinUsers} with (nolock)
        WHERE userName IS NOT NULL
        """
        df = processor.execute_jdbc_query(query)
        processor.create_temp_view(df, "usersClean")

    def add_fin_accounts(processor):
        query = f"""
        SELECT [Id], [PrimaryAccountId], [AccountHierarchyId], [Active], [AccountName], [CompanyName]
        FROM {tableFinAccounts} with (nolock)
        """
        df = processor.execute_jdbc_query(query)
        processor.create_temp_view(df, "accountsClean")

    def create_user_mapping(processor):
        query = """
        SELECT uc.userName, uc.Id, ac.AccountName
        FROM usersClean uc
        INNER JOIN accountsClean ac ON uc.AccountId = ac.Id
        """
        df = spark.sql(query)
        processor.create_temp_view(df, "dfUserMapping")

    def create_final_view(processor):
        # Load the DataFrames from the temporary views
        df_ra_model = spark.table("dfRAModel").cache()
        df_user_mapping = spark.table("dfUserMapping").cache()
        df_custom_string_clean = spark.table("dfModelRA").cache()

        # Perform the joins using DataFrame API and broadcast the smaller DataFrames
        df_final = df_ra_model.join(
            broadcast(df_user_mapping),
            df_ra_model.UserId == df_user_mapping.Id,
            "left"
        ).join(
            broadcast(df_custom_string_clean),
            lower(df_custom_string_clean.RAid) == lower(df_ra_model.ProcessId),
            "left"
        ).select(
            df_ra_model.ProcessId.alias("RAid"),
            df_ra_model.RequestedOn,
            df_user_mapping.userName,
            df_user_mapping.AccountName,
            df_ra_model.UserId,
            df_ra_model.SharingId,
            df_ra_model.ProductId,
            df_ra_model.TaskType,
            df_ra_model.QueueDurationInMS,
            df_ra_model.ComputeDurationInMS,
            df_ra_model.TotalDurationInMS,
            df_custom_string_clean.Value.alias("modelID")
        ).withColumn("snapshot_date", current_date()-1)

        # Create or replace the temporary view with the final DataFrame
        outputRAModel = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2RAModel/{}/".format(date_str)
        display(df_final.limit(10))
        df_final.write.mode("overwrite").parquet(outputRAModel)



processor = DataProcessor(sqlServer, sqlDBCatalystMVC, jdbc_username, jdbc_password)
processor.add_custom_fields()
processor.add_custom_entity_type()
processor.add_custom_entities()
processor.add_custom_strings()
processor.add_ra_model()
processor.add_fin_users()
processor.add_fin_accounts()
processor.create_user_mapping()
processor.modelIDRA()
processor.create_final_view()
