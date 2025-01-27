# Databricks notebook source
import dlt
from pyspark.sql.types import DateType, IntegerType,LongType, StructType, StructField, TimestampType, StringType

# COMMAND ----------

schemaTracking = StructType([
    StructField("TrackId", StringType(), True),
    StructField("CreationDate", TimestampType(), True),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("UserId", IntegerType(), True),
    StructField("ModuleId", StringType(), True),
    StructField("ProductId", StringType(), True),
    StructField("UserName", StringType(), True),
    StructField("UserClientName", StringType(), True),
    StructField("WaitTime", IntegerType(), True),
    StructField("ExecutionTime", IntegerType(), True)
])

@dlt.table(
    comment="This stream the Risk Assesment raw format",
    name="raw_comcatv2_moduleTracking"
)
def RawRA():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2ModuleTracking/*"
    raw_comcatv2_moduleTracking = spark.readStream.format("parquet").schema(schemaTracking).option("path", path).load()
    # raw_comcatv2_moduleTracking.createOrReplaceTempView("raw_comcatv2_moduleTracking")
    # display(raw_comcatv2_moduleTracking.limit(10))
    return raw_comcatv2_moduleTracking

# COMMAND ----------

schemaTracking = StructType([
    StructField("TrackId", StringType(), True),
    StructField("CreationDate", TimestampType(), True),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("UserId", IntegerType(), True),
    StructField("ModuleId", StringType(), True),
    StructField("ProductId", StringType(), True),
    StructField("UserName", StringType(), True),
    StructField("UserClientName", StringType(), True),
    StructField("WaitTime", IntegerType(), True),
    StructField("ExecutionTime", IntegerType(), True)
])

@dlt.table(
    comment="This stream the Risk Assesment raw format",
    name="raw_orbisAWS_moduleTracking"
)
def RawRA():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawOrbisAWSModuleTracking/*"
    raw_orbisAWS_moduleTracking = spark.readStream.format("parquet").schema(schemaTracking).option("path", path).load()
    # raw_comcatv2_moduleTracking.createOrReplaceTempView("raw_comcatv2_moduleTracking")
    # display(raw_comcatv2_moduleTracking.limit(10))
    return raw_orbisAWS_moduleTracking

# COMMAND ----------

schemaRA = StructType([
    StructField("Id", LongType(), True),
    StructField("UserId", IntegerType(), True),
    StructField("ProcessId", StringType(), True),
    StructField("SyncStatus", IntegerType(), True),
    StructField("ServerName", StringType(), True),
    StructField("DebugServerName", StringType(), True),
    StructField("StartedOn", TimestampType(), True),
    StructField("ItemType", IntegerType(), True),
    StructField("ParentProcessId", StringType(), True),
    StructField("FinishedOn", TimestampType(), True),
    StructField("ProductId", IntegerType(), True),
    StructField("Priority", IntegerType(), True),
    StructField("TaskType", IntegerType(), True),
    StructField("EntityType", IntegerType(), True),
    StructField("CarryOver", IntegerType(), True),
    StructField("RequestedOn", TimestampType(), True),
    StructField("Context", StringType(), True),
    StructField("NextCheckDate", TimestampType(), True),
    StructField("RemainingAttempt", IntegerType(), True),
    StructField("DuplicateCheck", StringType(), True),
    StructField("SharingId", IntegerType(), True),
    StructField("BulkId", StringType(), True),
    StructField("execution_seconds", LongType(), True),
    StructField("wait_seconds", LongType(), True)
])
@dlt.table(
    comment="This stream the Risk Assesment raw format",
    name="raw_comcatv2_riskAssesments"
)
def RawRA():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2RiskAssesment/*"
    raw_comcatv2_riskAssesment = spark.readStream.format("parquet").schema(schemaRA).option("path", path).load()
    # raw_comcatv2_riskAssesment.createOrReplaceTempView("raw_comcatv2_riskAssesment")
    # display(raw_comcatv2_riskAssesment.limit(10))
    return raw_comcatv2_riskAssesment

# COMMAND ----------

schemaAlert = StructType([
    StructField("SharingId", IntegerType(), True),
    StructField("AlertCriteria", IntegerType(), True),
    StructField("Count", IntegerType(), True),
    StructField("AlertDate", TimestampType(), True),
    StructField("snapshotdate", DateType(), True)
])

@dlt.table(
    comment="This stream the Alert raw format",
    name="raw_comcatv2_alerts"
)
def RawAlert():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawComCatV2Alerts/*"
    raw_comcatv2_alerts = spark.readStream.format("parquet").schema(schemaAlert).option("path", path).load()
    # raw_comcatv2_alerts.createOrReplaceTempView("raw_comcatv2_alerts")
    # display(raw_comcatv2_alerts.limit(10))
    return raw_comcatv2_alerts
