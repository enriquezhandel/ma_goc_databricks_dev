# Databricks notebook source
import dlt
from pyspark.sql.functions import col, hour, date_sub, trunc, add_months, to_date, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

# COMMAND ----------

schemaActions = StructType([
    StructField("userSessionId", StringType(), True),
    StructField("elastic_id", StringType(), True),
    StructField("apdexCategory", StringType(), True),
    StructField("application", StringType(), True),
    StructField("cdnBusyTime", LongType(), True),
    StructField("cdnResources", LongType(), True),
    StructField("cumulativeLayoutShift", DoubleType(), True),
    StructField("documentInteractiveTime", LongType(), True),
    StructField("domCompleteTime", LongType(), True),
    StructField("domContentLoadedTime", LongType(), True),
    StructField("duration", LongType(), True),
    StructField("endTime", LongType(), True),
    StructField("endTime_utc", StringType(), True),
    StructField("firstInputDelay", LongType(), True),
    StructField("firstPartyBusyTime", LongType(), True),
    StructField("firstPartyResources", LongType(), True),
    StructField("frontendTime", LongType(), True),
    StructField("internalApplicationId", StringType(), True),
    StructField("internalKeyUserActionId", StringType(), True),
    StructField("javascriptErrorCount", LongType(), True),
    StructField("keyUserAction", BooleanType(), True),
    StructField("largestContentfulPaint", LongType(), True),
    StructField("loadEventEnd", StringType(), True),
    StructField("loadEventStart", StringType(), True),
    StructField("longProperties", StringType(), True),
    StructField("name", StringType(), True),
    StructField("navigationStart", LongType(), True),
    StructField("networkTime", LongType(), True),
    StructField("requestErrorCount", LongType(), True),
    StructField("requestStart", LongType(), True),
    StructField("responseEnd", LongType(), True),
    StructField("responseStart", LongType(), True),
    StructField("serverTime", LongType(), True),
    StructField("speedIndex", LongType(), True),
    StructField("startTime", LongType(), True),
    StructField("startTime_utc", StringType(), True),
    StructField("stringProperties", StringType(), True),
    StructField("targetUrl", StringType(), True),
    StructField("thirdPartyBusyTime", LongType(), True),
    StructField("thirdPartyResources", LongType(), True),
    StructField("type", StringType(), True),
    StructField("visuallyCompleteTime", LongType(), True),
    StructField("threadname", StringType(), True),
    StructField("reportbooksection", StringType(), True),
    StructField("searchby", StringType(), True),
    StructField("accountid", StringType(), True),
    StructField("accountname", StringType(), True),
    StructField("snapshot_date", StringType(), True)
])
@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="Raw_User_Actions"
)
def RawUserActions():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/RawUserActions/*"
    RawUserActions = spark.readStream.format("parquet").schema(schemaActions).option("path", path).load()
    # RawUserActions.createOrReplaceTempView("RawUserActions")
    # display(RawUserActions.limit(10))
    return RawUserActions

# COMMAND ----------

schemaSessions = StructType([
    StructField("elastic_id", StringType(), True),
    StructField("userSessionId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("stringProperties", StringType(), True),
    StructField("startTime", LongType(), True),
    StructField("startTime_utc", StringType(), True),
    StructField("endTime", LongType(), True),
    StructField("endTime_utc", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("duration", LongType(), True),
    StructField("browserFamily", StringType(), True),
    StructField("browserMajorVersion", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("hasCrash", BooleanType(), True),
    StructField("hasError", BooleanType(), True),
    StructField("numberOfRageClicks", LongType(), True),
    StructField("numberOfRageTaps", LongType(), True),
    StructField("totalErrorCount", LongType(), True),
    StructField("userActionCount", LongType(), True),
    StructField("userExperienceScore", StringType(), True),
    StructField("userType", StringType(), True),
    StructField("snapshot_date", StringType(), True)
])

@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="Raw_User_Sessions"
)
def RawUserSessions():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/RawUserSessions/*"
    RawUserSessions = spark.readStream.format("parquet").schema(schemaSessions).option("path", path).load()
    # RawUserSession.createOrReplaceTempView("RawUserSessions")
    # display(RawUserSession.limit(10))
    return RawUserSessions
