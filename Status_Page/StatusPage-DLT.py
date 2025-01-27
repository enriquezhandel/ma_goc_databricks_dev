# Databricks notebook source
import dlt
from pyspark.sql.types import DateType, IntegerType,LongType, StructType, StructField, TimestampType, StringType

# COMMAND ----------

schemaStatusPage = StructType([
    StructField("Email", StringType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("LastLogin", DateType(), True),
    StructField("prodSubscrId", IntegerType(), True),
    StructField("prodStatsId", IntegerType(), True),
    StructField("productName", StringType(), True),
    StructField("ProductLastLogin", DateType(), True),
    StructField("fDashBoard", IntegerType(), True),
    StructField("snapshot_date", DateType(), True),
    StructField("StatusPageName", StringType(), True)
])

@dlt.table(
    comment="This stream the Status Page raw format",
    name="raw_status_page"
)
def raw_status_page():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawStatusPageSubscriptions/*"
    raw_status_page = spark.readStream.format("parquet").schema(schemaStatusPage).option("path", path).load()
    return raw_status_page
