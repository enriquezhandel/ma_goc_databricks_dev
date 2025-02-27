# Databricks notebook source
import dlt
from pyspark.sql.types import DateType, IntegerType,LongType, StructType, StructField, TimestampType, StringType, BooleanType, DoubleType
from pyspark.sql.functions import col

# COMMAND ----------

import dlt

@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="rawContinentsandCountries"
)
def rawContinents_and_Countries():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawContinents_and_Countries/*"
    df = spark.read.parquet(path)
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

import dlt

@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="rawExternal_MarketDataVendorList"
)
def rawExternalMarketDataVendorList():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawExternalMarketDataVendorList/*"
    df = spark.read.parquet(path)
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

@dlt.table(
    comment="This is just a view of the original source by data type for fourth party datasets",
    name="rawIPOriginal_Source_By_Data_Type_Fourth_Party_Datasets"
)

def rawIP_Original_Source_By_Data_Type_Fourth_Party_Datasets():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawIP_Original_Source_By_Data_Type_Fourth_Party_Datasets/*"
    df = spark.read.parquet(path)
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

@dlt.table(
    comment="This is a view of the raw IP reference codes",
    name="rawIPReference_Codes"
)
def rawIP_Reference_Codes():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawIP_Reference_Codes/*"
    df = spark.read.parquet(path)
    # display(df.limit(10))
    # df.printSchema()
    return df


# COMMAND ----------

@dlt.table(
    comment="This is a view of the datasets provided by vendors",
    name="raw_ListBvDDatasetsProvidedVendors"
)
def rawListBvDDatasetsProvidedVendors():
    df = spark.read.parquet("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawListBvDDatasetsProvidedVendors/*")
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

@dlt.table(
    comment="This is a view of the Orbis Data Quality Rulebook",
    name="raw_OrbisDataQualityRulebook"
)
def rawOrbisDataQualityRulebook():
    df = spark.read.parquet("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawOrbisDataQualityRulebook/*")
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

@dlt.table(
    comment="This is a view of the Orbis Data Quality Tests",
    name="raw_OrbisDataQualityTests"
)
def rawOrbisDataQualityTests():
    df = spark.read.parquet("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawOrbisData_Quality_Tests/*")
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

@dlt.table(
    comment="This is a view of the Orbis Domains data",
    name="raw_OrbisDomains"
)
def rawOrbisDomains():
    df = spark.read.parquet("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawOrbisDomains/*")
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

@dlt.table(
    comment="This is a view of the sources used by IPs",
    name="raw_SourcesUsedByIPs"
)
def rawSourcesUsedByIPs():
    df = spark.read.parquet("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawSources_used_by_IPs/*")
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

@dlt.table(
    comment="This is a view of the rawOrbis_4_1 data",
    name="raw_Orbis_4_1"
)
def rawOrbis_4_1_specific_date():
    df = spark.read.parquet("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawOrbis_4_1/*")
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

@dlt.table(
    comment="This is a view of the rawSource_Codes_for_Datasets",
    name="raw_Source_Codes_for_Datasets"
)
def rawSource_Codes_for_Datasets():
    df = spark.read.parquet("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawSource_Codes_for_Datasets/*")
    # display(df.limit(10))
    # df.printSchema()
    return df

# COMMAND ----------

@dlt.table(
    comment="This is a view of the rawStates data",
    name="rawStates_City"
)
def rawStates():
    df = spark.read.parquet("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawStates/*")
    # display(df.limit(10))
    # df.printSchema()
    return df
