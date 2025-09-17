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

# import dlt
# from pyspark.sql.types import StructType, StructField, StringType, DateType

# spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# @dlt.table(
#     comment="This is just a view of the original source by data type for fourth party datasets",
#     name="rawIPOriginal_Source_By_Data_Type_Fourth_PartyDatasets"
# )
# def rawIP_Original_Source_By_Data_Type_Fourth_Party_Datasets():
#     path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawIP_Original_Source_By_Data_Type_Fourth_Party_Datasets/*"
#     schema = StructType([
#         StructField("AssetType_AcronymCode", StringType(), True),
#         StructField("AssetType_Color", StringType(), True),
#         StructField("AssetType_DisplayNameEnabled", StringType(), True),
#         StructField("AssetType_IconCode", StringType(), True),
#         StructField("AssetType_Id", StringType(), True),
#         StructField("AssetType_SymbolType", StringType(), True),
#         StructField("DisplayName", StringType(), True),
#         StructField("Domain_Id", StringType(), True),
#         StructField("Domain_Name", StringType(), True),
#         StructField("FullName", StringType(), True),
#         StructField("Id", StringType(), True),
#         StructField("SourceRelation_018f815d-6fc5-78b6-887d-11c8ffe3c31a", StringType(), True),
#         StructField("SourceRelation_018f815f-936b-74ab-8774-fdaef3959f89", StringType(), True),
#         StructField("SourceRelation_019104e0-8f88-7c73-ab76-f20e01b77e41", StringType(), True),
#         StructField("SourceRelation_0192ba67-15b2-7ee5-9549-d735bf3aac38", StringType(), True),
#         StructField("Status_Id", StringType(), True),
#         StructField("Status_IsSystem", StringType(), True),
#         StructField("Status_Name", StringType(), True),
#         StructField("StringAttribute_00000000-0000-0000-0000-000000003114", StringType(), True),
#         StructField("StringAttribute_018f301b-901e-7684-a18c-7b80eaefb8d7", StringType(), True),
#         StructField("StringAttribute_0192d89e-6a3d-718b-a7f0-3a800a693d90", StringType(), True),
#         StructField("StringAttribute_0192d89e-f0c9-7eef-a567-e1feb7a25c37", StringType(), True),
#         StructField("StringAttribute_af0317b5-7567-476c-8170-312a7c57181b", StringType(), True),
#         StructField("StringAttribute_b16e53ce-4b8c-414a-9781-b05ee42ff3d1", StringType(), True),
#         StructField("TargetRelation_018f815e-6432-768e-a385-80e6a4c645fb", StringType(), True),
#         StructField("TargetRelation_018f8169-c99c-7eb6-ad0f-46e9b9664610", StringType(), True),
#         StructField("SourceRelation_018f815d-6fc5-78b6-887d-11c8ffe3c31a_Id_0", StringType(), True),
#         StructField("SourceRelation_018f815d-6fc5-78b6-887d-11c8ffe3c31a_Target_DisplayName_0", StringType(), True),
#         StructField("SourceRelation_018f815d-6fc5-78b6-887d-11c8ffe3c31a_Target_FullName_0", StringType(), True),
#         StructField("SourceRelation_018f815d-6fc5-78b6-887d-11c8ffe3c31a_Target_Id_0", StringType(), True),
#         StructField("SourceRelation_018f815f-936b-74ab-8774-fdaef3959f89_Id_1", StringType(), True),
#         StructField("SourceRelation_018f815f-936b-74ab-8774-fdaef3959f89_Target_DisplayName_1", StringType(), True),
#         StructField("SourceRelation_018f815f-936b-74ab-8774-fdaef3959f89_Target_FullName_1", StringType(), True),
#         StructField("SourceRelation_018f815f-936b-74ab-8774-fdaef3959f89_Target_Id_1", StringType(), True),
#         StructField("SourceRelation_019104e0-8f88-7c73-ab76-f20e01b77e41_Id_2", StringType(), True),
#         StructField("SourceRelation_019104e0-8f88-7c73-ab76-f20e01b77e41_Target_DisplayName_2", StringType(), True),
#         StructField("SourceRelation_019104e0-8f88-7c73-ab76-f20e01b77e41_Target_FullName_2", StringType(), True),
#         StructField("SourceRelation_019104e0-8f88-7c73-ab76-f20e01b77e41_Target_Id_2", StringType(), True),
#         StructField("SourceRelation_0192ba67-15b2-7ee5-9549-d735bf3aac38_Id_3", StringType(), True),
#         StructField("SourceRelation_0192ba67-15b2-7ee5-9549-d735bf3aac38_Target_DisplayName_3", StringType(), True),
#         StructField("SourceRelation_0192ba67-15b2-7ee5-9549-d735bf3aac38_Target_FullName_3", StringType(), True),
#         StructField("SourceRelation_0192ba67-15b2-7ee5-9549-d735bf3aac38_Target_Id_3", StringType(), True),
#         StructField("StringAttribute_00000000-0000-0000-0000-000000003114_Id_4", StringType(), True),
#         StructField("StringAttribute_00000000-0000-0000-0000-000000003114_Value_4", StringType(), True),
#         StructField("StringAttribute_018f301b-901e-7684-a18c-7b80eaefb8d7_Id_5", StringType(), True),
#         StructField("StringAttribute_018f301b-901e-7684-a18c-7b80eaefb8d7_Value_5", StringType(), True),
#         StructField("StringAttribute_0192d89e-6a3d-718b-a7f0-3a800a693d90_Id_6", StringType(), True),
#         StructField("StringAttribute_0192d89e-6a3d-718b-a7f0-3a800a693d90_Value_6", StringType(), True),
#         StructField("StringAttribute_0192d89e-f0c9-7eef-a567-e1feb7a25c37_Id_7", StringType(), True),
#         StructField("StringAttribute_0192d89e-f0c9-7eef-a567-e1feb7a25c37_Value_7", StringType(), True),
#         StructField("StringAttribute_af0317b5-7567-476c-8170-312a7c57181b_Id_8", StringType(), True),
#         StructField("StringAttribute_af0317b5-7567-476c-8170-312a7c57181b_Value_8", StringType(), True),
#         StructField("StringAttribute_b16e53ce-4b8c-414a-9781-b05ee42ff3d1_Id_9", StringType(), True),
#         StructField("StringAttribute_b16e53ce-4b8c-414a-9781-b05ee42ff3d1_Value_9", StringType(), True),
#         StructField("TargetRelation_018f815e-6432-768e-a385-80e6a4c645fb_Id_10", StringType(), True),
#         StructField("TargetRelation_018f815e-6432-768e-a385-80e6a4c645fb_Source_DisplayName_10", StringType(), True),
#         StructField("TargetRelation_018f815e-6432-768e-a385-80e6a4c645fb_Source_FullName_10", StringType(), True),
#         StructField("TargetRelation_018f815e-6432-768e-a385-80e6a4c645fb_Source_Id_10", StringType(), True),
#         StructField("TargetRelation_018f8169-c99c-7eb6-ad0f-46e9b9664610_Id_11", StringType(), True),
#         StructField("TargetRelation_018f8169-c99c-7eb6-ad0f-46e9b9664610_Source_DisplayName_11", StringType(), True),
#         StructField("TargetRelation_018f8169-c99c-7eb6-ad0f-46e9b9664610_Source_FullName_11", StringType(), True),
#         StructField("TargetRelation_018f8169-c99c-7eb6-ad0f-46e9b9664610_Source_Id_11", StringType(), True),
#         StructField("snapshot_date", DateType(), True)
#     ])
#     df = spark.read.schema(schema).parquet(path)
#     return df

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

# @dlt.table(
#     comment="This is a view of the datasets provided by vendors",
#     name="raw_ListBvDDatasetsProvidedVendors"
# )
# def rawListBvDDatasetsProvidedVendors():
#     df = spark.read.parquet("/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/rawListBvDDatasetsProvidedVendors/*")
#     # display(df.limit(10))
#     # df.printSchema()
#     return df

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
