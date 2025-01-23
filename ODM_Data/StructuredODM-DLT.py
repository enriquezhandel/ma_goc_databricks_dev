# Databricks notebook source
import dlt
from pyspark.sql.types import DateType, IntegerType,LongType, StructType, StructField, TimestampType, StringType
import datetime
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_ipdatasets():
    raw_ipdatasets = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_ipdatasets")
    # raw_ipdatasets.createOrReplaceTempView("raw_ipdatasets")
    # display(raw_ipdatasets.limit(10))
    return raw_ipdatasets

# COMMAND ----------

@dlt.table(
    name="structured_ipdatasets",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_ipdatasets():
    raw_ipdatasets = dlt.read("raw_ipdatasets")
    selected_columns = [
        "Status",
        "`[Product]_contains_data_on_[Geographic_Asset]_>_Full_Name`",
        "`[Product]_contains_data_on_[Geographic_Asset]_>_Asset_Id`",
        "`[Business_Asset]_has_ISO_[Code_Value]_>_Name`",
        "`[Business_Asset]_has_ISO_[Code_Value]_>_Full_Name`",
        "`[Business_Asset]_has_ISO_[Code_Value]_>_Asset_Id`",
        "`[Product]_is_provided_by_[Vendor]_>_full_name`",
        "`[Product]_is_provided_by_[Vendor]_>_Asset_Id`",
        "Name_Displayed_in_ORBIS",
        "Full_Name",
        "Asset_Id",
        "`[Product]_has_[Code_Value]_>_Full_Name`",
        "`[Product]_has_[Code_Value]_>_Asset_Id`",
        "Description",
        "`[Product]_is_fed_by_[Fourth-Party_Data_Asset]_>_full_name`",
        "`[Product]_is_fed_by_[Fourth-Party_Data_Asset]_>_Asset_Id`",
        "`[Product]_uses_data_from_ultimate_official_source_[Vendor]_>_full_name`",
        "`[Product]_uses_data_from_ultimate_official_source_[Vendor]_>_Asset_Id`",
        "`[Product]_is_governed_by_[Data_Domain]_>_full_name`",
        "`[Product]_is_governed_by_[Data_Domain]_>_Asset_Id`",
        "Delivery_Frequency",
        "Delivery_Method",
        "Data_Specialist",
        "Indexer",
        "dataset_Name",
        "snapshot_date"
    ]
    structured_ipdatasets = raw_ipdatasets.select(*selected_columns)
    return structured_ipdatasets

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_ipdatasets():
    raw_informationprovider = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_informationprovider")
    raw_informationprovider.createOrReplaceTempView("raw_informationprovider")
    display(raw_informationprovider.limit(10))
    return raw_informationprovider

# COMMAND ----------

@dlt.table(
    name="structured_informationprovider",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_informationprovider():
    raw_ipdatasets = dlt.read("raw_ipdatasets")
    selected_columns = [
        "Full_Name",
        "Asset_Id",
        "id",
        "Status",
        "Alias",
        "DSO_Manager",
        "Contract_IDs",
        "IP_Code_DSO",
        "Other_reference_IDs",
        "Description",
        "Restricted_Data",
        "url",
        "Vendor_sources_data_from_Fourth_Party_Data_Asset_Full_Name",
        "Vendor_sources_data_from_Fourth_Party_Data_Asset_Asset_Id",
        "dataset_Name",
        "snapshot_date"
    ]
    structured_ipdatasets = raw_ipdatasets.select(*selected_columns)
    return structured_ipdatasets

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_orbisdomain():
    raw_orbisdomain = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_orbisdomain")
    # raw_orbisdomain.createOrReplaceTempView("raw_orbisdomain")
    # display(raw_orbisdomain.limit(10))
    return raw_orbisdomain

# COMMAND ----------

@dlt.table(
    name="structured_ipdatasets",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_ipdatasets():
    raw_orbisdomain = dlt.read("raw_orbisdomain")
    selected_columns = [
        "Full_Name",
        "Asset_Id",
        "Definition",
        "Roles_Data_Steward_Author_User_Name",
        "Roles_Data_Steward_Author_First_Name",
        "Roles_Data_Steward_Author_Last_Name",
        "Data_Specialist",
        "Data_Custodian",
        "Product_Manager",
        "ODM_Rep",
        "Business_Asset_grouped_by_Business_Asset_Full_Name",
        "Business_Asset_grouped_by_Business_Asset_Asset_Id",
        "ID",
        "dataset_Name",
        "snapshot_date"
    ]
    structured_ipdatasets = raw_orbisdomain.select(*selected_columns)
    # display(structured_ipdatasets.limit(10))
    return structured_ipdatasets

# COMMAND ----------

# "Data_Quality_Tests": "018f6172-fa1d-711d-929d-db01ab352257",
@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_dataqualitytests():
    raw_dataqualitytests = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_dataqualitytests")
    raw_dataqualitytests.createOrReplaceTempView("raw_dataqualitytests")
    display(raw_dataqualitytests.limit(10))
    return raw_dataqualitytests

# COMMAND ----------

@dlt.table(
    name="structured_dataqualitytests",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_dataqualitytests():
    raw_dataqualitytests = dlt.read("raw_dataqualitytests")
    selected_columns = [
        "Full_Name",
        "Asset_Id",
        "Description",
        "Purpose",
        "Business_Rule_implements_Rule_Full_Name",
        "Business_Rule_implements_Rule_Asset_Id",
        "Business_Rule_implements_Column_Full_Name",
        "Business_Rule_implements_Column_Asset_Id",
        "Data_Quality_Rule_classified_by_Data_Quality_Dimension_Full_Name",
        "Data_Quality_Rule_classified_by_Data_Quality_Dimension_Asset_Id"
    ]
    structured_dataqualitytests = raw_dataqualitytests.select(*selected_columns)
    # display(structured_dataqualitytests.limit(10))
    return structured_dataqualitytests

# COMMAND ----------

# "Data_Quality_Rules": "ea07eb48-fc28-448b-b43c-bf46aea73773",
@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_dataqualityrules():
    raw_dataqualityrules = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_dataqualityrules")
    # raw_dataqualityrules.createOrReplaceTempView("raw_dataqualityrules")
    # display(raw_dataqualityrules.limit(10))
    return raw_dataqualityrules


# COMMAND ----------

@dlt.table(
    name="structured_dataqualityrules",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_dataqualitytests():
    raw_dataqualityrules = dlt.read("raw_dataqualityrules")
    selected_columns = [
        "Full_Name",
        "Asset_Id",
        "Description",
        "Predicate",
        "Data_Quality_Rule_governs_Column_Full_Name",
        "Data_Quality_Rule_governs_Column_Asset_Id",
        "Rule_is_implemented_by_Business_Rule_Full_Name",
        "Rule_is_implemented_by_Business_Rule_Asset_Id"
    ]
    structured_dataqualityrules = raw_dataqualityrules.select(*selected_columns)
    return structured_dataqualityrules

# COMMAND ----------

# "Orbis_4_1": "0191bc4f-ba72-7c3d-a2f5-d2d5cf389d28"
@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_orbis4_1():
    raw_orbis4_1 = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_orbis4_1")
    # raw_orbis4_1.createOrReplaceTempView("raw_orbis4_1")
    # display(raw_orbis4_1.limit(10))
    return raw_orbis4_1

# COMMAND ----------

@dlt.table(
    name="structured_orbis4_1",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_dataqualitytests():
    raw_orbis4_1 = dlt.read("raw_orbis4_1")
    selected_columns = [
        "Level",
        "File_Path",
        "Presentation_Line_ID",
        "Full_Name",
        "Asset_Id",
        "Description",
        "Technical_Data_Type",
        "Column_is_governed_by_Data_Domain_Full_Name",
        "Column_is_governed_by_Data_Domain_Asset_Id",
        "Data_Asset_represented_by_Business_Asset_Full_Name",
        "Data_Asset_represented_by_Business_Asset_Asset_Id",
        "Additional_Metadata",
        "Model_ID",
        "Period_Criteria",
        "Repetition_Criteria",
        "Ratio_Type",
        "Request_Code",
        "note"
    ]
    structured_orbis4_1 = raw_orbis4_1.select(*selected_columns)
    return structured_orbis4_1
