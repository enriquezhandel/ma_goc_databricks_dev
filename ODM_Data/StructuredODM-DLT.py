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
    name="structured_ip_data_sets",
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
def raw_informationprovider():
    raw_informationprovider = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_informationprovider")
    # raw_informationprovider.createOrReplaceTempView("raw_informationprovider")
    # display(raw_informationprovider.limit(10))
    return raw_informationprovider

# COMMAND ----------

@dlt.table(
    name="structured_informationprovider",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_informationprovider():
    raw_informationprovider = dlt.read("raw_informationprovider")
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
    structured_informationprovider = raw_informationprovider.select(*selected_columns)
    return structured_informationprovider

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
    name="structured_orbisdomain",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_orbisdomain():
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
    structured_orbisdomain = raw_orbisdomain.select(*selected_columns)
    # display(structured_ipdatasets.limit(10))
    return structured_orbisdomain

# COMMAND ----------

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
def structured_orbis4_1():
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

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_countries_and_continents():
    raw_countries_and_continents = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_countries_and_continents")
    # raw_countries_and_continents.createOrReplaceTempView("raw_countries_and_continents")
    # display(raw_countries_and_continents.limit(10))
    return raw_countries_and_continents


# COMMAND ----------

@dlt.table(
    name="structured_countries_and_continents",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_countries_and_continents():
    raw_countries_and_continents = dlt.read("raw_countries_and_continents")
    selected_columns = [
        "Full_Name",
        "Asset_Id",
        "Domain",
        "Domain_Id",
        "[Geographic_Asset]_has_ISO_[Code_Value]_>_Name",
        "[Geographic_Asset]_has_ISO_[Code_Value]_>_Full_Name",
        "[Geographic_Asset]_has_ISO_[Code_Value]_>_Asset_Id",
        "Description",
        "Note",
        "[Geographic_Asset]_geographic_data_provided_by_[Vendor]_>_Full_Name",
        "[Geographic_Asset]_geographic_data_provided_by_[Vendor]_>_Asset_Id",
        "[Geographic_Asset]_data_contained_in__[Product]_>_Full_Name",
        "[Geographic_Asset]_data_contained_in__[Product]_>_Asset_Id",
        "[Geographic_Asset]_data_contained_in_[Fourth-Party_Data_Asset]_>_Full_Name",
        "[Geographic_Asset]_data_contained_in_[Fourth-Party_Data_Asset]_>_Asset_Id",
        "[Geographic_Asset]_is_a_location_of_[Fourth-Party_Vendor]_>_Full_Name",
        "[Geographic_Asset]_is_a_location_of_[Fourth-Party_Vendor]_>_Asset_Id",
        "Country_Data_Overview",
        "Additional_Considerations",
        "Market_Coverage",
        "Filing_Requirements",
        "[Geographic_Asset]_main_official_source_of_company_information_[Fourth-Party_Vendor]_>_Full_Name",
        "[Geographic_Asset]_main_official_source_of_company_information_[Fourth-Party_Vendor]_>_Asset_Id",
        "[Geographic_Asset]_main_provider_of_country_data_is_[Vendor]_>_Full_Name",
        "[Geographic_Asset]_main_provider_of_country_data_is_[Vendor]_>_Asset_Id",
        "[Geographic_Asset]_main_provider_of_private_company_information_is_[Vendor]_>_Full_Name",
        "[Geographic_Asset]_main_provider_of_private_company_information_is_[Vendor]_>_Asset_Id",
        "Region_Code",
        "[Geographic_Asset]_groups_[Geographic_Asset]_>_Full_Name",
        "[Geographic_Asset]_groups_[Geographic_Asset]_>_Asset_Id",
        "[Geographic_Asset]_is_grouped_within_[Geographic_Asset]_>_Full_Name",
        "[Geographic_Asset]_is_grouped_within_[Geographic_Asset]_>_Asset_Id",
        "[Geographic_Asset]_is_located_in_[Geographic_Asset]_>_Full_Name",
        "[Geographic_Asset]_is_located_in_[Geographic_Asset]_>_Asset_Id",
        "[Geographic_Asset]_has_currency_code_[Code_Value]_>_Full_Name",
        "[Geographic_Asset]_has_currency_code_[Code_Value]_>_Asset_Id",
        "dataset_Name",
        "snapshot_date"
    ]
    structured_countries_and_continents = raw_countries_and_continents.select(*selected_columns)
    return structured_countries_and_continents

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_states():
    raw_states = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_states")
    # raw_states.createOrReplaceTempView("raw_states")
    # display(raw_states.limit(10))
    return raw_states

# COMMAND ----------

@dlt.table(
    name="structured_states",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_dataqualitytests():
    raw_states = dlt.read("raw_states")
    selected_columns = [
        "Full_Name",
        "Asset_Id",
        "Domain",
        "Domain_Id",
        "[Geographic_Asset]_has_ISO_[Code_Value]_>_Full_Name",
        "[Geographic_Asset]_has_ISO_[Code_Value]_>_Asset_Id",
        "Description",
        "[Geographic_Asset]_geographic_data_provided_by_[Vendor]_>_Full_Name",
        "[Geographic_Asset]_geographic_data_provided_by_[Vendor]_>_Asset_Id",
        "[Geographic_Asset]_data_contained_in__[Product]_>_Full_Name",
        "[Geographic_Asset]_data_contained_in__[Product]_>_Asset_Id",
        "[Geographic_Asset]_data_contained_in_[Fourth-Party_Data_Asset]_>_Full_Name",
        "[Geographic_Asset]_data_contained_in_[Fourth-Party_Data_Asset]_>_Asset_Id",
        "[Geographic_Asset]_is_a_location_of_[Fourth-Party_Vendor]_>_Full_Name",
        "[Geographic_Asset]_is_a_location_of_[Fourth-Party_Vendor]_>_Asset_Id",
        "Filing_Requirements",
        "Additional_Considerations",
        "[Geographic_Asset]_is_headquarters_to__[Fourth-Party_Vendor]_>_Full_Name",
        "[Geographic_Asset]_is_headquarters_to__[Fourth-Party_Vendor]_>_Asset_Id",
        "[Geographic_Asset]_main_official_source_of_company_information_[Fourth-Party_Vendor]_>_Full_Name",
        "[Geographic_Asset]_main_official_source_of_company_information_[Fourth-Party_Vendor]_>_Asset_Id",
        "[Geographic_Asset]_main_provider_of_country_data_is_[Vendor]_>_Full_Name",
        "[Geographic_Asset]_main_provider_of_country_data_is_[Vendor]_>_Asset_Id",
        "[Geographic_Asset]_main_provider_of_private_company_information_is_[Vendor]_>_Full_Name",
        "[Geographic_Asset]_main_provider_of_private_company_information_is_[Vendor]_>_Asset_Id",
        "Country_Data_Overview",
        "Market_Coverage",
        "[Geographic_Asset]_is_state_/_province_/_region_within_[Geographic_Asset]_>_Full_Name",
        "[Geographic_Asset]_is_state_/_province_/_region_within_[Geographic_Asset]_>_Asset_Id",
        "Note",
        "Region_Code",
        "[Geographic_Asset]_has_currency_code_[Code_Value]_>_Full_Name",
        "[Geographic_Asset]_has_currency_code_[Code_Value]_>_Asset_Id",
        "dataset_Name",
        "snapshot_date"
    ]
    structured_states = raw_states.select(*selected_columns)
    return structured_states

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_ip_reference_codes():
    raw_ip_reference_codes = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_ip_reference_codes")
    # raw_ip_reference_codes.createOrReplaceTempView("raw_ip_reference_codes")
    # display(raw_ip_reference_codes.limit(10))
    return raw_ip_reference_codes

# COMMAND ----------

@dlt.table(
    name="structured_ip_reference_codes",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_ip_reference_codes():
    raw_ip_reference_codes = dlt.read("raw_ip_reference_codes")
    selected_columns = [
        "Full_Name",
        "Asset_Id",
        "Status",
        "Status_Id",
        "[Code_Value]_identifies_[Vendor]_>_Full_Name",
        "[Code_Value]_identifies_[Vendor]_>_Asset_Id",
        "[Code_Value]_identifies_[Product]_>_Full_Name",
        "[Code_Value]_identifies_[Product]_>_Asset_Id",
        "Description",
        "[Code_Value]_groups_[Code_Value]_>_Full_Name",
        "[Code_Value]_groups_[Code_Value]_>_Asset_Id",
        "Additional_Metadata",
        "Available_in_which_products/platforms",
        "ID",
        "ID_Id",
        "dataset_Name",
        "snapshot_date"
    ]
    structured_ip_reference_codes = raw_ip_reference_codes.select(*selected_columns)
    return structured_ip_reference_codes

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_sourcecodesfordatasets():
    raw_sourcecodesfordatasets = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_sourcecodesfordatasets")
    # raw_sourcecodesfordatasets.createOrReplaceTempView("raw_sourcecodesfordatasets")
    # display(raw_sourcecodesfordatasets.limit(10))
    return raw_sourcecodesfordatasets

# COMMAND ----------

@dlt.table(
    name="structured_sourcecodesfordatasets",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_sourcecodesfordatasets():
    raw_sourcecodesfordatasets = dlt.read("raw_sourcecodesfordatasets")
    selected_columns = [
        "Full_Name",
        "Asset_Id",
        "Status",
        "Status_Id",
        "Status_Is_System",
        "[Code_Value]_identifies_[Product]_>_Full_Name",
        "[Code_Value]_identifies_[Product]_>_Asset_Id",
        "[Code_Value]_identifies_[Vendor]_>_Full_Name",
        "[Code_Value]_identifies_[Vendor]_>_Asset_Id",
        "Description",
        "[Code_Value]_grouped_by_[Code_Value]_>_Full_Name",
        "[Code_Value]_grouped_by_[Code_Value]_>_Asset_Id",
        "Available_in_which_products/platforms",
        "ID",
        "Additional_Metadata",
        "dataset_Name",
        "snapshot_date"
    ]
    structured_sourcecodesfordatasets = raw_sourcecodesfordatasets.select(*selected_columns)
    return structured_sourcecodesfordatasets

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_ip_original_source_by_data_type():
    raw_ip_original_source_by_data_type = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_ip_original_source_by_data_type")
    # raw_ip_original_source_by_data_type.createOrReplaceTempView("raw_ip_original_source_by_data_type")
    # display(raw_ip_original_source_by_data_type.limit(10))
    return raw_ip_original_source_by_data_type

# COMMAND ----------

@dlt.table(
    name="structured_ip_original_source_by_data_type",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_ip_original_source_by_data_type():
    raw_ip_original_source_by_data_type = dlt.read("raw_ip_original_source_by_data_type")
    selected_columns = [
        "[Fourth-Party_Data_Asset]_contains_data_on_[Geographic_Asset]_>_Full_Name",
        "[Fourth-Party_Data_Asset]_contains_data_on_[Geographic_Asset]_>_Asset_Id",
        "[Fourth-Party_Data_Asset]_has_ISO_[Code_Value]_>_Name",
        "[Fourth-Party_Data_Asset]_has_ISO_[Code_Value]_>_Full_Name",
        "[Fourth-Party_Data_Asset]_has_ISO_[Code_Value]_>_Asset_Id",
        "Full_Name",
        "Asset_Id",
        "[Fourth-Party_Data_Asset]_supplies_data_to_[Vendor]_>_Full_Name",
        "[Fourth-Party_Data_Asset]_supplies_data_to_[Vendor]_>_Asset_Id",
        "[Fourth-Party_Data_Asset]_receives_data_from__[Fourth-Party_Vendor]_>_Full_Name",
        "[Fourth-Party_Data_Asset]_receives_data_from__[Fourth-Party_Vendor]_>_Asset_Id",
        "[Fourth-Party_Data_Asset]_Feeds_[Product]_>_Full_Name",
        "[Fourth-Party_Data_Asset]_Feeds_[Product]_>_Asset_Id",
        "[Fourth-Party_Data_Asset]_is_classified_by_[Business_Dimension]_>_Full_Name",
        "[Fourth-Party_Data_Asset]_is_classified_by_[Business_Dimension]_>_Asset_Id",
        "IP_Data_Type_Delivery_Frequency_to_Moody's",
        "IP_Data_Type_Update_Frequency_from_their_Source",
        "Original_Source_Data_Frequency",
        "Name_Displayed_in_ORBIS",
        "dataset_Name",
        "snapshot_date"
    ]
    structured_ip_original_source_by_data_type = raw_ip_original_source_by_data_type.select(*selected_columns)
    return structured_ip_original_source_by_data_type

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_sources_used_by_ips():
    raw_sources_used_by_ips = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_sources_used_by_ips")
    # raw_sources_used_by_ips.createOrReplaceTempView("raw_sources_used_by_ips")
    # display(raw_sources_used_by_ips.limit(10))
    return raw_sources_used_by_ips

# COMMAND ----------

@dlt.table(
    name="structured_sources_used_by_ips",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_sources_used_by_ips():
    raw_sources_used_by_ips = dlt.read("raw_sources_used_by_ips")
    selected_columns = [
        "Original_Name",
        "Original_Name_Id",
        "Full_Name",
        "Asset_Id",
        "Alias",
        "[Fourth-Party_Vendor]_supplies_data_on_entities_in_ISO_[Code_Value]_>_Name",
        "[Fourth-Party_Vendor]_supplies_data_on_entities_in_ISO_[Code_Value]_>_Full_Name",
        "[Fourth-Party_Vendor]_supplies_data_on_entities_in_ISO_[Code_Value]_>_Asset_Id",
        "[Fourth-Party_Vendor]_operates_in_[Geographic_Asset]_>_Full_Name",
        "[Fourth-Party_Vendor]_operates_in_[Geographic_Asset]_>_Asset_Id",
        "Description",
        "URL",
        "[Fourth-Party_Vendor]_is_authoritative_source_of_[Vendor]_>_Full_Name",
        "[Fourth-Party_Vendor]_is_authoritative_source_of_[Vendor]_>_Asset_Id",
        "[Fourth-Party_Vendor]_is_main_source_of_company_information_[Geographic_Asset]_>_Full_Name",
        "[Fourth-Party_Vendor]_is_main_source_of_company_information_[Geographic_Asset]_>_Asset_Id",
        "[Fourth-Party_Vendor]_supplies_data_to__[Fourth-Party_Data_Asset]_>_Full_Name",
        "[Fourth-Party_Vendor]_supplies_data_to__[Fourth-Party_Data_Asset]_>_Asset_Id",
        "Registry_publication_latency",
        "Update_Frequency",
        "Note",
        "Background",
        "Update_Frequency_Notes",
        "dataset_Name",
        "snapshot_date"
    ]
    structured_sources_used_by_ips = raw_sources_used_by_ips.select(*selected_columns)
    return structured_sources_used_by_ips
