# Databricks notebook source
import dlt
from pyspark.sql.types import DateType, IntegerType,LongType, StructType, StructField, TimestampType, StringType, BooleanType, DoubleType
from pyspark.sql.functions import col

# COMMAND ----------

@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="Raw_IPDatasets"
)
def Raw_IPDatasets():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/IP_Datasets/*"
    schema = StructType([
        StructField("Status", StringType(), True),
        StructField("Status_Id", StringType(), True),
        StructField("Status_Is_System", BooleanType(), True),
        StructField("[Product]_contains_data_on_[Geographic_Asset]_>_Name", StringType(), True),
        StructField("[Product]_contains_data_on_[Geographic_Asset]_>_Full_Name", StringType(), True),
        StructField("[Product]_contains_data_on_[Geographic_Asset]_>_Asset_Id", StringType(), True),
        StructField("SourceRelation_019104ae-c2cf-75dc-a54d-c0f646956f6e_Id", StringType(), True),
        StructField("[Business_Asset]_has_ISO_[Code_Value]_>_Name", StringType(), True),
        StructField("[Business_Asset]_has_ISO_[Code_Value]_>_Full_Name", StringType(), True),
        StructField("[Business_Asset]_has_ISO_[Code_Value]_>_Asset_Id", StringType(), True),
        StructField("SourceRelation_82b7109b-85a7-4bde-83e9-718640d814cc_Id", StringType(), True),
        StructField("[Product]_is_provided_by_[Vendor]_>_Name", StringType(), True),
        StructField("[Product]_is_provided_by_[Vendor]_>_Full_Name", StringType(), True),
        StructField("[Product]_is_provided_by_[Vendor]_>_Asset_Id", StringType(), True),
        StructField("TargetRelation_de81c6cc-9733-4419-aa66-66724015e4e9_Id", StringType(), True),
        StructField("Name_Displayed_in_ORBIS", StringType(), True),
        StructField("Name_Displayed_in_ORBIS_Id", StringType(), True),
        StructField("Full_Name", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Asset_Id", StringType(), True),
        StructField("Asset_Type_Id", StringType(), True),
        StructField("Asset_Type_Color", StringType(), True),
        StructField("Asset_Type_SymbolType", StringType(), True),
        StructField("Asset_Type_IconCode", DoubleType(), True),
        StructField("Asset_Type_AcronymCode", StringType(), True),
        StructField("Asset_Type_Display_Name_Enabled", BooleanType(), True),
        StructField("Domain", StringType(), True),
        StructField("Domain_Id", StringType(), True),
        StructField("[Product]_has_[Code_Value]_>_Name", StringType(), True),
        StructField("[Product]_has_[Code_Value]_>_Full_Name", StringType(), True),
        StructField("[Product]_has_[Code_Value]_>_Asset_Id", StringType(), True),
        StructField("SourceRelation_795e9c10-a3f2-4602-b35e-649600497804_Id", StringType(), True),
        StructField("Alias", StringType(), True),
        StructField("Alias_Id", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Description_Id", StringType(), True),
        StructField("Filing_Requirements", StringType(), True),
        StructField("Filing_Requirements_Id", StringType(), True),
        StructField("[Product]_is_fed_by_[Fourth-Party_Data_Asset]_>_Name", StringType(), True),
        StructField("[Product]_is_fed_by_[Fourth-Party_Data_Asset]_>_Full_Name", StringType(), True),
        StructField("[Product]_is_fed_by_[Fourth-Party_Data_Asset]_>_Asset_Id", StringType(), True),
        StructField("TargetRelation_0192ba67-15b2-7ee5-9549-d735bf3aac38_Id", StringType(), True),
        StructField("[Product]_uses_data_from_ultimate_official_source_[Vendor]_>_Name", StringType(), True),
        StructField("[Product]_uses_data_from_ultimate_official_source_[Vendor]_>_Full_Name", StringType(), True),
        StructField("[Product]_uses_data_from_ultimate_official_source_[Vendor]_>_Asset_Id", StringType(), True),
        StructField("SourceRelation_9c7f7d98-9ff2-42cf-aeae-5e7fae06dd90_Id", StringType(), True),
        StructField("[Product]_is_governed_by_[Data_Domain]_>_Name", StringType(), True),
        StructField("[Product]_is_governed_by_[Data_Domain]_>_Full_Name", StringType(), True),
        StructField("[Product]_is_governed_by_[Data_Domain]_>_Asset_Id", StringType(), True),
        StructField("TargetRelation_c883de39-83aa-47ac-bc85-b111d6830ba2_Id", StringType(), True),
        StructField("Last_Review_Date", StringType(), True),
        StructField("Last_Review_Date_Id", StringType(), True),
        StructField("Delivery_Frequency", StringType(), True),
        StructField("Delivery_Frequency_Id", StringType(), True),
        StructField("Delivery_Method", StringType(), True),
        StructField("Delivery_Method_Id", StringType(), True),
        StructField("Data_Specialist", StringType(), True),
        StructField("Data_Specialist_Id", StringType(), True),
        StructField("Indexer", StringType(), True),
        StructField("Indexer_Id", StringType(), True),
        StructField("Frequency", StringType(), True),
        StructField("Frequency_Id", StringType(), True),
        StructField("Update_Frequency", StringType(), True),
        StructField("Update_Frequency_Id", StringType(), True),
        StructField("dataset_Name", StringType(), True),
        StructField("snapshot_date", StringType(), True)
    ])
    Raw_IPDatasets = spark.readStream.format("parquet").schema(schema).option("path", path).load()
    # Raw_IPDatasets.createOrReplaceTempView("Raw_IP_Datasets")
    # display(Raw_IPDatasets)
    return Raw_IPDatasets

# COMMAND ----------

@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="Raw_InformationProvider"
)
def Raw_IP():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/IP/*"
    schema = StructType([
        StructField("Full_Name", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Asset_Id", StringType(), True),
        StructField("Asset_Type_Id", StringType(), True),
        StructField("Asset_Type_Color", StringType(), True),
        StructField("Asset_Type_SymbolType", StringType(), True),
        StructField("Asset_Type_IconCode", DoubleType(), True),
        StructField("Asset_Type_AcronymCode", StringType(), True),
        StructField("Asset_Type_Display_Name_Enabled", BooleanType(), True),
        StructField("Domain", StringType(), True),
        StructField("Domain_Id", StringType(), True),
        StructField("ID", StringType(), True),
        StructField("ID_Id", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("Status_Id", StringType(), True),
        StructField("Status_Is_System", BooleanType(), True),
        StructField("Vendor_Relationship_Owner", StringType(), True),
        StructField("Vendor_Relationship_Owner_Id", StringType(), True),
        StructField("Alias", StringType(), True),
        StructField("Alias_Id", StringType(), True),
        StructField("Previous_Name", StringType(), True),
        StructField("Previous_Name_Id", StringType(), True),
        StructField("DSO_Manager", StringType(), True),
        StructField("DSO_Manager_Id", StringType(), True),
        StructField("Contract_IDs", StringType(), True),
        StructField("Contract_IDs_Id", StringType(), True),
        StructField("IP_Code_DSO", StringType(), True),
        StructField("IP_Code_DSO_Id", StringType(), True),
        StructField("Other_reference_IDs", StringType(), True),
        StructField("Other_reference_IDs_Id", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Description_Id", StringType(), True),
        StructField("SLA", DoubleType(), True),
        StructField("SLA_Id", DoubleType(), True),
        StructField("Restricted_Data", StringType(), True),
        StructField("Restricted_Data_Id", StringType(), True),
        StructField("Legal_Form", DoubleType(), True),
        StructField("Legal_Form_Id", DoubleType(), True),
        StructField("URL", StringType(), True),
        StructField("URL_Id", StringType(), True),
        StructField("Provider_Type", StringType(), True),
        StructField("Provider_Type_Id", StringType(), True),
        StructField("Vendor_sources_data_from_Fourth_Party_Data_Asset_Name", StringType(), True),
        StructField("Vendor_sources_data_from_Fourth_Party_Data_Asset_Full_Name", StringType(), True),
        StructField("Vendor_sources_data_from_Fourth_Party_Data_Asset_Asset_Id", StringType(), True),
        StructField("TargetRelation_018f815d-6fc5-78b6-887d-11c8ffe3c31a_Id", StringType(), True),
        StructField("dataset_Name", StringType(), True),
        StructField("snapshot_date", StringType(), True)
    ])
    Raw_InformationProvider = spark.readStream.format("parquet").schema(schema).option("path", path).load()
    # Raw_InformationProvider.createOrReplaceTempView("Raw_InformationProvider")
    # display(Raw_InformationProvider)
    return Raw_InformationProvider

# COMMAND ----------

@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="Raw_OrbisDomain"
)
def Raw_OrbisDomain():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/Orbis_Domains/*"
    schema = StructType([
        StructField("Full_Name", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Asset_Id", StringType(), True),
        StructField("Asset_Type_Color", StringType(), True),
        StructField("Asset_Type_SymbolType", StringType(), True),
        StructField("Asset_Type_IconCode", DoubleType(), True),
        StructField("Asset_Type_AcronymCode", StringType(), True),
        StructField("Asset_Type_Display_Name_Enabled", BooleanType(), True),
        StructField("Domain", StringType(), True),
        StructField("Domain_Id", StringType(), True),
        StructField("Asset_Type", StringType(), True),
        StructField("Asset_Type_Discriminator", StringType(), True),
        StructField("Asset_Type_Is_System", BooleanType(), True),
        StructField("Asset_Type_Id", StringType(), True),
        StructField("Asset_Type_Public_Id", StringType(), True),
        StructField("Asset_Type_Final_Type", BooleanType(), True),
        StructField("Asset_Type_Lock_Statuses", BooleanType(), True),
        StructField("Asset_Type_Product", StringType(), True),
        StructField("Definition", StringType(), True),
        StructField("Definition_Id", StringType(), True),
        StructField("Roles_Data_Steward_Author_User_Name", StringType(), True),
        StructField("Roles_Data_Steward_Author_First_Name", StringType(), True),
        StructField("Roles_Data_Steward_Author_Last_Name", StringType(), True),
        StructField("Roles_Data_Steward_Author_User_Id", StringType(), True),
        StructField("Roles_Data_Steward_Author_Group_Name", DoubleType(), True),
        StructField("Roles_Data_Steward_Author_Group_Id", DoubleType(), True),
        StructField("Data_Specialist", StringType(), True),
        StructField("Data_Specialist_Id", StringType(), True),
        StructField("Data_Custodian", StringType(), True),
        StructField("Data_Custodian_Id", StringType(), True),
        StructField("Product_Manager", StringType(), True),
        StructField("Product_Manager_Id", StringType(), True),
        StructField("ODM_Rep", StringType(), True),
        StructField("ODM_Rep_Id", StringType(), True),
        StructField("Business_Asset_grouped_by_Business_Asset_Name", StringType(), True),
        StructField("Business_Asset_grouped_by_Business_Asset_Full_Name", StringType(), True),
        StructField("Business_Asset_grouped_by_Business_Asset_Asset_Id", StringType(), True),
        StructField("TargetRelation_00000000_0000_0000_0000_000000007021_Id", StringType(), True),
        StructField("ID", StringType(), True),
        StructField("ID_Id", StringType(), True),
        StructField("dataset_Name", StringType(), True),
        StructField("snapshot_date", StringType(), True)
    ])
    Raw_OrbisDomain = spark.readStream.format("parquet").schema(schema).option("path", path).load()
    # Raw_OrbisDomain.createOrReplaceTempView("Raw_OrbisDomain")
    # display(Raw_OrbisDomain)
    return Raw_OrbisDomain

# COMMAND ----------

schema = StructType([
    StructField("Level", StringType(), True),
    StructField("Level_Id", StringType(), True),
    StructField("File_Path", StringType(), True),
    StructField("File_Path_Id", StringType(), True),
    StructField("Presentation_Line_ID", DoubleType(), True),
    StructField("Presentation_Line_ID_Id", DoubleType(), True),
    StructField("Full_Name", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Asset_Id", StringType(), True),
    StructField("Asset_Type_Color", StringType(), True),
    StructField("Asset_Type_SymbolType", StringType(), True),
    StructField("Asset_Type_IconCode", StringType(), True),
    StructField("Asset_Type_AcronymCode", StringType(), True),
    StructField("Asset_Type_Display_Name_Enabled", BooleanType(), True),
    StructField("Domain", StringType(), True),
    StructField("Domain_Id", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Description_Id", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Status_Id", StringType(), True),
    StructField("Status_Is_System", BooleanType(), True),
    StructField("Asset_Type", StringType(), True),
    StructField("Asset_Type_Discriminator", StringType(), True),
    StructField("Asset_Type_Is_System", BooleanType(), True),
    StructField("Asset_Type_Id", StringType(), True),
    StructField("Asset_Type_Public_Id", StringType(), True),
    StructField("Asset_Type_Final_Type", BooleanType(), True),
    StructField("Asset_Type_Lock_Statuses", BooleanType(), True),
    StructField("Asset_Type_Product", StringType(), True),
    StructField("Technical_Data_Type", StringType(), True),
    StructField("Technical_Data_Type_Id", StringType(), True),
    StructField("Column_is_governed_by_Data_Domain_Name", StringType(), True),
    StructField("Column_is_governed_by_Data_Domain_Full_Name", StringType(), True),
    StructField("Column_is_governed_by_Data_Domain_Asset_Id", StringType(), True),
    StructField("TargetRelation_01914c89_12ad_7392_a285_752b291e6783_Id", StringType(), True),
    StructField("Data_Asset_represented_by_Business_Asset_Name", StringType(), True),
    StructField("Data_Asset_represented_by_Business_Asset_Full_Name", StringType(), True),
    StructField("Data_Asset_represented_by_Business_Asset_Asset_Id", StringType(), True),
    StructField("TargetRelation_00000000_0000_0000_0000_000000007038_Id", StringType(), True),
    StructField("Additional_Metadata", DoubleType(), True),
    StructField("Additional_Metadata_Id", DoubleType(), True),
    StructField("Model_ID", StringType(), True),
    StructField("Model_ID_Id", StringType(), True),
    StructField("Period_Criteria", StringType(), True),
    StructField("Period_Criteria_Id", StringType(), True),
    StructField("Repetition_Criteria", StringType(), True),
    StructField("Repetition_Criteria_Id", StringType(), True),
    StructField("Ratio_Type", StringType(), True),
    StructField("Ratio_Type_Id", StringType(), True),
    StructField("Request_Code", DoubleType(), True),
    StructField("Request_Code_Id", StringType(), True),
    StructField("Note", StringType(), True),
    StructField("Note_Id", StringType(), True),
    StructField("Data_Element_is_part_of_Data_Set_Name", StringType(), True),
    StructField("Data_Element_is_part_of_Data_Set_Full_Name", StringType(), True),
    StructField("Data_Element_is_part_of_Data_Set_Asset_Id", StringType(), True),
    StructField("TargetRelation_00000000_0000_0000_0000_000000007062_Id", StringType(), True),
    StructField("dataset_Name", StringType(), True),
    StructField("snapshot_date", StringType(), True)
])

@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="Raw_Orbis4_1"
)
def Raw_Orbis4_1():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/Orbis_4_1/*"
    Raw_Orbis4_1 = spark.readStream.format("parquet").schema(schema).option("path", path).load()
    return Raw_Orbis4_1

# COMMAND ----------

schema = StructType([
    StructField("Full_Name", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Asset_Id", StringType(), True),
    StructField("Asset_Type_Color", StringType(), True),
    StructField("Asset_Type_SymbolType", StringType(), True),
    StructField("Asset_Type_IconCode", StringType(), True),
    StructField("Asset_Type_AcronymCode", StringType(), True),
    StructField("Asset_Type_Display_Name_Enabled", BooleanType(), True),
    StructField("Domain", StringType(), True),
    StructField("Domain_Id", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Description_Id", StringType(), True),
    StructField("Purpose", StringType(), True),
    StructField("Purpose_Id", StringType(), True),
    StructField("Business_Rule_implements_Rule_Name", StringType(), True),
    StructField("Business_Rule_implements_Rule_Full_Name", StringType(), True),
    StructField("Business_Rule_implements_Rule_Asset_Id", StringType(), True),
    StructField("TargetRelation_00000000_0000_0000_0000_000000007014_Id", StringType(), True),
    StructField("Business_Rule_implements_Column_Name", StringType(), True),
    StructField("Business_Rule_implements_Column_Full_Name", StringType(), True),
    StructField("Business_Rule_implements_Column_Asset_Id", StringType(), True),
    StructField("SourceRelation_019002a2_f412_7fb3_820e_df9fc2229064_Id", StringType(), True),
    StructField("Data_Quality_Rule_classified_by_Data_Quality_Dimension_Name", DoubleType(), True),
    StructField("Data_Quality_Rule_classified_by_Data_Quality_Dimension_Full_Name", DoubleType(), True),
    StructField("Data_Quality_Rule_classified_by_Data_Quality_Dimension_Asset_Id", DoubleType(), True),
    StructField("SourceRelation_00000000_0000_0000_0000_000000007053_Id", DoubleType(), True),
    StructField("Status", StringType(), True),
    StructField("Status_Id", StringType(), True),
    StructField("Status_Is_System", BooleanType(), True),
    StructField("Asset_Type", StringType(), True),
    StructField("Asset_Type_Discriminator", StringType(), True),
    StructField("Asset_Type_Is_System", BooleanType(), True),
    StructField("Asset_Type_Id", StringType(), True),
    StructField("Asset_Type_Public_Id", StringType(), True),
    StructField("Asset_Type_Final_Type", BooleanType(), True),
    StructField("Asset_Type_Lock_Statuses", BooleanType(), True),
    StructField("Asset_Type_Product", StringType(), True),
    StructField("dataset_Name", StringType(), True),
    StructField("snapshot_date", StringType(), True)
])
@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="Raw_DataQualityTests"
)
def Raw_DataQualityTests():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/Data_Quality_Tests/*"
    Raw_DataQualityTests = spark.readStream.format("parquet").schema(schema).option("path", path).load()
    # Raw_DataQualityTests.createOrReplaceTempView("Raw_DataQualityTests")
    # display(Raw_DataQualityTests)
    return Raw_DataQualityTests

# COMMAND ----------

schema = StructType([
    StructField("Full_Name", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Asset_Id", StringType(), True),
    StructField("Asset_Type_Color", StringType(), True),
    StructField("Asset_Type_SymbolType", StringType(), True),
    StructField("Asset_Type_IconCode", StringType(), True),
    StructField("Asset_Type_AcronymCode", StringType(), True),
    StructField("Asset_Type_Display_Name_Enabled", BooleanType(), True),
    StructField("Domain", StringType(), True),
    StructField("Domain_Id", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Description_Id", StringType(), True),
    StructField("Predicate", StringType(), True),
    StructField("Predicate_Id", StringType(), True),
    StructField("Data_Quality_Rule_governs_Column_Name", StringType(), True),
    StructField("Data_Quality_Rule_governs_Column_Full_Name", StringType(), True),
    StructField("Data_Quality_Rule_governs_Column_Asset_Id", StringType(), True),
    StructField("SourceRelation_00000000_0000_0000_0000_090000010022_Id", StringType(), True),
    StructField("Rule_is_implemented_by_Business_Rule_Name", StringType(), True),
    StructField("Rule_is_implemented_by_Business_Rule_Full_Name", StringType(), True),
    StructField("Rule_is_implemented_by_Business_Rule_Asset_Id", StringType(), True),
    StructField("SourceRelation_00000000_0000_0000_0000_000000007014_Id", StringType(), True),
    StructField("Asset_Type", StringType(), True),
    StructField("Asset_Type_Discriminator", StringType(), True),
    StructField("Asset_Type_Is_System", BooleanType(), True),
    StructField("Asset_Type_Id", StringType(), True),
    StructField("Asset_Type_Public_Id", StringType(), True),
    StructField("Asset_Type_Final_Type", BooleanType(), True),
    StructField("Asset_Type_Lock_Statuses", BooleanType(), True),
    StructField("Asset_Type_Product", StringType(), True),
    StructField("dataset_Name", StringType(), True),
    StructField("snapshot_date", StringType(), True)
])

@dlt.table(
    comment="This is just a view of the information to be used in the dashboard",
    name="Raw_DataQualityRules"
)
def Raw_DataQualityRules():
    path = "/Volumes/ds_goc_volumes_dev/external_data/ma-ds-goc-prd-prod-storage-layer-eu-central-1/raw_goc_nosara_lkh/Data_Quality_Rules/*"
    Raw_Data_Quality_Rules = spark.readStream.format("parquet").schema(schema).option("path", path).load()
    # Raw_DataQualityRules.createOrReplaceTempView("Raw_DataQualityRules")
    # display(Raw_DataQualityRules)
    return Raw_Data_Quality_Rules
