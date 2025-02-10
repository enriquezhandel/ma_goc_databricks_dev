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
def raw_status_page():
    raw_status_page = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_status_page")
    # raw_status_page.createOrReplaceTempView("raw_status_page")
    # display(raw_status_page.limit(10))
    return raw_status_page

# COMMAND ----------

@dlt.table(
    name="structured_statuspage",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_status_page():
    raw_status_page = dlt.read("raw_status_page")
    
    # Filter out rows where StatusPageName contains 'old_version'
    filtered_raw_status_page = raw_status_page.filter(~col("StatusPageName").contains("old_version"))
    
    # Sort the dataset by product name before making the grouping and collect list
    sorted_raw_status_page = filtered_raw_status_page.orderBy("StatusPageName")
    
    # Group by Email, FirstName, LastName and aggregate the products
    PivotFinDF = sorted_raw_status_page.groupBy(
        "Email", "FirstName", "LastName", "fDashBoard", "CompanyNameSource", "snapshot_date"
    ).agg(
        F.collect_list("StatusPageName").alias("StatusPageNameList"),
        F.count("StatusPageName").alias("StatusPageNameCount")
    ).withColumn(
        "StatusPageNameList", F.expr("array_distinct(StatusPageNameList)")
    ).withColumn(
        "StatusPageName", F.expr("array_join(StatusPageNameList, ';')")
    ).drop("StatusPageNameList").orderBy("Email")
    
    # Define a regex pattern for email validation
    email_regex = '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    # Rename the column fDashboard to Notification
    PivotFinDF = PivotFinDF.withColumnRenamed("fDashBoard", "Notification")

    # Classify emails as valid or invalid using Spark's `when` and `rlike`
    structured_status_page = PivotFinDF.withColumn(
        "EmailValidity",
        when(col("Email").rlike(email_regex), "valid").otherwise("invalid")
    ).withColumn(
        "EmailType",
        when(col("Email").rlike(".*(moodys|bvd).*"), "internal").otherwise("outside")
    ).withColumn(
        "BlankStatus",
        when(col("Email") == "", "blankEmail")
        .when(col("FirstName") == "", "blankName")
        .when(col("LastName") == "", "blankLastName")
        .when((col("Email") == "") & (col("FirstName") == ""), "blankEmail;blankName")
        .when((col("Email") == "") & (col("LastName") == ""), "blankEmail;blankLastName")
        .when((col("FirstName") == "") & (col("LastName") == ""), "blankName;blankLastName")
        .when((col("Email") == "") & (col("FirstName") == "") & (col("LastName") == ""), "blankEmail;blankName;blankLastName")
        .otherwise("valid")
    )
    # display(structured_status_page.limit(10))
    return structured_status_page
