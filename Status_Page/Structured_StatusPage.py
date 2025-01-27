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
    # raw_user_actions.createOrReplaceTempView("raw_user_actions")
    # display(raw_user_actions.limit(10))
    return raw_status_page

# COMMAND ----------

@dlt.table(
    name="structured_statuspage",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_status_page():
    raw_status_page = dlt.read("raw_status_page")
    # Filter the dataset to remove the old_version for the column StatusPageName
    # CleanFinDF = raw_status_page.filter(col("StatusPageName") != "old_version")

    # Group by Email, FirstName, LastName and aggregate the products
    PivotFinDF = raw_status_page.groupBy("Email", "FirstName", "LastName","fDashBoard","snapshot_date") \
                .agg(F.collect_list("productName").alias("productName"),
                    F.count("productName").alias("controlProduct")) \
                .withColumn("productName", F.expr("array_distinct(productName)")) \
                .withColumn("productName", F.expr("array_join(productName, ';')")) \
                .orderBy("Email")
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
    # display(structured_status_page)
    return structured_status_page

