# Databricks notebook source
import dlt 
from pyspark.sql.functions import col, hour, date_sub, trunc, add_months, to_date, unix_timestamp, when, trim, count, sum, last_day

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def structured_user_actions():
    structured_user_actions = spark.read.table("ds_goc_bronze_dev.ds_goc_silver_dev.structured_user_actions")
    # display(structured_user_actions.limit(10))
    return structured_user_actions

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def structured_user_sessions():
    structured_user_sessions = spark.read.table("ds_goc_bronze_dev.ds_goc_silver_dev.structured_user_sessions")
    structured_user_sessions.createOrReplaceTempView("structured_user_sessions")
    # display(structured_user_sessions.limit(10))
    return structured_user_sessions

# COMMAND ----------

@dlt.table(
    name="curated_user_actions_comCatV2",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "gold"}
)
def curated_user_actions_comCatV2():
    structured_user_actions = dlt.read("structured_user_actions")
    
    curated_user_actions_comCatV2 = structured_user_actions.filter(structured_user_actions.application == 'Compliance Catalyst v2 (AWS)')
    
    # display(curated_user_actions_comCatV2.limit(10))
    return curated_user_actions_comCatV2

# COMMAND ----------

@dlt.table(
    name="curated_user_actions_OrbisAWS",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "gold"}
)
def curated_user_actions_comCatV2():
    structured_user_actions = dlt.read("structured_user_actions")
    
    curated_user_actions_comCatV2 = structured_user_actions.filter(structured_user_actions.application == 'Orbis (AWS)')
    
    # display(curated_user_actions_comCatV2.limit(10))
    return curated_user_actions_comCatV2

# COMMAND ----------

@dlt.table(
    name="curated_user_actions",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "gold"}
)
def curated_user_actions():
    structured_user_actions = dlt.read("structured_user_actions")

    curated_user_actions = structured_user_actions.groupBy(
        "startTime_utc_hour",
        "apdexCategory",
        "application",
        "duration_seconds",
        "threadname",
        "accountname",
        "country",	
        "snapshot_date",
        "end_of_month"
    ).agg(
        count("userSessionId").alias("userSessionIdTotal")
    )

    # display(curated_user_actions.limit(10))
    return curated_user_actions

# COMMAND ----------

@dlt.table(
    name="curated_user_sessions",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "gold"}
)
def curated_user_sessions():
    structured_user_sessions = dlt.read("structured_user_sessions")

    sessionsDim = structured_user_sessions.groupBy(
        "country",
        "accountname",
        "session_userExperienceScore",
        "application",
        "snapshot_date",
        "end_of_month"
    ).agg(
        count("userSessionId").alias("userSessionIdTotal"),
        sum("userActionCount").alias("userActionCount"),
        sum("numberOfRageClicks").alias("numberOfRageClicks")
    )

    # display(sessionsDim.limit(10))
    return sessionsDim
