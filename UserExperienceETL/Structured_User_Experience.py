# Databricks notebook source
import dlt
from pyspark.sql.functions import col, hour, date_sub, trunc, add_months, to_date, unix_timestamp, when, trim, count, sum as _sum, last_day


# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_user_actions():
    raw_user_actions = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_user_actions")
    # raw_user_actions.createOrReplaceTempView("raw_user_actions")
    # display(raw_user_actions.limit(10))
    return raw_user_actions

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_user_sessions():
    raw_user_sessions = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_user_sessions")
    # raw_user_sessions.createOrReplaceTempView("raw_user_sessions")
    # display(raw_user_sessions.limit(10))
    return raw_user_sessions

# COMMAND ----------

@dlt.table(
    name="structured_user_sessions",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_user_sessions():
    raw_user_sessions = dlt.read("raw_user_sessions")
    raw_user_actions = dlt.read("raw_user_actions")

    accountClean = raw_user_actions.filter(col("accountname") != 'no_accountname') \
        .select("userSessionId", "accountname").distinct()

    account_df = accountClean.groupBy("userSessionId", "accountname") \
        .agg(count("userSessionId").alias("idControl")) \
        .filter(col("idControl") == 1)

    application_df = raw_user_actions.select("application", "elastic_id").distinct()

    sessionsClean = raw_user_sessions.alias("sessions") \
        .join(account_df.alias("account"), col("sessions.userSessionId") == col("account.userSessionId"), "left") \
        .join(application_df.alias("app"), col("sessions.elastic_id") == col("app.elastic_id"), "left") \
        .filter((col("sessions.userType") == 'REAL_USER') & 
                (col("app.application").isin('Compliance Catalyst v2 (AWS)', 'Orbis (AWS)'))) \
        .select(
            col("sessions.elastic_id"),
            col("sessions.userSessionId"),
            when(trim(col("sessions.userId")) == 'no_user', 'no_user')
                .when(trim(col("sessions.userId")).isNull(), 'no_user')
                .otherwise(col("sessions.userId")).alias("userId"),
            when(trim(col("sessions.country")) == 'no_country', 'no_country')
                .when(trim(col("sessions.country")).isNull(), 'no_country')
                .otherwise(col("sessions.country")).alias("country"),
            col("sessions.userActionCount"),
            col("sessions.numberOfRageClicks"),
            col("sessions.duration").alias("session_duration"),
            when(trim(col("account.accountname")).isNull(), 'no_accountname')
                .when(trim(col("account.accountname")) == 'n.a', 'no_accountname')
                .otherwise(trim(col("account.accountname"))).alias("accountname"),
            col("sessions.browserFamily"),
            col("sessions.userExperienceScore").alias("session_userExperienceScore"),
            col("sessions.browserMajorVersion"),
            col("app.application"),
            col("sessions.userType"),
            col("sessions.startTime_utc").cast("date").alias("snapshot_date")
        )

    sessionsClean = sessionsClean.withColumn("end_of_month", last_day(col("snapshot_date")))
    # sessionsDim.createOrReplaceTempView("sessionsClean")
    display(sessionsClean.limit(10))
    return sessionsClean

# COMMAND ----------

@dlt.table(
    name="structured_user_actions",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_user_actions():
    raw_user_sessions = dlt.read("raw_user_sessions")
    raw_user_actions = dlt.read("raw_user_actions")
    
    accountClean = raw_user_actions.filter(col("accountname") != 'no_accountname').select("userSessionId", "accountname").distinct()
    
    account_df = accountClean.groupBy("userSessionId", "accountname").agg(count("userSessionId").alias("idControl")).filter(col("idControl") == 1)
    
    actionsClean = raw_user_actions.alias("ua") \
        .join(raw_user_sessions.alias("us"), col("ua.elastic_id") == col("us.elastic_id"), "left") \
        .join(account_df.alias("ad"), col("ua.userSessionId") == col("ad.userSessionId"), "left") \
        .filter(col("ua.application").isin('Compliance Catalyst v2 (AWS)', 'Orbis (AWS)')) \
        .select(
            col("ua.elastic_id"),
            col("ua.userSessionId"),
            col("ua.startTime_utc"),
            hour(col("ua.startTime_utc")).alias("startTime_utc_hour"),
            when(col("ua.apdexCategory") == 'TOLERATING', 'TOLERATED').otherwise(col("ua.apdexCategory")).alias("apdexCategory"),
            col("ua.application"),
            col("ua.duration"),
            (col("ua.duration").cast("float") / 1000).alias("duration_seconds"),
            col("ua.name"),
            col("ua.targetUrl"),
            col("ua.type"),
            col("ua.threadname"),
            col("ua.reportbooksection"),
            col("ua.searchby"),
            col("ad.accountname"),
            col("us.userId"),
            col("us.country"),
            col("us.startTime_utc").cast("date").alias("snapshot_date")
        )
    
    # actionsClean.createOrReplaceTempView("actionsDim")
    actionsClean = actionsClean.withColumn("end_of_month", last_day(col("snapshot_date")))
    display(actionsClean.limit(10))
    return actionsClean
