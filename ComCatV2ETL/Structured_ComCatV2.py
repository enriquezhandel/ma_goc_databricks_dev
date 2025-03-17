# Databricks notebook source
import dlt
from pyspark.sql import Window, functions as F; from pyspark.sql.functions import col, hour, date_sub, trunc, add_months, to_date, unix_timestamp, when, trim, count, sum as _sum, expr, avg, stddev, last_day

# COMMAND ----------

@dlt.table(
    name="structured_comcatV2_ra_model",
    comment="Curated version of the table for RAModel Transformation",
    table_properties={"quality": "silver"}
)
def delta_live_comcatv2_ra_model():
    df = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_comcatv2_ra_model")
    
    df = df.withColumn("ProductName", 
                       when(col("ProductId") == 171, "ComplianceV2")
                       .when(col("ProductId") == 173, "CreditCatalyst")
                       .when(col("ProductId") == 174, "ProcurementCatalyst"))
    
    df = df.withColumn("waiting_seconds", col("QueueDurationInMS") / 1000) \
           .withColumn("execution_seconds", col("ComputeDurationInMS") / 1000) \
           .withColumn("duration_seconds", col("TotalDurationInMS") / 1000)

    df.select("RAid", "RequestedOn", "UserName", "AccountName", "UserId", "SharingId", 
                     "ProductId", "ProductName", "TaskType", "QueueDurationInMS", 
                     "ComputeDurationInMS", "TotalDurationInMS", "modelID", 
                     "waiting_seconds", "execution_seconds", "duration_seconds", "snapshot_date")
    df.createOrReplaceTempView("structured_comcatv2_ra_model")
    return df

# COMMAND ----------

@dlt.table(
    name="structured_comcatV2_ra_model_kpis",
    comment="Aggregated metrics for RAModel Transformation",
    table_properties={"quality": "silver"}
)
def delta_live_aggregated_comcatv2_ra_model():
    df = spark.table("structured_comcatv2_ra_model")
    
    window_spec = Window.partitionBy("snapshot_date", "AccountName", "ProductName")
    
    df = df.withColumn("avg_waiting_seconds", avg("waiting_seconds").over(window_spec)) \
           .withColumn("median_waiting_seconds", expr("percentile_approx(waiting_seconds, 0.5)").over(window_spec)) \
           .withColumn("p90_waiting_seconds", expr("percentile_approx(waiting_seconds, 0.9)").over(window_spec)) \
           .withColumn("p95_waiting_seconds", expr("percentile_approx(waiting_seconds, 0.95)").over(window_spec)) \
           .withColumn("avg_execution_seconds", avg("execution_seconds").over(window_spec)) \
           .withColumn("median_execution_seconds", expr("percentile_approx(execution_seconds, 0.5)").over(window_spec)) \
           .withColumn("p90_execution_seconds", expr("percentile_approx(execution_seconds, 0.9)").over(window_spec)) \
           .withColumn("p95_execution_seconds", expr("percentile_approx(execution_seconds, 0.95)").over(window_spec)) \
           .withColumn("avg_duration_seconds", avg("duration_seconds").over(window_spec)) \
           .withColumn("median_duration_seconds", expr("percentile_approx(duration_seconds, 0.5)").over(window_spec)) \
           .withColumn("p90_duration_seconds", expr("percentile_approx(duration_seconds, 0.9)").over(window_spec)) \
           .withColumn("p95_duration_seconds", expr("percentile_approx(duration_seconds, 0.95)").over(window_spec))
    
    df = df.select("AccountName", "SharingId", "ProductId", "ProductName", "snapshot_date", 
                   "avg_waiting_seconds", "median_waiting_seconds", "p90_waiting_seconds", "p95_waiting_seconds", 
                   "avg_execution_seconds", "median_execution_seconds", "p90_execution_seconds", "p95_execution_seconds", 
                   "avg_duration_seconds", "median_duration_seconds", "p90_duration_seconds", "p95_duration_seconds").distinct()
    
    return df

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def delta_live_jdbc_accounts():
    delta_live_jdbc_accounts = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.delta_live_jdbc_accounts")
    # delta_live_jdbc_accounts.createOrReplaceTempView("delta_live_jdbc_accounts")
    # display(delta_live_jdbc_accounts.limit(10))
    return delta_live_jdbc_accounts

# COMMAND ----------

# Retrieve secrets on SQL Server
auxDocs = dbutils.secrets.get(scope="goc_secrets", key="auxDocs")

# COMMAND ----------

@dlt.view(
    comment="This is a view of the information from the S3 document"
)
def enums_output_view():
    enums_output_df = spark.read.csv(auxDocs +"enums_output.csv", header=True, inferSchema=True)
    # enums_output_df.createOrReplaceTempView("enums_output_view")
    # display(enums_output_df.limit(10))
    return enums_output_df

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_comcatv2_alerts():
    raw_comcatv2_alerts = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_comcatv2_alerts")
    # raw_comcatv2_alerts.createOrReplaceTempView("raw_comcatv2_alerts")
    # display(raw_comcatv2_alerts.limit(10))
    return raw_comcatv2_alerts

# COMMAND ----------

@dlt.view(
    comment="This is just a view of the information to be used in the dashboard"
)
def raw_comcatv2_riskassesment():
    raw_comcatv2_riskassesment = spark.read.table("ds_goc_bronze_dev.ds_goc_bronze_dev.raw_comcatv2_riskassesments")
    # raw_comcatv2_riskassesment.createOrReplaceTempView("raw_comcatv2_riskassesment")
    # display(raw_comcatv2_riskassesment.limit(10))
    return raw_comcatv2_riskassesment

# COMMAND ----------

@dlt.table(
    name="structured_comcatV2_alerts",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_comcatV2_alerts():
    delta_live_jdbc_accounts = dlt.read("delta_live_jdbc_accounts")
    raw_comcatv2_alerts = dlt.read("raw_comcatv2_alerts")
    enums_output_view = dlt.read("enums_output_view")
    
    structured_comcatV2_alerts = raw_comcatv2_alerts.alias("dfAlerts") \
        .join(delta_live_jdbc_accounts.alias("dfAccount"), raw_comcatv2_alerts.SharingId == delta_live_jdbc_accounts.PrimaryAccountId, "left") \
        .join(enums_output_view.alias("dfClass"), raw_comcatv2_alerts.AlertCriteria == enums_output_view.Value, "left") \
        .selectExpr(
            "dfAlerts.SharingId",
            "CASE WHEN dfAccount.AccountName IS NULL THEN 'no_account' ELSE dfAccount.AccountName END AS AccountName",
            "dfAlerts.AlertCriteria",
            "CASE WHEN dfClass.Enum_Name IS NULL THEN dfAlerts.AlertCriteria ELSE dfClass.Enum_Name END AS alertName",
            "dfAlerts.Count",
            "dfAlerts.AlertDate",
            "hour(dfAlerts.AlertDate) AS hourAlert",
            "dfAlerts.snapshotdate",
            "date_sub(trunc(add_months(dfAlerts.snapshotdate, 1), 'MONTH'), 1) AS eom_snapshotdate"
        )
    
    # display(structured_comcatV2_alerts.limit(10))
    return structured_comcatV2_alerts

# COMMAND ----------

@dlt.table(
    name="structured_comcatV2_riskassesmentKPIs",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_comcatV2_riskassesmentKPIs():
    raw_comcatv2_riskassesment = dlt.read("raw_comcatv2_riskassesment")
    
    overall = raw_comcatv2_riskassesment.agg(
        avg("wait_seconds").alias("avg_waiting_seconds"),
        avg("execution_seconds").alias("avg_execution_seconds"),
        stddev("wait_seconds").alias("stddev_waiting_seconds"),
        stddev("execution_seconds").alias("stddev_execution_seconds")
    )
    
    partitioned = raw_comcatv2_riskassesment.groupBy("SharingId").agg(
        avg("wait_seconds").alias("avg_waiting_seconds"),
        avg("execution_seconds").alias("avg_execution_seconds"),
        count("*").alias("transaction_count")
    )
    
    weighted_avg = partitioned.agg(
        avg("avg_waiting_seconds").alias("weighted_avg_waiting_seconds"),
        avg("avg_execution_seconds").alias("weighted_avg_execution_seconds")
    )
    
    percentiles = raw_comcatv2_riskassesment.selectExpr(
        "percentile_approx(wait_seconds, 0.5) as median_waiting_seconds",
        "percentile_approx(execution_seconds, 0.5) as median_execution_seconds",
        "percentile_approx(wait_seconds, 0.90) as p90_waiting_seconds",
        "percentile_approx(wait_seconds, 0.95) as p95_waiting_seconds",
        "percentile_approx(execution_seconds, 0.90) as p90_execution_seconds",
        "percentile_approx(execution_seconds, 0.95) as p95_execution_seconds"
    )
    
    time_based = raw_comcatv2_riskassesment.groupBy(expr("DATE(RequestedOn)").alias("transaction_day")).agg(
        stddev("wait_seconds").alias("daily_stddev_waiting_seconds"),
        stddev("execution_seconds").alias("daily_stddev_execution_seconds"),
        expr("percentile_approx(wait_seconds, 0.5)").alias("daily_median_waiting_seconds"),
        expr("percentile_approx(execution_seconds, 0.5)").alias("daily_median_execution_seconds"),
        avg("wait_seconds").alias("daily_avg_waiting_seconds"),
        avg("execution_seconds").alias("daily_avg_execution_seconds"),
        count("*").alias("daily_num_task"),
        expr("percentile_approx(wait_seconds, 0.90)").alias("daily_p90_waiting_seconds"),
        expr("percentile_approx(execution_seconds, 0.90)").alias("daily_p90_execution_seconds"),
        expr("percentile_approx(wait_seconds, 0.95)").alias("daily_p95_waiting_seconds"),
        expr("percentile_approx(execution_seconds, 0.95)").alias("daily_p95_execution_seconds")
    ).orderBy("transaction_day")
    
    structured_comcatV2_riskassesmentKPIs = overall.crossJoin(weighted_avg).crossJoin(percentiles).crossJoin(time_based)
    structured_comcatV2_riskassesmentKPIs = structured_comcatV2_riskassesmentKPIs.withColumn("end_of_month", last_day(col("transaction_day")))

    # display(structured_comcatV2_riskassesmentKPIs.limit(10))
    return structured_comcatV2_riskassesmentKPIs

# COMMAND ----------

@dlt.table(
    name="structured_comcatV2_riskassesmentCompanyLvl",
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "silver"}
)
def structured_comcatV2_riskassesmentCompanyLvl():
    delta_live_jdbc_accounts = dlt.read("delta_live_jdbc_accounts")
    raw_comcatv2_riskassesment = dlt.read("raw_comcatv2_riskassesment")
    
    structured_comcatV2_riskassesmentCompanyLvl = raw_comcatv2_riskassesment.alias("dfRiskAssessments") \
        .join(delta_live_jdbc_accounts.alias("dfAccount"), raw_comcatv2_riskassesment.SharingId == delta_live_jdbc_accounts.PrimaryAccountId, "left") \
        .groupBy(
            "dfRiskAssessments.SharingId",
            expr("CASE WHEN dfAccount.AccountName IS NULL THEN 'no_account' ELSE dfAccount.AccountName END").alias("AccountName"),
            expr("CAST(dfRiskAssessments.StartedOn AS DATE)").alias("snapshot_date"),
            expr("date_sub(trunc(add_months(CAST(dfRiskAssessments.StartedOn AS DATE), 1), 'MONTH'), 1)").alias("eom_snapshotdate"),
            "dfRiskAssessments.ProductId"
        ) \
        .agg(
            count("dfRiskAssessments.Id").alias("num_tasks"),
            avg("dfRiskAssessments.execution_seconds").alias("average_execution_seconds"),
            avg("dfRiskAssessments.wait_seconds").alias("average_waiting_seconds"),
            expr("percentile_approx(dfRiskAssessments.execution_seconds, 0.5)").alias("median_execution_seconds"),
            expr("percentile_approx(dfRiskAssessments.wait_seconds, 0.5)").alias("median_waiting_seconds"),
            expr("percentile_approx(dfRiskAssessments.execution_seconds, 0.90)").alias("p90_execution_seconds"),
            expr("percentile_approx(dfRiskAssessments.wait_seconds, 0.90)").alias("p90_waiting_seconds"),
            expr("percentile_approx(dfRiskAssessments.execution_seconds, 0.95)").alias("p95_execution_seconds"),
            expr("percentile_approx(dfRiskAssessments.wait_seconds, 0.95)").alias("p95_waiting_seconds")
        ) \
        .withColumn("total_average_duration_seconds", expr("average_execution_seconds + average_waiting_seconds")) \
        .withColumn("total_median_duration_seconds", expr("median_execution_seconds + median_waiting_seconds"))
    
    # display(structured_comcatV2_riskassesmentCompanyLvl.limit(10))
    structured_comcatV2_riskassesmentCompanyLvl = structured_comcatV2_riskassesmentCompanyLvl.withColumn("end_of_month", last_day(col("snapshot_date")))
    return structured_comcatV2_riskassesmentCompanyLvl
