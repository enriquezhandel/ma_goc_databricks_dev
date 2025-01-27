# Databricks notebook source
# MAGIC %md
# MAGIC Aim of the code is to build data extraction for ComCatV2 for:
# MAGIC
# MAGIC - Alert Changing
# MAGIC - Account linkage

# COMMAND ----------

def JDBC_connection(server,db,username,password,table):
    # Set up the JDBC connection properties
    jdbc_url = (f"jdbc:{server};"
        f"databaseName={db};"
        "encrypt=true;"
        "trustServerCertificate=true")
    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    # Read data from SQL Server view
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table,
        properties=connection_properties
    )
    # Display the DataFrame
    return df

# COMMAND ----------

# Retrieve secrets on SQL Server
sqlServer = dbutils.secrets.get(scope="goc_secrets", key="sqlQIFServer")
jdbc_username = dbutils.secrets.get(scope="goc_secrets", key="sqlQIFUser")
jdbc_password = sqlQIFPass = dbutils.secrets.get(scope="goc_secrets", key="sqlQIFPass")

# ALERT TABLE ONLY 
sqlDBCatalystMVC = dbutils.secrets.get(scope="goc_secrets", key="sqlDBCatalystMVC")
tableAlerts = sqlQIFPass = dbutils.secrets.get(scope="goc_secrets", key="sqlTableCatalystAlerts")

# ACCOUNTS TABLE ONLY
sqlDBFinUsersAccount = dbutils.secrets.get(scope="goc_secrets",key="sqlDBFinUsersAccount")
tableAccounts = sqlQIFPass = dbutils.secrets.get(scope="goc_secrets", key="sqlTableFinUsersAccount")


# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# Define the Delta Live Table
@dlt.table(
    comment="This table reads and processes data from a source and writes it to a Delta table",
    table_properties={"quality": "test"},
)
def delta_live_jdbc_accounts():   
    dfAccounts = JDBC_connection(sqlServer, sqlDBFinUsersAccount, jdbc_username, jdbc_password, tableAccounts)
    dfAccounts.createOrReplaceTempView("dfAccounts")
    dfAccountClean = spark.sql("""
        WITH accountControl AS(
        SELECT  
            PrimaryAccountId,
            AccountHierarchyId,
            len(AccountHierarchyId) as controlParent,
            MIN(len(AccountHierarchyId)) OVER (PARTITION BY PrimaryAccountId) AS hierachyControl,
            Active,
            AccountName,
            CompanyName
        FROM dfAccounts)
        
        SELECT DISTINCT
            PrimaryAccountId,
            AccountHierarchyId,
            controlParent,
            hierachyControl,
            Active,
            AccountName,
            CompanyName
        FROM accountControl
        WHERE controlParent = hierachyControl
    """)
    dfAccountClean.createOrReplaceTempView("dfAccountClean")
    return dfAccountClean
