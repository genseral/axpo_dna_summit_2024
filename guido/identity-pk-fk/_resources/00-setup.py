# Databricks notebook source
# MAGIC %md 
# MAGIC #### Setup for PK/FK demo notebook
# MAGIC
# MAGIC Hide this notebook result.

# COMMAND ----------

import pyspark.sql.functions as F
import re

#catalog = "uc_demos_"+current_user_no_at
catalog = "dbw_databricks_dna_hackathon_databricks_stream"

catalog_exists = False
for r in spark.sql("SHOW CATALOGS").collect():
    if r['catalog'] == catalog:
        catalog_exists = True

#As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
if not catalog_exists:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
spark.sql(f"USE CATALOG {catalog}")

database = 'guido_oswald'
db_not_exist = len([db for db in spark.catalog.listDatabases() if db.name == database]) == 0
if db_not_exist:
  print(f"creating {database} database")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
  spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{database} TO `account users`")
  spark.sql(f"ALTER SCHEMA {catalog}.{database} OWNER TO `account users`")
spark.sql(f"USE DATABASE {database}")
