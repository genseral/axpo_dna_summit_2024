# Databricks notebook source
# MAGIC %pip install --force-reinstall dbdemos

# COMMAND ----------

import dbdemos

# COMMAND ----------

dbdemos.help()

# COMMAND ----------

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('aibi-marketing-campaign', use_current_cluster=True, overwrite=True, catalog='dbw_databricks_dna_hackathon_databricks_stream', schema='guido_oswald')
