# Databricks notebook source
# MAGIC %pip install --force-reinstall dbdemos

# COMMAND ----------

import dbdemos

# COMMAND ----------

dbdemos.help()

# COMMAND ----------

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('aibi-customer-support', use_current_cluster=True, overwrite=True, catalog='dbw_databricks_dna_hackathon_databricks_stream', schema='guido_oswald')

# COMMAND ----------

dbdemos.install('aibi-marketing-campaign', use_current_cluster=True, overwrite=True, catalog='dbw_databricks_dna_hackathon_databricks_stream', schema='guido_oswald')

# COMMAND ----------

dbdemos.install('lakehouse-fsi-fraud', use_current_cluster=True, overwrite=True, catalog='dbw_databricks_dna_hackathon_databricks_stream', schema='guido_oswald')

# COMMAND ----------

dbdemos.install('lakehouse-iot-platform', use_current_cluster=True, overwrite=True, catalog='dbw_databricks_dna_hackathon_databricks_stream', schema='guido_oswald')
