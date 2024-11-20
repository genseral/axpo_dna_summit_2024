# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Simplify ETL with Delta Live Table
# MAGIC
# MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you.
# MAGIC
# MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-1.png" width="700"/>
# MAGIC
# MAGIC **Accelerate ETL development** <br/>
# MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC
# MAGIC **Remove operational complexity** <br/>
# MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC
# MAGIC **Trust your data** <br/>
# MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC
# MAGIC **Simplify batch and streaming** <br/>
# MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC
# MAGIC ## Our Delta Live Table pipeline
# MAGIC
# MAGIC We will be using three tables that have already been defined as the starting point of our DLT pipeline. 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Our tables have already been ingested by the instructor, and reside in the 'source' catalog and the 'raw' schema:
# MAGIC
# MAGIC * raw_txs (loans uploader here in every few minutes)
# MAGIC * ref_accounting_treatment (reference table, mostly static)
# MAGIC * raw_historical_loans (loan from legacy system, new data added every week)
# MAGIC
# MAGIC As a first step, we are going to reference these tables in our DLT pipeline, as if they were ingested here. 
# MAGIC
# MAGIC We use the keyword `INCREMENTAL` and `stream()` to indicate we are incrementally loading data. Without `INCREMENTAL` you will scan and ingest all the data available at once. 

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import col, weekofyear, year, expr, round, avg, to_date, concat, lit, sum, window, date_format
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

source = '/Volumes/axpo_dna_summit/hackathon_dataset/datasets/data/'

# COMMAND ----------

@dlt.table(
    name = f"raw_carbon_eua",
    comment = "Raw data power data from france",
)
@dlt.expect_or_drop("vaild_volume", F.col("clearing_volume") != 'NULL')
def raw_carbon_eua():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{source}dataset_3")
        .select("*")
    )

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC
# MAGIC ## Silver layer: joining tables while ensuring data quality
# MAGIC
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-3.png" width="600"/>
# MAGIC
# MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the `LIVE` spacename. 
# MAGIC
# MAGIC To consume only increment from the Bronze layer like `raw_txs`, we'll be using the `stream` keyworkd: `stream(LIVE.raw_txs)`
# MAGIC
# MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
# MAGIC
# MAGIC #### Expectations
# MAGIC By defining expectations (`CONSTRAINT <name> EXPECT <condition>`), you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

# COMMAND ----------

@dlt.table(
    comment="Cleaned raw power data from france"
)
def silver_CO2_EUA():
    raw_power_fra =  dlt.read("raw_carbon_eua")

    raw_power_fra = raw_power_fra.filter(F.col("DeliveryStartDate") <= F.col("DeliveryEndDate"))
    raw_power_fra = raw_power_fra.filter(F.col("DeliveryStartDate") == F.col("AdjustedDeliveryStartDate"))
    raw_power_fra = raw_power_fra.filter(F.col("DeliveryEndDate") == F.col("AdjustedDeliveryEndDate"))
    
    return (
        raw_power_fra.select(
                col("TimeSeries_FID"),
                col("PublicationDateIndex_FID"),
                col("PublicationDate"),
                col("QuoteDateIndex_FID"),
                col("QuoteTime"),
                col("DeliveryGridPointName"),
                col("DeliveryStartDate"),
                col("DeliveryEndDate"),
                col("DeliveryRelativeBucketName"),
                col("DeliveryRelativeBucketNumber"),
                col("clearing_price"),
                col("clearing_volume"),
                col("Point_ID")
            )
    )

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC
# MAGIC ## Gold layer
# MAGIC
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-4.png" width="600"/>
# MAGIC
# MAGIC Our last step is to materialize the Gold Layer.
# MAGIC
# MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using `pipelines.autoOptimize.zOrderCols`, and DLT will handle the rest.

# COMMAND ----------

@dlt.table(
    comment="Aggregated Power data from france"
)
def gold_CO2_EUA():
    return (
        dlt.read("silver_CO2_EUA")
            .select(
            '*'
            )
)
