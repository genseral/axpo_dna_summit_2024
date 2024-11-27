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
# MAGIC We will be using one table of the provided AXPO data about clearing prices/volume of power data from France.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The CSV data that we utilize here is already landed on a volume and is ready to be ingested from there. In this simple example you will just find one CSV file on that volume (_/Volumes/dbw-databricks-dna-hackathon-databricks-stream/alexander_genser/data_volume/data/_). The CSV named 'dataset_3.csv' contains the clearing prices/volume of the data from France and Germany. Note that in a production scenario, new recores of data will arrive in batches or in a streaming fashion. Nevertheless, the technology to ingest and transform the data remains, [Autoloader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/) is the perfect tool for such use-cases as Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage.
# MAGIC
# MAGIC We use the keyword `INCREMENTAL` and `readstream()` to indicate we are incrementally loading data. Without `INCREMENTAL` or by just using `read()`  you will scan and ingest all the data available at once. 

# COMMAND ----------

# importing libraries
import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import col, weekofyear, year, expr, round, avg, to_date, concat, lit, sum, window, date_format
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

# path to volume where data is landed (you can find the volume in Unity catalog)
source = '/Volumes/dbw_databricks_dna_hackathon_databricks_stream/02_data_product_trading/data_volume/data/'

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # What is Databricks Auto Loader?
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader/autoloader-edited-anim.gif" style="float:right; margin-left: 10px" />
# MAGIC
# MAGIC [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html) lets you scan a cloud storage folder (S3, ADLS, GS) and only ingest the new data that arrived since the previous run.
# MAGIC
# MAGIC This is called **incremental ingestion**.
# MAGIC
# MAGIC Auto Loader can be used in a near real-time stream or in a batch fashion, e.g., running every night to ingest daily data.
# MAGIC
# MAGIC Auto Loader provides a strong gaurantee when used with a Delta sink (the data will only be ingested once).
# MAGIC
# MAGIC ## How Auto Loader simplifies data ingestion
# MAGIC
# MAGIC Ingesting data at scale from cloud storage can be really hard at scale. Auto Loader makes it easy, offering these benefits:
# MAGIC
# MAGIC
# MAGIC * **Incremental** & **cost-efficient** ingestion (removes unnecessary listing or state handling)
# MAGIC * **Simple** and **resilient** operation: no tuning or manual code required
# MAGIC * Scalable to **billions of files**
# MAGIC   * Using incremental listing (recommended, relies on filename order)
# MAGIC   * Leveraging notification + message queue (when incremental listing can't be used)
# MAGIC * **Schema inference** and **schema evolution** are handled out of the box for most formats (csv, json, avro, images...)
# MAGIC
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&org_id=1444828305810485&notebook=%2F01-Auto-loader-schema-evolution-Ingestion&demo_name=auto-loader&event=VIEW&path=%2F_dbdemos%2Fdata-engineering%2Fauto-loader%2F01-Auto-loader-schema-evolution-Ingestion&version=1">
# MAGIC
# MAGIC <img style="float: center; padding-left: 10px" src="https://github.com/genseral/axpo_dna_summit_2024/blob/main/figures/Power_data_simple_DLT_bronze.drawio.png?raw=true" width="600"/>

# COMMAND ----------

@dlt.table(
    name = f"bronze_simple_power",
    comment = "Raw data power data from france and germany",
)
@dlt.expect_or_drop("valid_price", F.col("clearing_price") >= 0)
@dlt.expect_or_drop("valid_StartDate", F.col("DeliveryStartDate") <= F.col("DeliveryEndDate"))
@dlt.expect_or_drop("no_adjustments_start_date", F.col("DeliveryStartDate") == F.col("AdjustedDeliveryStartDate"))
@dlt.expect_or_drop("no_adjustments_end_date", F.col("DeliveryEndDate") == F.col("AdjustedDeliveryEndDate"))
def bronze_simple_power():
    """
    Reads raw power data from a specified source directory, applies data quality expectations,
    and returns a streaming DataFrame for further processing.
    """
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
# MAGIC <img style="float: center; padding-left: 10px" src="https://github.com/genseral/axpo_dna_summit_2024/blob/main/figures/Power_data_simple_DLT_silver.drawio.png?raw=true" width="600"/>
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
    name = f"silver_simple_power_fra",
    comment="Cleaned raw power data from france"
)
def silver_simple_power_fra():
    """
    Reads raw power data from the bronze table, filters for France-specific data,
    and selects relevant columns for further processing.
    """
    t_id = 112047001
    raw_power_fra =  dlt.read("bronze_simple_power")
    raw_power_fra = raw_power_fra.filter(F.col("TimeSeries_FID") == t_id)
    
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

@dlt.table(
    name = f"silver_simple_power_ger",
    comment="Cleaned raw power data from germany"
)
def silver_simple_power_ger():
    """
    Reads raw power data from the bronze table, filters for Germany-specific data,
    and selects relevant columns for further processing.
    """
    t_id = 112069251
    raw_power_fra =  dlt.read("bronze_simple_power")
    raw_power_fra = raw_power_fra.filter(F.col("TimeSeries_FID") == t_id)

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
# MAGIC <img style="float: center; padding-left: 10px" src="https://github.com/genseral/axpo_dna_summit_2024/blob/main/figures/Power_data_simple_DLT_gold.drawio.png?raw=true" width="600"/>
# MAGIC
# MAGIC Our last step is to materialize the Gold Layer.
# MAGIC
# MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using `pipelines.autoOptimize.zOrderCols`, and DLT will handle the rest.

# COMMAND ----------

@dlt.table(
    name = f"gold_simple_power_fra",
    comment="Aggregated Power data from france"
)
def gold_simple_power_fra():
    """
    Aggregates cleaned power data from the silver table for France,
    calculates average clearing price and total clearing volume per delivery date.
    """
    return (
        dlt.read("silver_simple_power_fra")
            .withColumn("DeliveryDate", F.to_date("DeliveryStartDate"))
            .select("DeliveryDate", "clearing_price", "clearing_volume")
            .groupBy("DeliveryDate")
            .agg(
                F.round(F.avg("clearing_price"), 0).alias("avg_clearing_price"),
                F.sum("clearing_volume").alias("total_clearing_volume")
            )
            .orderBy("DeliveryDate")
    )

# COMMAND ----------

@dlt.table(
    name = f"gold_simple_power_ger",
    comment="Aggregated Power data from germany"
)
def gold_simple_power_ger():
    """
    Aggregates cleaned power data from the silver table for Germany,
    calculates average clearing price and total clearing volume per delivery date.
    """
    return (
        dlt.read("silver_simple_power_ger")
            .withColumn("DeliveryDate", F.to_date("DeliveryStartDate"))
            .select("DeliveryDate", "clearing_price", "clearing_volume")
            .groupBy("DeliveryDate")
            .agg(
                F.round(F.avg("clearing_price"), 0).alias("avg_clearing_price"),
                F.sum("clearing_volume").alias("total_clearing_volume")
            )
            .orderBy("DeliveryDate")
    )