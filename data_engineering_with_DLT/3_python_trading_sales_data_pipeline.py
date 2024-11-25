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
# MAGIC Finally we will be using all the four data sources together and build one pipeline that
# MAGIC - transforms all the data together
# MAGIC - uses the FX rate to convert the pricing of the coal dataset (dataset_2.csv) to EUR. 
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The datasets we are going to use are the following:
# MAGIC
# MAGIC Feel free to choose on of the 4 CSV files that are available for a try-out:
# MAGIC * dataset_1.csv -> Carbon emission trading specifically European Union Allowances (EUA)
# MAGIC * dataset_2.csv -> Coal pricing for API 2 
# MAGIC * dataset_3.csv -> French electricity market, specifically focusing on power futures contracts
# MAGIC * dataset_4.csv -> -	Foreign exchange (FX) rate information EUR-USD
# MAGIC
# MAGIC As a first step, we are going to ingest all the four datasets and create the corresponding bronze tables. 
# MAGIC
# MAGIC We use the keyword `INCREMENTAL` and `stream()` to indicate we are incrementally loading data. Without `INCREMENTAL` you will scan and ingest all the data available at once. 

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import col, weekofyear, year, expr, round, avg, to_date, concat, lit, sum, window, date_format
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

# path to raw data landing zone
source = '/Volumes/dbw_databricks_dna_hackathon_databricks_stream/02_data_product_trading/data_volume/data/'

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC
# MAGIC ### Bronze layer:
# MAGIC
# MAGIC <img style="float: left; padding-left: 10px" src="https://github.com/genseral/axpo_dna_summit_2024/blob/main/figures/Full_DLT_bronze.drawio.png?raw=true" width="600"/>
# MAGIC

# COMMAND ----------

@dlt.table(
    name = "raw_CO2_EUA",
    comment = "Raw data from CO2_EUA",
)
@dlt.expect_or_fail("positive_settlement_price", F.col("settlement_price") > 0)
@dlt.expect_or_fail("traded_volume_null", (F.col("traded_volume") == 'NULL'))
@dlt.expect_or_fail("open_interest_null", (F.col("open_interest") == 'NULL'))
@dlt.expect_or_fail("block_volume_null", (F.col("Block_Volume") == 'NULL'))
@dlt.expect_or_fail("spread_volume_null", (F.col("Spread_Volume") == 'NULL'))
def raw_carbon_eua():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{source}dataset_1")
        .select("*")
    )

# COMMAND ----------

@dlt.table(
    name = "raw_COAL_API2",
    comment = "Raw data from coal api2",
)
def raw_carbon_eua():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{source}dataset_2")
        .select("*")
    )

# COMMAND ----------

@dlt.table(
    name = "raw_POWER_FRA",
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

@dlt.table(
    name = "raw_FX_EURUSD",
    comment = "Raw data for FX from euro to usd",
)
def raw_carbon_eua():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{source}dataset_4")
        .select("*")
    )

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC
# MAGIC ### Silver layer:
# MAGIC
# MAGIC <img style="float: left; padding-left: 10px" src="https://github.com/genseral/axpo_dna_summit_2024/blob/main/figures/Full_DLT_silver.drawio.png?raw=true" width="600"/>
# MAGIC

# COMMAND ----------

@dlt.table(
    comment="Cleaned raw data for CO2 emissions from eua"
)
def silver_CO2_EUA():
    raw_co2_eua = dlt.read("raw_CO2_EUA")
    
    raw_co2_eua = raw_co2_eua.filter(F.col("DeliveryStartDate") <= F.col("DeliveryEndDate"))
    raw_co2_eua = raw_co2_eua.filter(F.col("DeliveryStartDate") == F.col("AdjustedDeliveryStartDate"))
    raw_co2_eua = raw_co2_eua.filter(F.col("DeliveryEndDate") == F.col("AdjustedDeliveryEndDate"))
    return (
        raw_co2_eua.select(
                col("TimeSeries_FID"),
                col("QuoteDateIndex_FID"),
                col("QuoteTime"),
                col("PublicationDateIndex_FID"),
                col("PublicationDate"),
                col("DeliveryGridPointName"),
                col("DeliveryStartDate"),
                col("DeliveryEndDate"),
                col("DeliveryRelativeBucketName"),
                col("DeliveryRelativeBucketNumber"),
                col("settlement_price"),
                col("Point_ID")
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Cleaned raw data for coal api2"
)
def silver_COAL_API2():
    raw_fx = dlt.read("raw_FX_EURUSD")
    raw_coal_api2 = dlt.read("raw_COAL_API2")
    
    raw_coal_api2 = raw_coal_api2.filter(F.col("DeliveryStartDate") <= F.col("DeliveryEndDate"))
    raw_coal_api2 = raw_coal_api2.filter(F.col("DeliveryStartDate") == F.col("AdjustedDeliveryStartDate"))
    raw_coal_api2 = raw_coal_api2.filter(F.col("DeliveryEndDate") == F.col("AdjustedDeliveryEndDate"))

    # First, fix the computation of the 'date' column to correctly use 'next_day'
    raw_fx_avg = raw_fx.withColumn("week", weekofyear("DeliveryStartDate")) \
                    .withColumn("year", year("DeliveryStartDate")) \
                    .groupBy("week", "year") \
                    .agg(round(avg("FX_Rate"), 2).alias("fx_rate_weekly_avg")) \
                    .withColumn("week_starting_sunday", expr("date_add(next_day(to_date(concat(year, '-01-01')), 'Sunday'), (week - 1) * 7)")) \
                    .withColumn("DeliveryStartDate", expr("date_sub(week_starting_sunday, 7)"))

    silver_coal_with_fx = raw_coal_api2.join(raw_fx_avg, on=['DeliveryStartDate'], how='left')
    silver_coal_with_fx = silver_coal_with_fx.withColumn('price_EUR', round(silver_coal_with_fx.price / silver_coal_with_fx.fx_rate_weekly_avg, 2))
    silver_coal_with_fx = silver_coal_with_fx.withColumnRenamed('price', 'price_USD')
    
    return silver_coal_with_fx.select(
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
        col("price_EUR"),
        col("price_USD"),
        col("fx_rate_weekly_avg"),
        col("Point_ID")
    )

# COMMAND ----------

@dlt.table(
    comment="Cleaned raw power data from france"
)
def silver_POWER_FRA():
    raw_power_fra =  dlt.read("raw_POWER_FRA")

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

@dlt.table(
    comment="Cleaned FX rates"
)
def silver_FX_EURUSD():
    raw_FX_EURUSD =  dlt.read("raw_FX_EURUSD")

    raw_FX_EURUSD = raw_FX_EURUSD.filter(F.col("FX_RATE") > 0)
    raw_FX_EURUSD = raw_FX_EURUSD.filter(F.col("DeliveryStartDate") <= F.col("DeliveryEndDate"))
    raw_FX_EURUSD = raw_FX_EURUSD.filter(F.col("DeliveryStartDate") == F.col("AdjustedDeliveryStartDate"))
    raw_FX_EURUSD = raw_FX_EURUSD.filter(F.col("DeliveryEndDate") == F.col("AdjustedDeliveryEndDate"))

    return (
        raw_FX_EURUSD.select(
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
             col("FX_Rate"),
             col("Point_ID")
            )
    )

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC
# MAGIC ### Gold layer:
# MAGIC
# MAGIC <img style="float: left; padding-left: 10px" src="https://github.com/genseral/axpo_dna_summit_2024/blob/main/figures/Full_DLT_gold.drawio.png?raw=true" width="600"/>
# MAGIC

# COMMAND ----------

@dlt.table(
    comment="Aggregated CO2 EUA"
)
def gold_CO2_EUA():
    return (
        dlt.read("silver_CO2_EUA")
            .select(
            '*'
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Aggregated COAL API2"
)
def gold_COAL_API2():
    return (
        dlt.read("silver_COAL_API2")
            .select(
            '*'
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Aggregated Power data from france"
)
def gold_POWER_FRA():
    return (
        dlt.read("silver_POWER_FRA")
            .select(
            '*'
            )
)

# COMMAND ----------

@dlt.table(
    comment="Aggregated FX data from EURUSD"
)
def gold_FX_EURUSD():
    return (
        dlt.read("silver_FX_EURUSD")
            .select(
            '*'
            )
)