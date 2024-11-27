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
# MAGIC - dataset_1.csv -> CO2_EUA id  88021503: Daily EoD EUA monthly settlement; OIL_G id  88076251: Low Sulphur Gasoil
# MAGIC - dataset_2.csv -> GAS_THE id 320082501: Gas THE (Germany) daily Settlement; COAL_API2 id 320019501: Coal API2 USD Weekly Argus settlement in  Amsterdam, Rotterdam region in the Netherlands, and Antwerp region in Belgium.
# MAGIC - dataset_3.csv -> POWER_DEU id 112069251: Germany Power Daily settlement; POWER_FRA id  112047001: France Power Daily Settlement
# MAGIC - dataset_4.csv -> -	Foreign exchange (FX) rate information EUR-USD
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
# MAGIC
# MAGIC

# COMMAND ----------

@dlt.table(
    name = "bronze_CO2_EUA_OIL_G",
    comment = "Raw data from CO2_EUA and OIL_G",
)
@dlt.expect_or_fail("positive_settlement_price", F.col("settlement_price") > 0)
def bronze_CO2_EUA_OIL_G():
    """
    This function creates a Delta Live Table (DLT) for raw data from CO2_EUA and OIL_G.
    It reads streaming data from the specified source path, infers column types, and selects all columns.
    The function also includes an expectation that the settlement price must be positive.
    """
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
    name = "bronze_GAS_THE_COAL_API2",
    comment = "Raw data from GAS THE and Coal API2",
)
def bronze_GAS_THE_COAL_API2():
    """
    This function creates a Delta Live Table (DLT) for raw data from GAS THE and Coal API2.
    It reads streaming data from the specified source path, infers column types, and selects all columns.
    """
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
    name = "bronze_POWER_FRA_GER",
    comment = "Raw data power data from france and germany",
)
#@dlt.expect_or_drop("vaild_volume", F.col("clearing_volume") != 'NULL')
def bronze_POWER_FRA_GER():
    """
    This function creates a Delta Live Table (DLT) for raw power data from France and Germany.
    It reads streaming data from the specified source path, infers column types, and selects all columns.
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

@dlt.table(
    name = "bronze_FX_EURUSD",
    comment = "Raw data for FX from euro to usd",
)
def bronze_FX_EURUSD():
    """
    This function creates a Delta Live Table (DLT) for raw FX data from EUR to USD.
    It reads streaming data from the specified source path, infers column types, and selects all columns.
    """
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
    name = "silver_CO2_EUA",
    comment="Cleaned raw data for CO2 emissions from eua"
)
def silver_CO2_EUA():
    """
    This function creates a Delta Live Table (DLT) for cleaned CO2 emissions data from EUA.
    It reads data from the bronze_CO2_EUA_OIL_G table, filters it based on specific conditions,
    and selects relevant columns for the final table.
    """
    raw_co2_eua = dlt.read("bronze_CO2_EUA_OIL_G")
    
    raw_co2_eua = raw_co2_eua.filter(F.col("TimeSeries_FID") == 88021503)
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
    name = "silver_OIL_G",
    comment="Cleaned raw data for OIL_G - Low Sulphur Gasoil"
)
def silver_OIL_G():
    """
    This function creates a Delta Live Table (DLT) for cleaned OIL_G data.
    It reads data from the bronze_CO2_EUA_OIL_G table, filters it based on specific conditions,
    and selects relevant columns for the final table.
    """
    raw_OIL_G = dlt.read("bronze_CO2_EUA_OIL_G")
    
    raw_OIL_G = raw_OIL_G.filter(F.col("TimeSeries_FID") == 88076251)
    raw_OIL_G = raw_OIL_G.filter(F.col("DeliveryStartDate") <= F.col("DeliveryEndDate"))
    raw_OIL_G = raw_OIL_G.filter(F.col("DeliveryStartDate") == F.col("AdjustedDeliveryStartDate"))
    raw_OIL_G = raw_OIL_G.filter(F.col("DeliveryEndDate") == F.col("AdjustedDeliveryEndDate"))
    return (
        raw_OIL_G.select(
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
    """
    This function creates a Delta Live Table (DLT) for cleaned COAL_API2 data.
    It reads data from the bronze_GAS_THE_COAL_API2 table, filters it based on specific conditions,
    and selects relevant columns for the final table. Additionally, it joins with FX data to 
    calculate the price in EUR.
    """
    raw_fx = dlt.read("bronze_FX_EURUSD")
    raw_coal_api2 = dlt.read("bronze_GAS_THE_COAL_API2")
    
    raw_coal_api2 = raw_coal_api2.filter(F.col("TimeSeries_FID") == 320019501)
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
    comment="Cleaned raw data for GAS_THE"
)
def silver_GAS_THE():
    """
    This function creates a Delta Live Table (DLT) for cleaned GAS_THE data.
    It reads data from the bronze_GAS_THE_COAL_API2 table, filters it based on specific conditions,
    and selects relevant columns for the final table. Additionally, it joins with FX data to 
    calculate the price in EUR.
    """
    raw_fx = dlt.read("bronze_FX_EURUSD")
    raw_GAS_THE = dlt.read("bronze_GAS_THE_COAL_API2")
    
    raw_GAS_THE = raw_GAS_THE.filter(F.col("TimeSeries_FID") == 320019501)
    raw_GAS_THE = raw_GAS_THE.filter(F.col("DeliveryStartDate") <= F.col("DeliveryEndDate"))
    raw_GAS_THE = raw_GAS_THE.filter(F.col("DeliveryStartDate") == F.col("AdjustedDeliveryStartDate"))
    raw_GAS_THE = raw_GAS_THE.filter(F.col("DeliveryEndDate") == F.col("AdjustedDeliveryEndDate"))

    # First, fix the computation of the 'date' column to correctly use 'next_day'
    raw_fx_avg = raw_fx.withColumn("week", weekofyear("DeliveryStartDate")) \
                    .withColumn("year", year("DeliveryStartDate")) \
                    .groupBy("week", "year") \
                    .agg(round(avg("FX_Rate"), 2).alias("fx_rate_weekly_avg")) \
                    .withColumn("week_starting_sunday", expr("date_add(next_day(to_date(concat(year, '-01-01')), 'Sunday'), (week - 1) * 7)")) \
                    .withColumn("DeliveryStartDate", expr("date_sub(week_starting_sunday, 7)"))

    silver_coal_with_fx = raw_GAS_THE.join(raw_fx_avg, on=['DeliveryStartDate'], how='left')
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
    raw_power_fra =  dlt.read("bronze_POWER_FRA_GER")

    raw_power_fra = raw_power_fra.filter(F.col("TimeSeries_FID") == 112047001)
    raw_power_fra = raw_power_fra.filter(F.col("DeliveryStartDate") <= F.col("DeliveryEndDate"))
    raw_power_fra = raw_power_fra.filter(F.col("DeliveryStartDate") == F.col("AdjustedDeliveryStartDate"))
    raw_power_fra = raw_power_fra.filter(F.col("DeliveryEndDate") == F.col("AdjustedDeliveryEndDate"))
    
    raw_power_fra = raw_power_fra.withColumn("clearing_price", col("clearing_price").cast("double"))
    raw_power_fra = raw_power_fra.withColumn("clearing_volume", col("clearing_volume").cast("double"))
    
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
    comment="Cleaned raw power data from germany"
)
def silver_POWER_GER():
    """
    This function creates a Delta Live Table (DLT) for cleaned POWER_GER data.
    It reads data from the bronze_POWER_FRA_GER table, filters it based on specific conditions,
    and selects relevant columns for the final table. Additionally, it casts the clearing_price 
    and clearing_volume columns to double type.
    """
    raw_power_ger =  dlt.read("bronze_POWER_FRA_GER")

    raw_power_ger = raw_power_ger.filter(F.col("TimeSeries_FID") == 112069251)
    raw_power_ger = raw_power_ger.filter(F.col("DeliveryStartDate") <= F.col("DeliveryEndDate"))
    raw_power_ger = raw_power_ger.filter(F.col("DeliveryStartDate") == F.col("AdjustedDeliveryStartDate"))
    raw_power_ger = raw_power_ger.filter(F.col("DeliveryEndDate") == F.col("AdjustedDeliveryEndDate"))
    
    raw_power_ger = raw_power_ger.withColumn("clearing_price", col("clearing_price").cast("double"))
    raw_power_ger = raw_power_ger.withColumn("clearing_volume", col("clearing_volume").cast("double"))
    return (
        raw_power_ger.select(
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
    """
    This function creates a Delta Live Table (DLT) for cleaned FX_EURUSD data.
    It reads data from the bronze_FX_EURUSD table, filters it based on specific conditions,
    and selects relevant columns for the final table.
    """
    raw_FX_EURUSD =  dlt.read("bronze_FX_EURUSD")

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

# COMMAND ----------

@dlt.table(
    comment="Aggregated CO2 EUA"
)
def gold_CO2_EUA():
    """
    This function creates a Delta Live Table (DLT) for aggregated CO2 EUA data.
    It reads data from the silver_CO2_EUA table and selects all columns for the final table.
    """
    return (
        dlt.read("silver_CO2_EUA")
            .select(
            '*'
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Aggregated OIL G"
)
def gold_OIL_G():
    """
    This function creates a Delta Live Table (DLT) for aggregated OIL G data.
    It reads data from the silver_OIL_G table and selects all columns for the final table.
    """
    return (
        dlt.read("silver_OIL_G")
            .select(
            '*'
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Aggregated COAL API2"
)
def gold_COAL_API2():
    """
    This function creates a Delta Live Table (DLT) for aggregated COAL API2 data.
    It reads data from the silver_COAL_API2 table and selects all columns for the final table.
    """
    return (
        dlt.read("silver_COAL_API2")
            .select(
            '*'
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Aggregated GAS THE"
)
def gold_GAS_THE():
    """
    This function creates a Delta Live Table (DLT) for aggregated GAS THE data.
    It reads data from the silver_GAS_THE table and selects all columns for the final table.
    """
    return (
        dlt.read("silver_GAS_THE")
            .select(
            '*'
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Aggregated Power data from france"
)
def gold_POWER_FRA():
    """
    This function creates a Delta Live Table (DLT) for aggregated Power data from France.
    It reads data from the silver_POWER_FRA table and selects all columns for the final table.
    """
    return (
        dlt.read("silver_POWER_FRA")
            .select(
            '*'
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Aggregated Power data from germany"
)
def gold_POWER_GER():
    """
    This function creates a Delta Live Table (DLT) for aggregated Power data from Germany.
    It reads data from the silver_POWER_GER table and selects all columns for the final table.
    """
    return (
        dlt.read("silver_POWER_GER")
            .select(
            '*'
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Aggregated FX data from EURUSD"
)
def gold_FX_EURUSD():
    """
    This function creates a Delta Live Table (DLT) for aggregated FX data from EURUSD.
    It reads data from the silver_FX_EURUSD table and selects all columns for the final table.
    """
    return (
        dlt.read("silver_FX_EURUSD")
            .select(
            '*'
            )
    )