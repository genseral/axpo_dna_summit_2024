# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Engineering
# MAGIC Our first step is to analyze the data and build the features we'll use to train our model. Let's see how this can be done.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-1.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=3391241334329287&notebook=%2F01-mlops-quickstart%2F01_feature_engineering&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F01_feature_engineering&version=1">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `Current Cluster` from the dropdown menu ([open cluster configuration](https://adb-3391241334329287.7.azuredatabricks.net/#setting/clusters/1120-132929-qomz1zyx/configuration)). <br />

# COMMAND ----------

# DBTITLE 1,Install latest feature engineering client for UC [for MLR < 13.2] and databricks python sdk
# MAGIC %pip install --quiet mlflow==2.14.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory Data Anaylsis
# MAGIC To get a feel of the data, what needs cleaning, pre-processing etc.
# MAGIC - **Use Databricks's native visualization tools**
# MAGIC   - After running a SQL query in a notebook cell, use the `+` tab to add charts to visualize the results.
# MAGIC - Bring your own visualization library of choice (i.e. seaborn, plotly)

# COMMAND ----------

# MAGIC %sql SELECT * FROM dbw_databricks_dna_hackathon_databricks_stream.02_data_product_trading.gold_power_fra

# COMMAND ----------

import matplotlib.pyplot as plt

powerDF = spark.read.table("dbw_databricks_dna_hackathon_databricks_stream.02_data_product_trading.gold_power_fra").toPandas()

# Convert DelieryStartDate to datetime
powerDF['DeliveryStartDate'] = pd.to_datetime(powerDF['DeliveryStartDate'])

# Plot the time series
plt.figure(figsize=(10, 6))
plt.plot(powerDF['DeliveryStartDate'], powerDF['clearing_price'])
plt.xlabel('Delivery Start Date')
plt.ylabel('Clearing Price')
plt.title('Time Series of Clearing Price')
plt.grid(True)
plt.show()

# COMMAND ----------

# DBTITLE 1,Read in Gold Delta table using Spark
# Read into Spark
powerDF = spark.read.table("dbw_databricks_dna_hackathon_databricks_stream.02_data_product_trading.gold_power_fra")
display(powerDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define data cleaning and featurization Logic
# MAGIC
# MAGIC We will define a function to clean the data and implement featurization logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Pandas On Spark API
# MAGIC
# MAGIC Because our Data Scientist team is familiar with Pandas, we'll use the [pandas on spark API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html) to scale `pandas` code. The Pandas instructions will be converted in the spark engine under the hood and distributed at scale.

# COMMAND ----------

# DBTITLE 1,Define featurization function
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import pyspark.pandas as ps

def clean_features(dataDF: DataFrame) -> DataFrame:
  """
  Simple cleaning function leveraging pandas API
  """

  # Convert to pandas on spark dataframe
  data_psdf = dataDF.pandas_api()

  # Drop all columns except DeliveryStartDate and clearing_price
  data_psdf = data_psdf[["DeliveryStartDate", "clearing_price"]]

  # Assuming powerDF is the pandas dataframe with DeliveryStartDate and clearing_price columns
  data_psdf['DeliveryStartDate'] = ps.to_datetime(data_psdf['DeliveryStartDate'])

  # Set DeliveryStartDate as the index
  data_psdf.set_index('DeliveryStartDate', inplace=True)

  # Resample to daily frequency and compute the mean of clearing_price
  daily_mean_df = data_psdf['clearing_price'].resample('D').mean().reset_index()

  # Rename columns
  daily_mean_df = daily_mean_df.rename(columns={"index": "ds"})
  daily_mean_df = daily_mean_df.rename(columns={"clearing_price": "y"})

  # Return the cleaned Spark dataframe
  return daily_mean_df.to_spark()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Compute features & write table with features and labels
# MAGIC
# MAGIC Once our features are ready, we'll save them along with the labels as a Delta Lake table. This can then be retrieved later for model training.
# MAGIC
# MAGIC In this Quickstart demo, we will look at how we train a model using this dataset saved as a Delta Lake table and capture the table-model lineage. Model lineage brings traceability and governance in our deployment, letting us know which model is dependent of which set of feature tables.

# COMMAND ----------

# DBTITLE 1,Compute Features
features = clean_features(powerDF)
display(features)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write table for training
# MAGIC
# MAGIC Write the data that has the prepared features as a Delta Table. We will later use this table to train the model to forecast.

# COMMAND ----------

# Write table for training
(
  features.write.mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("mlops_forecast_training")
)

# Add comment to the table
spark.sql(f"""COMMENT ON TABLE {catalog}.{db}.mlops_forecast_training IS \'The features in this table are derived from the dbw_databricks_dna_hackathon_databricks_stream.02_data_product_trading.gold_power_fra table in the lakehouse. We created features to train a forecasting model by aggregating the hourly data on the daily resolution.'""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! The feature is now ready to be used for training.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating forecasting model creation using Databricks AutoML
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC
# MAGIC Databricks simplify model creation and MLOps. However, bootstraping new ML projects can still be long and inefficient.
# MAGIC
# MAGIC Instead of creating the same boilerplate for each new project, Databricks AutoML can automatically generate state of the art models for classifications, regression, and forecast.
# MAGIC
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC
# MAGIC ### Using Databricks AutoML with our forecast dataset
# MAGIC
# MAGIC AutoML is available in the "Machine Learning" space. All we have to do is start a new AutoML experiment and select the table we just created (`{catalog}.{schema}.mlops_forecast_training`).
# MAGIC
# MAGIC Our target variable is the `y` column.
# MAGIC
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/en/machine-learning/automl/train-ml-model-automl-api.html)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using AutoML with labelled feature tables
# MAGIC
# MAGIC [AutoML](https://docs.databricks.com/en/machine-learning/automl/how-automl-works.html) works on an input table with prepared features and the corresponding labels. For this quicktstart demo, this is what we will be doing. We run AutoML on the table `{catalog}.{schema}.mlops_forecast_training` and capture the table lineage at training time.

# COMMAND ----------

display(features)

# COMMAND ----------

# DBTITLE 1,Run 'baseline' autoML experiment in the back-ground
from databricks import automl
from datetime import datetime

xp_path = f"/Shared/dbdemos/experiments/forecast/{dbName}"
xp_name = f"automl_forecast_{dbName}_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"

automl_run = automl.forecast(
    experiment_name = xp_name,
    experiment_dir = xp_path,
    dataset = features,
    target_col = "y",
    time_col="ds",
    horizon=7, 
    frequency="D",  
    primary_metric="smape",
    timeout_minutes=5,
)
# Make sure all users can access dbdemos shared experiment
DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")