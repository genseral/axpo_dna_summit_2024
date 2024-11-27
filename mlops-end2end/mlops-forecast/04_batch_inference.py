# Databricks notebook source
# MAGIC %md
# MAGIC # Forecasting Model Inference
# MAGIC
# MAGIC ## Inference with the Champion model
# MAGIC
# MAGIC With Models in Unity Catalog, they can be loaded for use in batch inference pipelines. The generated predictions can used to devise customer retention strategies, or be used for analytics. The model in use is the __Champion__ model, and we will load this for use in our pipeline.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-5.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=3391241334329287&notebook=%2F01-mlops-quickstart%2F05_batch_inference&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F05_batch_inference&version=1">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `Current Cluster` from the dropdown menu ([open cluster configuration](https://adb-3391241334329287.7.azuredatabricks.net/#setting/clusters/1120-132929-qomz1zyx/configuration)). <br />

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install --quiet mlflow==2.14.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $setup_inference_data=true

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <!--img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" /-->
# MAGIC
# MAGIC Now that our model is available in the Unity Catalog Model Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, you can get sample code from the __"Artifacts"__ page of the model's experiment run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run inferences

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository

requirements_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.mlops_forecast@Champion").download_artifacts(artifact_path="requirements.txt") # download model from remote registry

# COMMAND ----------

# MAGIC %pip install --quiet -r $requirements_path
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch inference on the Champion model
# MAGIC
# MAGIC We are ready to run inference on the Champion model. We will load the model as a python model and generate predictions.

# COMMAND ----------

# DBTITLE 1,In a python notebook
import mlflow
# Load customer features to be scored
inference_df = spark.read.table(f"mlops_forecast_training").orderBy("ds").toPandas()

# Load champion model as a Spark UDF
champion_model = mlflow.pyfunc.load_model(model_uri=f"models:/{catalog}.{db}.mlops_forecast@Champion")

# COMMAND ----------

# Predict future with the default horizon
forecast_pd = champion_model._model_impl.python_model.predict_timeseries()
forecast_pd.rename(columns={'y': 'yhat'}, inplace=True)

# COMMAND ----------

from databricks.automl_runtime.forecast.pmdarima.utils import plot
earliest_ds = forecast_pd["ds"].min()
filtered_inference_df = inference_df[inference_df["ds"] >= earliest_ds]
fig = plot(filtered_inference_df, forecast_pd)

# COMMAND ----------

predict_cols = ["ds", "yhat"]
forecast_pd = forecast_pd.reset_index()
display(forecast_pd[predict_cols].tail(7))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! Our data can now be saved as a table and re-used by the teams to take special action based on the forecast!
# MAGIC
# MAGIC Your data will also be available within Genie to answer any forecast-related question using plain text english!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC
# MAGIC This is all for the quickstart demo! We have looked at basic concepts of MLOps and how Databricks helps you achieve them. They include:
# MAGIC
# MAGIC - Feature engineering and storing features in Databricks
# MAGIC - AutoML, model training and experiment tracking in MLflow
# MAGIC - Registering models as Models in Unity Catalog for governed usage
# MAGIC - Model validation, Champion-Challenger testing, and model promotion
# MAGIC - Batch inference by loading the model as a pyfunc
