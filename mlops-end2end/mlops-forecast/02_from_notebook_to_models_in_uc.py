# Databricks notebook source
# MAGIC %md
# MAGIC # Managing the model lifecycle in Unity Catalog
# MAGIC
# MAGIC One of the primary challenges among data scientists and ML engineers is the absence of a central repository for models, their versions, and the means to manage them throughout their lifecycle.
# MAGIC
# MAGIC [Models in Unity Catalog](https://docs.databricks.com/en/mlflow/models-in-uc.html) addresses this challenge and enables members of the data team to:
# MAGIC <br>
# MAGIC * **Discover** registered models, current aliases in model development, experiment runs, and associated code with a registered model
# MAGIC * **Promote** models to different phases of their lifecycle with the use of model aliases
# MAGIC * **Tag** models to capture metadata specific to your MLOps process
# MAGIC * **Deploy** different versions of a registered model, offering MLOps engineers ability to deploy and conduct testing of different model versions
# MAGIC * **Test** models in an automated fashion
# MAGIC * **Document** models throughout their lifecycle
# MAGIC * **Secure** access and permission for model registrations, execution or modifications
# MAGIC
# MAGIC We will look at how we test and promote a new __Challenger__ model as a candidate to replace an existing __Champion__ model.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-3.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=3391241334329287&notebook=%2F01-mlops-quickstart%2F03_from_notebook_to_models_in_uc&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F03_from_notebook_to_models_in_uc&version=1">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `Current Cluster` from the dropdown menu ([open cluster configuration](https://adb-3391241334329287.7.azuredatabricks.net/#setting/clusters/1120-132929-qomz1zyx/configuration)). <br />

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use Models in Unity Catalog
# MAGIC Typically, data scientists who use MLflow will conduct many experiments, each with a number of runs that track and log metrics and parameters. During the course of this development cycle, they will select the best run within an experiment and register its model to Unity Catalog.  Think of this as **committing** the model to the Unity Catalog, much as you would commit code to a version control system.
# MAGIC
# MAGIC Unity Catalog proposes free-text model alias i.e. `Baseline`, `Challenger`, `Champion` along with tagging.
# MAGIC
# MAGIC Users with appropriate permissions can create models, modify aliases and tags, use models etc.

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install --quiet mlflow==2.14.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Programmatically find best run and push model to the Unity Catalog for validation
# MAGIC
# MAGIC We have completed the training runs to find a candidate __Challenger__ model. We'll programatically select the best model from our last ML experiment and register it to Unity Catalog. We can easily do that using MLFlow `search_runs` API:

# COMMAND ----------

import mlflow

experiment_name = f"automl_forecast_{dbName}"
model_name = f"{catalog}.{db}.mlops_forecast"
print(f"Finding best run from {experiment_name}_* and pushing new model version to {model_name}")

xp_path = f"/Shared/dbdemos/experiments/forecast/{dbName}"
experiment_id = mlflow.search_experiments(filter_string=f"name LIKE '{xp_path}%'", order_by=["last_update_time DESC"])[0].experiment_id
print(experiment_id)

# COMMAND ----------

# Let's get our best ml run
best_model = mlflow.search_runs(
  experiment_ids=experiment_id,
  order_by=["metrics.val_smape ASC"],
  max_results=1,
  filter_string="status = 'FINISHED'",
)
# Optional: Load MLflow Experiment as a spark df and see all runs
# df = spark.read.format("mlflow-experiment").load(experiment_id)
best_model

# COMMAND ----------

# MAGIC %md Once we have our best model, we can now register it to the Unity Catalog Model Registry using it's run ID

# COMMAND ----------

print(f"Registering model to {model_name}")  # {model_name} is defined in the setup script

# Get the run id from the best model
run_id = best_model.iloc[0]['run_id']

# Register best model from experiments run to MLflow model registry
model_details = mlflow.register_model(f"runs:/{run_id}/model", model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC At this point the model does not yet have any aliases or description that indicates its lifecycle and meta-data/info.  Let's update this information.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Give the registered model a description
# MAGIC
# MAGIC We'll do this for the registered model overall.

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# The main model description, typically done once.
client.update_registered_model(
  name=model_details.name,
  description="This model forecasts the clearing price of power of the next 7 days.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC And add some more details on the new version we just registered

# COMMAND ----------

# Provide more details on this specific model version
best_score = best_model['metrics.val_smape'].values[0]
run_name = best_model['tags.mlflow.runName'].values[0]
version_desc = f"This model version has smape of {round(best_score,4)*100}%. Follow the link to its training run for more details."

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description=version_desc
)

# We can also tag the model version with the F1 score for visibility
client.set_model_version_tag(
  name=model_details.name,
  version=model_details.version,
  key="smape",
  value=f"{round(best_score,4)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set the latest model version as the Challenger model
# MAGIC
# MAGIC We will set this newly registered model version as the __Challenger__ model. Challenger models are candidate models to replace the Champion model, which is the model currently in use.
# MAGIC
# MAGIC We will use the model's alias to indicate the stage it is at in its lifecycle.

# COMMAND ----------

# Set this version as the Challenger model, using its model alias
client.set_registered_model_alias(
  name=model_name,
  alias="Challenger",
  version=model_details.version
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now, visually inspect the model verions in Unity Catalog Explorer. You should see the version description and `Challenger` alias applied to the version.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Validation of the Challenger model
# MAGIC
# MAGIC At this point, with the __Challenger__ model registered, we would like to validate the model. The validation steps are implemented in a notebook, so that the validation process can be automated as part of a Databricks Workflow job.
# MAGIC
# MAGIC If the model passes all the tests, it'll be promoted to `Champion`.
# MAGIC
# MAGIC Next: Find out how the model is being tested befored being promoted as `Champion`.