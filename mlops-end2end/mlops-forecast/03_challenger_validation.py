# Databricks notebook source
# MAGIC %md
# MAGIC # Challenger model validation
# MAGIC
# MAGIC This notebook performs validation tasks on the candidate __Challenger__ model.
# MAGIC
# MAGIC It goes through a few steps to validate the model before labelling it (by setting its alias) to `Challenger`.
# MAGIC
# MAGIC When organizations first start to put MLOps processes in place, they should consider having a "human-in-the-loop" to perform visual analyses to validate models before promoting them. As they get more familiar with the process, they can consider automating the steps in a __Workflow__ . The benefits of automation is to ensure that these validation checks are systematically performed before new models are integrated into inference pipelines or deployed for realtime serving. Of course, organizations can opt to retain a "human-in-the-loop" in any part of the process and put in place the degree of automation that suits its business needs.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-4.png?raw=true" width="1200">
# MAGIC
# MAGIC *Note: in a typical mlops setup, this would run as part of an automated job to validate new model. For this simpel demo, we'll run it as an interactive notebook.*
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=3391241334329287&notebook=%2F01-mlops-quickstart%2F04_challenger_validation&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F04_challenger_validation&version=1">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `Current Cluster` from the dropdown menu ([open cluster configuration](https://adb-3391241334329287.7.azuredatabricks.net/#setting/clusters/1120-132929-qomz1zyx/configuration)). <br />

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## General Validation Checks
# MAGIC
# MAGIC <!--img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook-1.png" width=600 -->
# MAGIC
# MAGIC In the context of MLOps, there are more tests than simply how accurate a model will be.  To ensure the stability of our ML system and compliance with any regulatory requirements, we will subject each model added to the registry to a series of validation checks.  These include, but are not limited to:
# MAGIC <br>
# MAGIC * __Model documentation__
# MAGIC * __Inference on production data__
# MAGIC * __Champion-Challenger testing to ensure that business KPIs are acceptable__
# MAGIC
# MAGIC In this notebook we explore some approaches to performing these tests, and how we can add metadata to our models with tagging if they have passed a given test or not.
# MAGIC
# MAGIC This part is typically specific to your line of business and quality requirements.
# MAGIC
# MAGIC For each test, we'll add information using tags to know what has been validated in the model. We can also add Comments to a model if needed.

# COMMAND ----------

# MAGIC %pip install --quiet mlflow==2.14.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Model information
# MAGIC
# MAGIC We will fetch the model information for the __Challenger__ model from Unity Catalog.

# COMMAND ----------

# We are interested in validating the Challenger model
model_alias = "Challenger"
model_name = f"{catalog}.{db}.mlops_forecast"

client = MlflowClient()
model_details = client.get_model_version_by_alias(model_name, model_alias)
model_version = int(model_details.version)

print(f"Validating {model_alias} model for {model_name} on model version {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model checks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Description check
# MAGIC
# MAGIC Has the data scientist provided a description of the model being submitted?

# COMMAND ----------

# If there's no description or an insufficient number of charaters, tag accordingly
if not model_details.description:
  has_description = False
  print("Please add model description")
elif not len(model_details.description) > 20:
  has_description = False
  print("Please add detailed model description (40 char min).")
else:
  has_description = True

print(f'Model {model_name} version {model_details.version} has description: {has_description}')
client.set_model_version_tag(name=model_name, version=str(model_details.version), key="has_description", value=has_description)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model performance metric
# MAGIC
# MAGIC We want to validate the model performance metric. Typically, we want to compare this metric obtained for the Challenger model agaist that of the Champion model. Since we have yet to register a Champion model, we will only retrieve the metric for the Challenger model without doing a comparison.
# MAGIC
# MAGIC The registered model captures information about the MLflow experiment run, where the model metrics were logged during training. This gives you traceability from the deployed model back to the initial training runs.
# MAGIC
# MAGIC Here, we will use the validation set smape score for the out-of-sample test data that was set aside at training time.

# COMMAND ----------

model_run_id = model_details.run_id
smape_score = mlflow.get_run(model_run_id).data.metrics['val_smape']

try:
    #Compare the challenger f1 score to the existing champion if it exists
    champion_model = client.get_model_version_by_alias(model_name, "Champion")
    champion_smape = mlflow.get_run(champion_model.run_id).data.metrics['val_smape_score']
    print(f'Champion smape score: {champion_smape}. Challenger smape score: {smape_score}.')
    metric_smape_passed = smape_score >= champion_smape
except:
    print(f"No Champion found. Accept the model as it's the first one.")
    metric_smape_passed = True

print(f'Model {model_name} version {model_details.version} metric_smape_passed: {metric_smape_passed}')
# Tag that F1 metric check has passed
client.set_model_version_tag(name=model_name, version=model_details.version, key="metric_smape_passed", value=metric_smape_passed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promoting the Challenger to Champion
# MAGIC
# MAGIC When we are satisfied with the results of the __Challenger__ model, we can then promote it to Champion. This is done by setting its alias to `@Champion`. Inference pipelines that load the model using the `@Champion` alias will then be loading this new model. The alias on the older Champion model, if there is one, will be automatically unset. The model retains its `@Challenger` alias until a newer Challenger model is deployed with the alias to replace it.

# COMMAND ----------


print('register model as Champion!')
client.set_registered_model_alias(
  name=model_name,
  alias="Champion",
  version=model_version
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations, our model is now validated and promoted accordingly
# MAGIC
# MAGIC We now have the certainty that our model is ready to be used in inference pipelines and in realtime serving endpoints, as it matches our validation standards.
# MAGIC
# MAGIC
# MAGIC Next: Run batch inference from our newly promoted Champion model