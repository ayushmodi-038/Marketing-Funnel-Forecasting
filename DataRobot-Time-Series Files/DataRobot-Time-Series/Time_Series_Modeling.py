#!/usr/bin/env python
# coding: utf-8

# # Time Series Modeling
# 
# ## Overview
# This example provides an introduction to a few of DataRobot's time series modeling capabilities with a sales dataset.
# Here is a list of things we will touch on during this notebook:
# 
# - Installing the `datarobot` package
# - Configuring the client
# - Creating a project
# - Denoting known-in-advance features
# - Specifying a partitioning scheme
# - Running the automated modeling process
# - Generating predictions
# 
# 
# ## Prerequisites
# In order to run this notebook yourself, you will need the following:
# 
# - This notebook. If you are viewing this in the HTML documentation bundle, you can download all of the example notebooks and supporting materials from [Downloads](../index.rst).
# - The required datasets, which is included in the same directory as this notebook.
# - A DataRobot API token. You can find your API token by logging into the DataRobot Web User Interface and looking in your `Profile`.
# - The `xlrd` Python package is needed for the pandas `read_excel` function. You can install this with `pip install xlrd`.
# 
# 
# ### Installing the `datarobot` package
# The `datarobot` package is hosted on PyPI. You can install it via:
# ```
# pip install datarobot
# ```
# from the command line. Its main dependencies are `numpy` and `pandas`, which could take some time to install on a new system. We highly recommend use of virtualenvs to avoid conflicts with other dependencies in your system-wide python installation.

# ## Getting Started
# This line imports the `datarobot` package. By convention, we always import it with the alias `dr`.

# In[1]:


import datarobot as dr


# ### Other Important Imports
# We'll use these in this notebook as well. If the previous cell and the following
# cell both run without issue, you're in good shape.

# In[2]:


import datetime
import pandas as pd


# ### Configure the Python Client
# Configuring the client requires the following two things:
# 
# - A DataRobot endpoint - where the API server can be found
# - A DataRobot API token - a token the server uses to identify and validate the user making API requests
# 
# The endpoint is usually the URL you would use to log into the DataRobot Web User Interface (e.g., https://app.datarobot.com) with "/api/v2/" appended, e.g., (https://app.datarobot.com/api/v2/).
# 
# You can find your API token by logging into the DataRobot Web User Interface and looking in your `Profile.`
# 
# The Python client can be configured in several ways. The example we'll use in this notebook is to point to a `yaml` file that has the information. This is a text file containing two lines like this:
# ```yaml
# endpoint: https://app.datarobot.com/api/v2/
# token: not-my-real-token
# ```
# 
# If you want to run this notebook without changes, please save your configuration in a file located under your home directory called `~/.config/datarobot/drconfig.yaml`.

# In[3]:


# Initialization with arguments
dr.Client(token='NWYwZTE3MzNjZjZlZTUxYjE3ZGUzY2U1OjdjVU9Tdis3QWxmdm9XMUg1Vnd4dTd5ZHF0OHo2cTJqWkJPTENnRXRscmM9', endpoint='https://app.datarobot.com/api/v2/')

# Initialization with a config file in the same directory as this notebook
# dr.Client(config_path='drconfig.yaml')


# ## Create the Project
# Here, we use the `datarobot` package to upload a new file and create a project. The name of the project is optional, but can be helpful when trying to sort among many projects on DataRobot.

# In[4]:


filename = 'data/DR_Demo_Sales_Multiseries_training.xlsx'
now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M')
project_name = 'DR_Demo_Sales_Multiseries_{}'.format(now)
proj = dr.Project.create(sourcedata=filename,
                         project_name=project_name,
                         max_wait=3600)
print('Project ID: {}'.format(proj.id))


# ## Identify Known-In-Advance Features
# This dataset has five columns that will always be known-in-advance and available for prediction.

# In[5]:


known_in_advance = ['Marketing', 'Near_Xmas', 'Near_BlackFriday',
                    'Holiday', 'DestinationEvent']

feature_settings = [dr.FeatureSettings(feat_name,
                                       known_in_advance=True)
                    for feat_name in known_in_advance]


# ## Create a Partition Specification
# This problem has a time component to it, and it would be bad practice to train on data from the present and predict on the past. We could manually add a column to the dataset to indicate which rows should be used for training, test, and validation, but it is straightforward to allow DataRobot to do it automatically. This dataset contains sales data from multiple individual stores so we use `multiseries_id_columns` to tell DataRobot there are actually multiple time series in this file and to indicate the column that identifies the series each row belongs to.

# In[6]:


time_partition = dr.DatetimePartitioningSpecification(
    datetime_partition_column='Date',
    multiseries_id_columns=['Store'],
    use_time_series=True,
    feature_settings=feature_settings,
)


# ## Run the Automated Modeling Process
# Now we can start the modeling process. The target for this problem is called `Sales` and we let DataRobot automatically select the metric for scoring and comparing models.
# 
# The `partitioning_method` is used to specify that we would like DataRobot to use the partitioning schema we specified previously
# 
# Finally, the `worker_count` parameter specifies how many workers should be used for this project. Passing a value of `-1` tells DataRobot to set the worker count to the maximum available to you. You can also specify the exact number of workers to use, but this command will fail if you request more workers than your account allows. If you need more resources than what has been allocated to you, you should think about upgrading your license.
# 
# The second command provides a URL that can be used to see the project execute on the DataRobot UI.
# 
# The last command in this cell is just a blocking loop that periodically checks on the project to see if it is done, printing out the number of jobs in progress and in the queue along the way so you can see progress. The automated model exploration process will occasionally add more jobs to the queue, so don't be alarmed if the number of jobs does not strictly decrease over time.

# In[7]:


proj.set_target(
    target='Sales',
    partitioning_method=time_partition,
    max_wait=3600,
    worker_count=-1
)

print(proj.get_leaderboard_ui_permalink())

proj.wait_for_autopilot()


# ## Choose the Best Model
# First, we take a look at the top of the leaderboard. In this example, we choose the model that has the lowest backtesting error.

# In[7]:


proj.get_datetime_models()[:10]


# In[16]:


lb = proj.get_datetime_models()
valid_models = [m for m in lb if
                m.metrics[proj.metric]['backtesting']]
best_model = min(valid_models,
                 key=lambda m: m.metrics[proj.metric]['backtesting'])

print(best_model.model_type)
print(best_model.get_leaderboard_ui_permalink())


# ## Generate Predictions
# This example notebook uses the modeling API to make predictions, which uses modeling servers to score the predictions. If you have dedicated prediction servers, you should use that API for faster performance.
# 
# ### Finish training
# First, we unlock the holdout data to fully train the best model. The last command in the next cell prints the URL to examine the fully-trained model in the DataRobot UI.

# In[10]:


proj.unlock_holdout()
job = best_model.request_frozen_datetime_model()
retrained_model = job.get_result_when_complete()

print(retrained_model.get_leaderboard_ui_permalink())


# ### Execute a prediction job
# First, we find the latest date in the training data. Then, we upload a dataset to predict from, setting the starting `forecast_point` to be the end of the training data. Finally, we execute the prediction request.

# In[11]:


d = pd.read_excel('DR_Demo_Sales_Multiseries_training.xlsx')
last_train_date = pd.to_datetime(d['Date']).max()

dataset = proj.upload_dataset(
    'DR_Demo_Sales_Multiseries_prediction.xlsx',
    forecast_point=last_train_date
)

pred_job = retrained_model.request_predictions(dataset_id=dataset.id)
preds = pred_job.get_result_when_complete()


# Each row of the resulting predictions has a `prediction` of sales at a `timestamp` for a particular `series_id` and can be matched to the the uploaded prediction data set through the `row_id` field. The `forecast_distance` is the number of time units after the forecast point for a given row.

# In[12]:


preds.head()

# we could also write predictions out to a file for subsequent analysis
# preds.to_csv('DR_Demo_Sales_Multiseries_prediction_output.csv', index=False)


# In[ ]:




