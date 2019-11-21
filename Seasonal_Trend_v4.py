# Databricks notebook source
# DBTITLE 1,Import Data from MYSQL
# Utility and Spark
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *

# Broken as of 11/14/2019 on this cluster until MJ and Matt fix it
#from ds_utils.ds_utils import connect_to_dslab
# Use Apollo SAPT ETL library instead
#import Apollo_nonSAP_ETL_HF.connect as connector
# Tip from Will use pyodbc

# Must specify specific version of scipy
import pkg_resources
# Modified to use specific scipy
#pkg_resources.require("scipy==1.2.1")
import scipy

# Package for ACF
from statsmodels.tsa.stattools import acf, pacf
import pandas as pd
import numpy as np

# COMMAND ----------

# DBTITLE 1,Data Access
# Use this for the ANC Seasonal Plots
import pyodbc

server = "datasciencelab1sqlsrv.database.windows.net"
database = "datasciencelab1-sqldb"
username = dbutils.secrets.get(scope='DSLabCreds',key='DSLabCred_UserName_ReadWrite')
password = dbutils.secrets.get(scope='DSLabCreds',key='DSLabCred_Password_ReadWrite')
port = "1433"
driver = "ODBC Driver 17 for SQL Server"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(server, port, database)

connectionProperties = pyodbc.connect( 'DRIVER={'+driver+'};'
                             'SERVER='+server+';'
                             'DATABASE='+database+';UID='+username+';'
                             'PWD='+password)

connectionProperties.autocommit = True

connection = {
  "user" : username,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# DBTITLE 1,Table Names
# table names
ancMatLoc = "fcst.ancCleanRawMATLOC"
ancMatLocCust = "fcst.ancCleanRawMATLOCCUST"
product = "mstr.HT_MATERIAL_PLANT_MASTER"

# COMMAND ----------

# DBTITLE 1,SQL Queries
# base table at the dist, mat, loc, sales org level for anc
baseTable = """
SELECT 
      DISTRIBUTION_CHANNEL, 
      MATERIAL_CODE, 
      PLANT_CODE, 
      SALES_ORG, 
      FORECAST_DATE, format(FORECAST_DATE, 'MMMM') as MONTH, 
      YEAR(FORECAST_DATE) as YEAR, 
      DATEADD(mm, DATEDIFF(mm, 0, FORECAST_DATE) - 1, 0) as MONTH_DATE, 
      UNCLEAN_SOH, 
      CLEAN_SOH
      FROM 
""" + ancMatLoc

# sql anc matloc query
query_base_group = """
SELECT 
      DISTRIBUTION_CHANNEL, 
      MATERIAL_CODE, 
      PLANT_CODE, 
      SALES_ORG, 
      YEAR, 
      MONTH, 
      MONTH_DATE, 
      FORECAST_DATE,
      SUM(UNCLEAN_SOH) as SUM_UNCLEAN_SOH, 
      SUM(CLEAN_SOH) as SUM_CLEAN_SOH
      FROM (
          SELECT 
                DISTRIBUTION_CHANNEL, 
                MATERIAL_CODE, 
                PLANT_CODE, 
                SALES_ORG, 
                FORECAST_DATE, format(FORECAST_DATE, 'MMMM') as MONTH, 
                YEAR(FORECAST_DATE) as YEAR, 
                DATEADD(mm, DATEDIFF(mm, 0, FORECAST_DATE) - 1, 0) as MONTH_DATE, 
                UNCLEAN_SOH, 
                CLEAN_SOH
                FROM """ + ancMatLoc + """
      ) MATLOC
      GROUP BY
      DISTRIBUTION_CHANNEL, MATERIAL_CODE, PLANT_CODE, SALES_ORG, YEAR, MONTH, MONTH_DATE, FORECAST_DATE
"""

# sql anc matloccust query

query_stratcust_group = """
SELECT 
      DISTRIBUTION_CHANNEL,
      StratCust,
      MATERIAL_CODE, 
      PLANT_CODE, 
      SALES_ORG, 
      YEAR, 
      MONTH, 
      MONTH_DATE, 
      FORECAST_DATE,
      SUM(UNCLEAN_SOH) AS SUM_UNCLEAN_SOH, 
      SUM(CLEAN_SOH) AS SUM_CLEAN_SOH
      FROM (
          SELECT 
                DISTRIBUTION_CHANNEL, 
                StratCust, 
                MATERIAL_CODE, 
                PLANT_CODE, 
                SALES_ORG,
                FORECAST_DATE, format(FORECAST_DATE, 'MMMM') as MONTH, 
                YEAR(FORECAST_DATE) as YEAR, 
                DATEADD(mm, DATEDIFF(mm, 0, FORECAST_DATE) - 1, 0) as MONTH_DATE, 
                UNCLEAN_SOH, 
                CLEAN_SOH
                FROM """ + ancMatLocCust + """
      ) MATLOCCUST
      GROUP BY
      DISTRIBUTION_CHANNEL, StratCust, MATERIAL_CODE, PLANT_CODE, SALES_ORG, YEAR, MONTH, MONTH_DATE, FORECAST_DATE
"""   

# sql product query used to get the product categories
productTable = """
SELECT
      PLANT as PLANT_CODE, 
      MATL as MATERIAL_CODE, 
      PRODUCT_CATEGORY, 
      DIVISION 
      FROM 
""" + product

# COMMAND ----------

# DBTITLE 1,Retrieve Data
# base table
base = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", baseTable).load()
# table aggregations to run acf at the group level
base_group = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query_base_group).load()
customer_group = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query_stratcust_group).load()
# product table to join after acf and aggregation calculations
product_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", productTable).load()

# COMMAND ----------

# DBTITLE 1,Create Product Category Table
# Join product table to the anc base table
category_table = base.join(product_table, on = ['PLANT_CODE', 'MATERIAL_CODE'], how = 'left')

# COMMAND ----------

# DBTITLE 1,Create Auto-correlation columns and append based on group
# iterating through groups

# Group parameters
group_agg_base = ['DISTRIBUTION_CHANNEL', 'MATERIAL_CODE', 'PLANT_CODE', 'SALES_ORG']
group_agg_cust = ['DISTRIBUTION_CHANNEL', 'StratCust', 'MATERIAL_CODE', 'PLANT_CODE', 'SALES_ORG']
group_agg_category = ['DISTRIBUTION_CHANNEL', 'SALES_ORG', 'PRODUCT_CATEGORY'] # new enhancement for Pablo

# ACF features
acf_var = ['ACF_UNCLEAN_SOH', 'ACF_CLEAN_SOH', 'unclean_ci_lower', 'unclean_ci_upper', 'unclean_lj_lower', 'unclean_lj_upper', 'clean_ci_lower', 'clean_ci_upper', 'clean_lj_lower', 'clean_lj_upper']

def autocorrelation_agg(pdf, group_agg):
  # ignore warnings about NA or 0 division
  np.seterr(divide = 'ignore', invalid = 'ignore')
  appended_data = []
  for name, group in pdf.groupby(group_agg):
    try:
      # sort by month
      group = group.sort_values('MONTH_DATE')
      length = len(group) - 1
      # autocorrelation unclean soh
      acf_est, acf_ci = acf(group['SUM_UNCLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
      acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
      group['ACF_UNCLEAN_SOH'] = acf_est.astype(float)
      group['unclean_lj_lower'] = acf_lj_lower.astype(float)
      group['unclean_lj_upper'] = acf_lj_upper.astype(float)
      group[['unclean_ci_lower', 'unclean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=group.index).astype(float) 

      # autocorrelation clean soh
      acf_est, acf_ci = acf(group['SUM_CLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
      acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
      group['ACF_CLEAN_SOH'] = acf_est.astype(float)
      group['clean_lj_lower'] = acf_lj_lower.astype(float)
      group['clean_lj_upper'] = acf_lj_upper.astype(float)
      group[['clean_ci_lower', 'clean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=group.index).astype(float)

      # create the lag variable
      group.insert(0, 'LAG', range(0, len(group)))
      appended_data.append(group)
    except:
      pass

  # see pd.concat documentation for more info
  appended_data = pd.concat(appended_data)
  
  return(appended_data)

# base, customer, category group by tables
base_acf = autocorrelation_agg(pdf = base_group.toPandas(), group_agg = group_agg_base)
#cust_acf = autocorrelation_agg(pdf = customer_group.toPandas(), group_agg = group_agg_cust)
#category_acf = autocorrelation_agg(pdf = customer_group.toPandas(), group_agg = group_agg_category)

# COMMAND ----------

# Group parameters
group_agg_base = ['DISTRIBUTION_CHANNEL', 'MATERIAL_CODE', 'PLANT_CODE', 'SALES_ORG']
group_agg_cust = ['DISTRIBUTION_CHANNEL', 'StratCust', 'MATERIAL_CODE', 'PLANT_CODE', 'SALES_ORG']
group_agg_category = ['DISTRIBUTION_CHANNEL', 'SALES_ORG', 'PRODUCT_CATEGORY'] # new enhancement for Pablo

# COMMAND ----------

# DBTITLE 1,Create Pyspark Data Frame for result table
# create the schema for the resulting data frame - need to dynamically create it for different aggregates this one only works for group_base
def schema_type(schema_name):
  if(schema_name == 'matloc'):
    schema = StructType([StructField('LAG', LongType(), True),
                          StructField('DISTRIBUTION_CHANNEL', StringType(), True),
                          StructField('MATERIAL_CODE', StringType(), True),
                          StructField('PLANT_CODE', StringType(), True),
                          StructField('SALES_ORG', StringType(), True),
                          StructField('YEAR', IntegerType(), True),
                          StructField('MONTH', StringType(), True),
                          StructField('MONTH_DATE', DateType(), True),
                          StructField('FORECAST_DATE', DateType(), True),
                          StructField('SUM_UNCLEAN_SOH', DoubleType(), True),
                          StructField('SUM_CLEAN_SOH', DoubleType(), True),
                          StructField('ACF_UNCLEAN_SOH', DoubleType(), True),
                          StructField('ACF_CLEAN_SOH', DoubleType(), True),
                          StructField('unclean_lj_lower', DoubleType(), True),
                          StructField('unclean_lj_upper', DoubleType(), True),
                          StructField('unclean_ci_lower', DoubleType(), True),
                          StructField('unclean_ci_upper', DoubleType(), True),
                          StructField('clean_lj_lower', DoubleType(), True),
                          StructField('clean_lj_upper', DoubleType(), True),
                          StructField('clean_ci_lower', DoubleType(), True),
                          StructField('clean_ci_upper', DoubleType(), True)])
    
  elif(schema_name == 'stratcust'):
    # create the schema for the resulting data frame - need to dynamically create it for different aggregates this one only works for group_base
    schema = StructType([StructField('LAG', LongType(), True),
                          StructField('DISTRIBUTION_CHANNEL', StringType(), True),
                          StructField('MATERIAL_CODE', StringType(), True),
                          StructField('PLANT_CODE', StringType(), True),
                          StructField('SALES_ORG', StringType(), True),
                          StructField('StratCust', StringType(), True),
                          StructField('YEAR', IntegerType(), True),
                          StructField('MONTH', StringType(), True),
                          StructField('MONTH_DATE', DateType(), True),
                          StructField('FORECAST_DATE', DateType(), True),
                          StructField('SUM_UNCLEAN_SOH', DoubleType(), True),
                          StructField('SUM_CLEAN_SOH', DoubleType(), True),
                          StructField('ACF_UNCLEAN_SOH', DoubleType(), True),
                          StructField('ACF_CLEAN_SOH', DoubleType(), True),
                          StructField('unclean_lj_lower', DoubleType(), True),
                          StructField('unclean_lj_upper', DoubleType(), True),
                          StructField('unclean_ci_lower', DoubleType(), True),
                          StructField('unclean_ci_upper', DoubleType(), True),
                          StructField('clean_lj_lower', DoubleType(), True),
                          StructField('clean_lj_upper', DoubleType(), True),
                          StructField('clean_ci_lower', DoubleType(), True),
                          StructField('clean_ci_upper', DoubleType(), True)])
    
  elif(schema_name == 'category'):
    # create the schema for the resulting data frame - need to dynamically create it for different aggregates this one only works for group_base
    schema = StructType([StructField('LAG', LongType(), True),
                          StructField('DISTRIBUTION_CHANNEL', StringType(), True),
                          StructField('MATERIAL_CODE', StringType(), True),
                          StructField('PRODUCT_CATEGORY', StringType(), True),
                          StructField('PLANT_CODE', StringType(), True),
                          StructField('SALES_ORG', StringType(), True),
                          StructField('YEAR', IntegerType(), True),
                          StructField('MONTH', StringType(), True),
                          StructField('MONTH_DATE', DateType(), True),
                          StructField('FORECAST_DATE', DateType(), True),
                          StructField('SUM_UNCLEAN_SOH', DoubleType(), True),
                          StructField('SUM_CLEAN_SOH', DoubleType(), True),
                          StructField('ACF_UNCLEAN_SOH', DoubleType(), True),
                          StructField('ACF_CLEAN_SOH', DoubleType(), True),
                          StructField('unclean_lj_lower', DoubleType(), True),
                          StructField('unclean_lj_upper', DoubleType(), True),
                          StructField('unclean_ci_lower', DoubleType(), True),
                          StructField('unclean_ci_upper', DoubleType(), True),
                          StructField('clean_lj_lower', DoubleType(), True),
                          StructField('clean_lj_upper', DoubleType(), True),
                          StructField('clean_ci_lower', DoubleType(), True),
                          StructField('clean_ci_upper', DoubleType(), True)])
  return(schema)

# COMMAND ----------

# schema parameters
schemaBase = schema_type(schema_name = 'matloc')
schemaCust = schema_type(schema_name = 'customer')
schemaCat = schema_type(schema_name = 'category')

# COMMAND ----------

# DBTITLE 1,Autocorrelation Matloc Base
# schema parameters
schemaBase = schema_type(schema_name = 'matloc')

# function for autocorrelation
@pandas_udf(schemaBase, PandasUDFType.GROUPED_MAP)
def autocorrelation_agg(df):
  # ignore warnings about NA or 0 division
  np.seterr(divide = 'ignore', invalid = 'ignore')
  
  # sort by MONTH_DATE
  df = df.sort_values('MONTH_DATE')

  # length of the group wrt time
  length = len(df) - 1

  # autocorrelation unclean soh
  acf_est, acf_ci = acf(df['SUM_UNCLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
  acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
  df['ACF_UNCLEAN_SOH'] = acf_est.astype(float)
  df['unclean_lj_lower'] = acf_lj_lower.astype(float)
  df['unclean_lj_upper'] = acf_lj_upper.astype(float)
  df[['unclean_ci_lower', 'unclean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=df.index).astype(float) 

  # autocorrelation clean soh
  acf_est, acf_ci = acf(df['SUM_CLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
  acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
  df['ACF_CLEAN_SOH'] = acf_est.astype(float)
  df['clean_lj_lower'] = acf_lj_lower.astype(float)
  df['clean_lj_upper'] = acf_lj_upper.astype(float)
  df[['clean_ci_lower', 'clean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=df.index).astype(float)

  # create the lag variable
  df.insert(0, 'LAG', range(0, length + 1))
  
  # fill nas with 0
  df[acf_var] = df[acf_var].fillna(0)
  
  return(df)

# call
base_acf = base_group.groupby(group_agg_base).apply(autocorrelation_agg)

# COMMAND ----------

# DBTITLE 1,Autocorrelation Customer
# schema parameters
schemaCust = schema_type(schema_name = 'stratcust')

# function for autocorrelation
@pandas_udf(schemaCust, PandasUDFType.GROUPED_MAP)
def autocorrelation_agg(df):
  # ignore warnings about NA or 0 division
  np.seterr(divide = 'ignore', invalid = 'ignore')
  
  # sort by MONTH_DATE
  df = df.sort_values('MONTH_DATE')

  # length of the group wrt time
  length = len(df) - 1

  # autocorrelation unclean soh
  acf_est, acf_ci = acf(df['SUM_UNCLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
  acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
  df['ACF_UNCLEAN_SOH'] = acf_est.astype(float)
  df['unclean_lj_lower'] = acf_lj_lower.astype(float)
  df['unclean_lj_upper'] = acf_lj_upper.astype(float)
  df[['unclean_ci_lower', 'unclean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=df.index).astype(float) 

  # autocorrelation clean soh
  acf_est, acf_ci = acf(df['SUM_CLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
  acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
  df['ACF_CLEAN_SOH'] = acf_est.astype(float)
  df['clean_lj_lower'] = acf_lj_lower.astype(float)
  df['clean_lj_upper'] = acf_lj_upper.astype(float)
  df[['clean_ci_lower', 'clean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=df.index).astype(float)

  # create the lag variable
  df.insert(0, 'LAG', range(0, length + 1))
  
  # fill nas with 0
  df[acf_var] = df[acf_var].fillna(0)
  
  return(df)

# call
cust_acf = customer_group.groupby(group_agg_cust).apply(autocorrelation_agg)

# COMMAND ----------

# DBTITLE 1,Autocorrelation Category
# schema parameters
schemaCat = schema_type(schema_name = 'category')

# function for autocorrelation
@pandas_udf(schemaCat, PandasUDFType.GROUPED_MAP)
def autocorrelation_agg(df):
  # ignore warnings about NA or 0 division
  np.seterr(divide = 'ignore', invalid = 'ignore')
  
  # sort by MONTH_DATE
  df = df.sort_values('MONTH_DATE')

  # length of the group wrt time
  length = len(df) - 1

  # autocorrelation unclean soh
  acf_est, acf_ci = acf(df['SUM_UNCLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
  acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
  df['ACF_UNCLEAN_SOH'] = acf_est.astype(float)
  df['unclean_lj_lower'] = acf_lj_lower.astype(float)
  df['unclean_lj_upper'] = acf_lj_upper.astype(float)
  df[['unclean_ci_lower', 'unclean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=df.index).astype(float) 

  # autocorrelation clean soh
  acf_est, acf_ci = acf(df['SUM_CLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
  acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
  df['ACF_CLEAN_SOH'] = acf_est.astype(float)
  df['clean_lj_lower'] = acf_lj_lower.astype(float)
  df['clean_lj_upper'] = acf_lj_upper.astype(float)
  df[['clean_ci_lower', 'clean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=df.index).astype(float)

  # create the lag variable
  df.insert(0, 'LAG', range(0, length + 1))
  
  # fill nas with 0
  df[acf_var] = df[acf_var].fillna(0)
  
  return(df)

# call
category_acf = category_table.groupby(group_agg_category).apply(autocorrelation_agg)

# COMMAND ----------

# DBTITLE 1,Create PySpark Dataframe
cleanRawSOH_DF_BASE = spark.createDataFrame(base_acf)
cleanRawSOH_DF_CUST = spark.createDataFrame(cust_acf)
cleanRawSOH_DF_CATEGORY = spark.createDataFrame(category_acf)

# COMMAND ----------

# DBTITLE 1,Join product table with auto correlation table
master_base = cleanRawSOH_DF_BASE.join(product_table, on = ['PLANT_CODE', 'MATERIAL_CODE'], how = 'left')
master_cust = cleanRawSOH_DF_CUST.join(product_table, on = ['PLANT_CODE', 'MATERIAL_CODE'], how = 'left')
master_category = cleanRawSOH_DF_CATEGORY

# COMMAND ----------

display(master_base)

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD @ distr, mat, loc, sales org
master_base.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTD', mode='overwrite', properties=connection) 

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD @ distr, mat, loc, sales, org, strat cust
master_cust.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTDCUST', mode='overwrite', properties=connection) 

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD Category @ product category
master_category.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTDCategory', mode='overwrite', properties=connection) 