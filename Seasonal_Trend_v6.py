# Databricks notebook source
# DBTITLE 1,Import Data from MYSQL
# Utility and Spark
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
base_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query_base_group).load()
customer_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query_stratcust_group).load()
# product table to join after acf and aggregation calculations
product_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", productTable).load()

# COMMAND ----------

# DBTITLE 1,Create Product Category Table
# Join product table to the anc base table
category_table = base.join(product_table, on = ['PLANT_CODE', 'MATERIAL_CODE'], how = 'left')

# COMMAND ----------

# DBTITLE 1,Join product table with base and customer table
# maybe try joining product table to the tables beforehand
base_table = base_table.join(product_table, on = ['MATERIAL_CODE', 'PLANT_CODE'], how = 'left')
customer_table = customer_table.join(product_table, on = ['MATERIAL_CODE', 'PLANT_CODE'], how = 'left')

# COMMAND ----------

# DBTITLE 1,Autocorrelation Function and Parallelization function
# need to create a parallelization group by function making a list of dataframes
def group_for_parallel(df, group):
  groups = list()
  for k, v in df.groupby(group):
    groups.append(v)
  return(groups)
    
# function for autocorrelation
def autocorrelation_agg(df):
  # ignore warnings about NA or 0 division
  np.seterr(divide = 'ignore', invalid = 'ignore')
  
  # fill null values in SUM_UNCLEAN_SOH and SUM_CLEAN_SOH with 0
  df[['SUM_UNCLEAN_SOH', 'SUM_CLEAN_SOH']] = df[['SUM_UNCLEAN_SOH', 'SUM_CLEAN_SOH']].fillna(0)
  
  # ACF features
  acf_var = ['ACF_UNCLEAN_SOH', 'ACF_CLEAN_SOH', 'unclean_ci_lower', 'unclean_ci_upper', 'unclean_lj_lower', 'unclean_lj_upper', 'clean_ci_lower', 'clean_ci_upper', 'clean_lj_lower', 'clean_lj_upper']
  
  try:
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
    
    # replace nas with 0s
    df[acf_var] = df[acf_var].fillna(0)
    
  except:
    pass

  return(df)

# COMMAND ----------

# DBTITLE 1,Group by parameters
# Group parameters
group_agg_base = ['DISTRIBUTION_CHANNEL', 'MATERIAL_CODE', 'PLANT_CODE', 'SALES_ORG']
group_agg_customer = ['DISTRIBUTION_CHANNEL', 'StratCust', 'MATERIAL_CODE', 'PLANT_CODE', 'SALES_ORG']
group_agg_category = ['DISTRIBUTION_CHANNEL', 'SALES_ORG', 'PRODUCT_CATEGORY'] # new enhancement for Pablo

# COMMAND ----------

# DBTITLE 1,Create the group for parallelization
# base, customer, category group by tables
# toPandas() takes a long time (5 minutes)
list_base_group = group_for_parallel(base_table.toPandas(), group_agg_base)
list_customer_group = group_for_parallel(customer_table.toPandas(), group_agg_customer)
list_category_group = group_for_parallel(category_table.toPandas(), group_agg_category)

# COMMAND ----------

# DBTITLE 1,Parallelization
baseRDD = sc.parallelize(list_base_group, 8)
customerRDD = sc.parallelize(list_customer_group, 8)
categoryRDD = sc.parallelize(list_category_group, 8)

# COMMAND ----------

# DBTITLE 1,Map Autocorrleation to partitioned data
base_ac = baseRDD.map(autocorrelation_agg)
customer_ac = customerRDD.map(autocorrelation_agg)
category_ac = categoryRDD.map(autocorrelation_agg)

# COMMAND ----------

# DBTITLE 1,Collect datasets
# run time
base_acf= base_ac.collect()
customer_acf = customer_ac.collect()
category_acf = category_ac.collect()

# COMMAND ----------

# DBTITLE 1,Concatenate the list of data frames into a single data set
df_base = pd.concat(base_acf, sort = True, ignore_index = True).reset_index()
df_customer = pd.concat(customer_acf, sort = True, ignore_index = True).reset_index()
df_category = pd.concat(category_acf, sort = True, ignore_index = True).reset_index()

# COMMAND ----------

display(df_base)

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD @ distr, mat, loc, sales org
master_base.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTD_test', mode='overwrite', properties=connection) 

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD @ distr, mat, loc, sales, org, strat cust
master_cust.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTDCUST_test', mode='overwrite', properties=connection) 

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD Category @ product category
category_acf.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTDCategory_test', mode='overwrite', properties=connection) 