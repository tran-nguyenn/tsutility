# Databricks notebook source
# DBTITLE 1,Import Data from MYSQL
# Utility and Spark
import pyspark.sql.functions as f

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

# Import Koalas
# requires koalas 0.19.0
import databricks.koalas as ks
import pandas as pd
import numpy as np

# COMMAND ----------

# DBTITLE 1,Data Acess
# Credentials
serverName= "datasciencelab1sqlsrv.database.windows.net"
databaseName= "datasciencelab1-sqldb"
port= "1433"
driver = "ODBC Driver 17 for SQL Server"
username = dbutils.secrets.get(scope='DSLabCreds',key='DSLabCred_UserName_ReadWrite')
password = dbutils.secrets.get(scope='DSLabCreds',key='DSLabCred_Password_ReadWrite')
connection = connect_to_dslab(username, password)
connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(serverName, port, databaseName)

# COMMAND ----------

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

# DBTITLE 1,Data Extraction: 
# Data Exctraction
attTable = "fcst.ancCleanRawMATLOC"
attTableCust = "fcst.ancCleanRawMATLOCCUST"
product = "mstr.HT_MATERIAL_PLANT_MASTER"

query = "select DISTRIBUTION_CHANNEL, MATERIAL_CODE, PLANT_CODE, SALES_ORG, FORECAST_DATE, format(FORECAST_DATE, 'MMMM') as MONTH, YEAR(FORECAST_DATE) as YEAR, UNCLEAN_SOH, CLEAN_SOH from " + attTable
query_cust = "select DISTRIBUTION_CHANNEL, StratCust, MATERIAL_CODE, PLANT_CODE, SALES_ORG, FORECAST_DATE, format(FORECAST_DATE, 'MMMM') as MONTH, YEAR(FORECAST_DATE) as YEAR, UNCLEAN_SOH, CLEAN_SOH from " + attTableCust
query_product = "select PLANT as PLANT_CODE, MATL as MATERIAL_CODE, PRODUCT_CATEGORY, DIVISION from " + product

base = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query).load()
stratcust = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query_cust).load()
product_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query_product).load()

# Get the MONTH date in date format (first day of the month)
base = base.withColumn('MONTH_DATE', f.trunc('FORECAST_DATE', 'month'))
stratcust = stratcust.withColumn('MONTH_DATE', f.trunc('FORECAST_DATE', 'month'))
# category table is joined with product to later group by category
category_table = base.join(product_table, on = ['PLANT_CODE', 'MATERIAL_CODE'], how = 'left')

# COMMAND ----------

# data extraction through Apollo ETL library
attTable = "fcst.ancCleanRawMATLOC"
attTableCust = "fcst.ancCleanRawMATLOCCUST"
product = "mstr.HT_MATERIAL_PLANT_MASTER"

query = "select DISTRIBUTION_CHANNEL, MATERIAL_CODE, PLANT_CODE, SALES_ORG, FORECAST_DATE, format(FORECAST_DATE, 'MMMM') as MONTH, YEAR(FORECAST_DATE) as YEAR, UNCLEAN_SOH, CLEAN_SOH from " + attTable
query_cust = "select DISTRIBUTION_CHANNEL, StratCust, MATERIAL_CODE, PLANT_CODE, SALES_ORG, FORECAST_DATE, format(FORECAST_DATE, 'MMMM') as MONTH, YEAR(FORECAST_DATE) as YEAR, UNCLEAN_SOH, CLEAN_SOH from " + attTableCust
query_product = "select PLANT as PLANT_CODE, MATL as MATERIAL_CODE, PRODUCT_CATEGORY, DIVISION from " + product

base = spark.createDataFrame(connector.get_data(query))
stratcust = spark.createDataFrame(connector.get_data(query_cust))
product_table = spark.createDataFrame(connector.get_data(query_product))

# Get the MONTH date in date format (first day of the month)
base = base.withColumn('MONTH_DATE', f.trunc('FORECAST_DATE', 'month'))
stratcust = stratcust.withColumn('MONTH_DATE', f.trunc('FORECAST_DATE', 'month'))
#category table is joined with product to later group by category
category_table = base.join(product_table, on = ['PLANT_CODE', 'MATERIAL_CODE'], how = 'left')

# COMMAND ----------

# DBTITLE 1,Create the forecast groups
# Create groupby in koalas and sum over forecast_date to remove duplicates include customer level data if customer = 'INCLUDE'
def forecast_group(df, aggregation):
  # Convert sql pyspark data frame into pandas and then convert to koalas
  kdf = ks.DataFrame(df)
  
  if(aggregation == 'customer'):
    kdf_group = kdf.groupby(['DISTRIBUTION_CHANNEL', 'StratCust', 'MATERIAL_CODE', 'PLANT_CODE', 'SALES_ORG', 'YEAR', 'MONTH', 'MONTH_DATE', 'FORECAST_DATE'])['UNCLEAN_SOH', 'CLEAN_SOH'].sum().reset_index()
  elif(aggregation == 'category'):
    kdf_group = kdf.groupby(['DISTRIBUTION_CHANNEL', 'PRODUCT_CATEGORY', 'SALES_ORG', 'YEAR', 'MONTH', 'MONTH_DATE', 'FORECAST_DATE'])['UNCLEAN_SOH', 'CLEAN_SOH'].sum().reset_index()
  elif(aggregation == 'base'):
    kdf_group = kdf.groupby(['DISTRIBUTION_CHANNEL', 'MATERIAL_CODE', 'PLANT_CODE', 'SALES_ORG', 'YEAR', 'MONTH', 'MONTH_DATE', 'FORECAST_DATE'])['UNCLEAN_SOH', 'CLEAN_SOH'].sum().reset_index()
  return(kdf_group)

# Create base tables and convert to pandas dataset
kdf_group_base = forecast_group(df = base, aggregation = 'base').toPandas()
kdf_group_cust = forecast_group(df = stratcust, aggregation = 'customer').toPandas()
kdf_group_category = forecast_group(df = category_table, aggregation = 'category').toPandas()

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
      acf_est, acf_ci = acf(group['UNCLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
      acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
      group['ACF_UNCLEAN_SOH'] = acf_est.astype(float)
      group['unclean_lj_lower'] = acf_lj_lower.astype(float)
      group['unclean_lj_upper'] = acf_lj_upper.astype(float)
      group[['unclean_ci_lower', 'unclean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=group.index).astype(float) 

      # autocorrelation clean soh
      acf_est, acf_ci = acf(group['CLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
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

# base level pass in an array to groupby as the second parameter
test = kdf_group_base.head(1000)
test2 = kdf_group_cust.head(1000)

# base, customer, category group by tables
base_acf = autocorrelation_agg(pdf = kdf_group_base, group_agg = group_agg_base)
cust_acf = autocorrelation_agg(pdf = kdf_group_cust, group_agg = group_agg_cust)
category_acf = autocorrelation_agg(pdf = kdf_group_category, group_agg = group_agg_category)

# COMMAND ----------

# DBTITLE 1,Convert data types and fill NA
# Convert data types and fill NAs
def clean_data(df, acf_var):
  # time variable to convert
  time_var = ['MONTH_DATE', 'FORECAST_DATE']
  
  # Change data types to string, numeric, and time
  df[acf_var] = df[acf_var].fillna(0)
  df.loc[:, time_var] = df.loc[:, time_var].apply(pd.to_datetime, format = '%Y-%m-%d', errors = 'ignore')
  
  return(df)

base_df = clean_data(df = base_acf, acf_var = acf_var)
cust_df = clean_data(df = cust_acf, acf_var = acf_var)
category_df = clean_data(df = category_acf, acf_var = acf_var)

# COMMAND ----------

# DBTITLE 1,Create PySpark Dataframe
cleanRawSOH_DF_BASE = spark.createDataFrame(base_df)
cleanRawSOH_DF_CUST = spark.createDataFrame(cust_df)
cleanRawSOH_DF_CATEGORY = spark.createDataFrame(category_df)

# COMMAND ----------

# DBTITLE 1,Join product table with auto correlation table
master_base = cleanRawSOH_DF_BASE.join(product_table, on = ['PLANT_CODE', 'MATERIAL_CODE'], how = 'left')
master_cust = cleanRawSOH_DF_CUST.join(product_table, on = ['PLANT_CODE', 'MATERIAL_CODE'], how = 'left')
master_category = cleanRawSOH_DF_CATEGORY

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD
master_base.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTD', mode='overwrite', properties=connection) 

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD CUST
master_cust.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTDCUST', mode='overwrite', properties=connection) 

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD Category
master_category.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTDCategory', mode='overwrite', properties=connection) 