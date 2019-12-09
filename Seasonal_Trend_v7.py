from pyspark.sql.types import *

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

# DBTITLE 1,Retrieve Data
# base table
base = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", baseTable).load()
# table aggregations to run acf at the group level
base_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query_base_group).load()
customer_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query_stratcust_group).load()
# product table to join after acf and aggregation calculations
product_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", productTable).load()
# category table
category_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", query_product_group).load()

# COMMAND ----------

# DBTITLE 1,Join product table with base and customer table
# maybe try joining product table to the tables beforehand
base_table = base_table.join(product_table, on = ['MATERIAL_CODE', 'PLANT_CODE'], how = 'left')
customer_table = customer_table.join(product_table, on = ['MATERIAL_CODE', 'PLANT_CODE'], how = 'left')

# DBTITLE 1,Group by parameters
# COMMAND ----------

# DBTITLE 1,Create the group for parallelization
# base, customer, category group by tables
# toPandas() takes a long time (5 minutes)
list_base_group = group_for_parallel(base_table.toPandas(), group_agg_base)
list_customer_group = group_for_parallel(customer_table.toPandas(), group_agg_customer)
list_category_group = group_for_parallel(category_table.toPandas(), group_agg_category)

# COMMAND ----------

# DBTITLE 1,Parallelization
baseRDD = sc.parallelize(list_base_group, 100)
customerRDD = sc.parallelize(list_customer_group, 100)
categoryRDD = sc.parallelize(list_category_group, 100)

# COMMAND ----------

# DBTITLE 1,Map Autocorrleation to partitioned data
base_ac = baseRDD.map(autocorrelation_agg)
customer_ac = customerRDD.map(autocorrelation_agg)
category_ac = categoryRDD.map(autocorrelation_agg)

# COMMAND ----------

# DBTITLE 1,Collect datasets
# run time
base_acf= base_ac.collect()

# COMMAND ----------

customer_acf = customer_ac.collect()

# COMMAND ----------

category_acf = category_ac.collect()

# COMMAND ----------

# DBTITLE 1,Concatenate the list of data frames into a single data set
df_base = pd.concat(base_acf, sort = True, ignore_index = True).reset_index()
df_customer = pd.concat(customer_acf, sort = True, ignore_index = True).reset_index()
df_category = pd.concat(category_acf, sort = True, ignore_index = True).reset_index()

# COMMAND ----------

# DBTITLE 1,Rewrite Pandas Dataframe into Pyspark data frame
df_base_spark = spark.createDataFrame(df_base)

# COMMAND ----------

df_customer_spark = spark.createDataFrame(df_customer)

# COMMAND ----------

df_category_spark = spark.createDataFrame(df_category)

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD @ distr, mat, loc, sales org
df_base_spark.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTD', mode='overwrite', properties=connection)

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD @ distr, mat, loc, sales, org, strat cust
df_customer_spark.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTDCUST', mode='overwrite', properties=connection)

# COMMAND ----------

# DBTITLE 1,Write seasonal trend and auto correlation into mysql MTD Category @ product category
df_category_spark.write.jdbc(url=jdbcUrl, table='fcst.SeasonalAutoCorrelationMTDCategory', mode='overwrite', properties=connection)
