import pyodbc

#------------------------------------------------------------------------------------------------------
#- 1. Database Credentials
#-----------------------------------------------------------------------------------------------------

# credentials
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

#------------------------------------------------------------------------------------------------------
#- 2. Parameters
#-----------------------------------------------------------------------------------------------------

# aggregations
GROUP_AGG_MATLOC = ['material', 'location', 'sales_org', 'distribution_channel']
GROUP_AGG_CUSTOMER = ['material', 'location', 'sales_org','distribution_channel', 'strategic_customer']

#------------------------------------------------------------------------------------------------------
#- 3. Function Imports
#-----------------------------------------------------------------------------------------------------

# queries
%run ./sql/sql_queries

# functions
%run ./src/tscalc

%run ./src/tsxyz

%run ./src/utils

%run ./src/pre_processing

#------------------------------------------------------------------------------------------------------
#- 4. Data Imports
#-----------------------------------------------------------------------------------------------------

# query to db
dataset = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", matloc_xyz).load()

#------------------------------------------------------------------------------------------------------
#- 5. Pre-Processing
#-----------------------------------------------------------------------------------------------------

# drop columns
columns_to_drop = ['run_date_ar', 'run_date_at', 'run_id_at', 'run_id_ar', 'material_ar', 'material_at', 'Sales_Organization', 'RUN_DATE', 'Plant']
dataset = dataset.drop(*columns_to_drop)

# data pre-process: base table MATLOC: spark data frame, table name, return df object instead writing to db, last cut off date, aggregation level
ts_cov = data_pre_process(dataset, 'apollo.XYZ_COV_ANC', 'df', '2020-01-01', GROUP_AGG_MATLOC)

# join mlResultsRF (with apollo results) to the full dataset
df_cov = spark.createDataFrame(ts_cov)

#------------------------------------------------------------------------------------------------------
#- 6. Random Forest (skip for now)
#-----------------------------------------------------------------------------------------------------

# RF
#Categorical_Response, Quantatative_Response, features, cov_features, datacol = default_features()
#mlResultsRF = tsxyz().random_forest(df = ts_cov)

#------------------------------------------------------------------------------------------------------
#- 7. Post-Processing
#-----------------------------------------------------------------------------------------------------

# filtering and cleaning
df_filtered = tsxyz().filter_columns(df_cov)

# add filter and xyz renaming
df1 = tsxyz().clean_xyz(df_filtered, 'bm_wfa_bucket')
df2 = tsxyz().clean_xyz(df1, 'descovXYZ')
df3 = tsxyz().clean_xyz(df2, 'rawcovXYZ')
df4 = tsxyz().clean_xyz(df3, 'covXYZ')

#------------------------------------------------------------------------------------------------------
#- 8. Database
#-----------------------------------------------------------------------------------------------------

# write to db
hot_table = "apollo.XYZ_RF_ANC"

df4.write.jdbc(url=jdbcUrl, table=hot_table, mode='overwrite', properties=connection)
