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
GROUP_AGG_MAT = ['material']
GROUP_AGG_MATLOC = ['material', 'location', 'sales_org', 'distribution_channel']
GROUP_AGG_BASE = ['material', 'location', 'distribution_channel', 'sales_org']
GROUP_AGG_CUSTOMER = ['material', 'location', 'distribution_channel', 'sales_org', 'strategic_customer']
GROUP_AGG_CATEGORY = ['product_category', 'distribution_channel', 'sales_org']

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
product_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", product_query).load()
dataset = dataset.join(product_table, on = ['material', 'location'], how = 'left')


#------------------------------------------------------------------------------------------------------
#- 5. Pre-Processing
#-----------------------------------------------------------------------------------------------------

# drop columns
columns_to_drop = ['run_date_ar', 'run_date_at', 'run_id_at', 'run_id_ar', 'material_ar', 'material_at', 'Sales_Organization', 'RUN_DATE', 'Plant']
dataset = dataset.drop(*columns_to_drop)

# data pre-process: base table MATLOC: spark data frame, table name, return df object instead writing to db, last cut off date, aggregation level
ts_cov = data_pre_process(dataset, 'apollo.XYZ_COV_ANC', 'df', '2020-01-01', GROUP_AGG_MATLOC)

#------------------------------------------------------------------------------------------------------
#- 6. Random Forest
#-----------------------------------------------------------------------------------------------------

# RF
#Categorical_Response, Quantatative_Response, features, cov_features, datacol = default_features()
mlResultsRF = tsxyz().random_forest(df = ts_cov)

#------------------------------------------------------------------------------------------------------
#- 7. COV
#-----------------------------------------------------------------------------------------------------

# Add cov
mlResultscov_final = tsxyz().add_cov(mlResultsRF)

# filtering and cleaning
mlResultsFinal = tsxyz().filter_ml(mlResultscov_final)

#------------------------------------------------------------------------------------------------------
#- 8. Post-Processing
#-----------------------------------------------------------------------------------------------------

# input is spark dataframe output is a pandas dataframe
mlResultsCleanWFA =  tsxyz().clean_bm_wfa_bucket(mlResultsClean, 'bm_wfa_bucket')
mlResultsFinal = tsxyz().clean_xyz(mlResultsCleanWFA, 'dsXYZ')
mlResultsFinal = tsxyz().clean_xyz(mlResultsFinal, 'descovXYZ')
mlResultsFinal = tsxyz().clean_xyz(mlResultsFinal, 'rawcovXYZ')
mlResultsFinal = tsxyz().clean_xyz(mlResultsFinal, 'covXYZ')

#------------------------------------------------------------------------------------------------------
#- 9. Database
#-----------------------------------------------------------------------------------------------------

# write to db
hot_table = "apollo.XYZ_RF_ANC"

mlResultsFinal.write.jdbc(url=jdbcUrl, table=hot_table, mode='overwrite', properties=connection)
