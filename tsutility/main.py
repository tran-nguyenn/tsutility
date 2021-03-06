import pyodbc

#------------------------------------------------------------------------------------------------------
#- 1. Database Credentials
#-----------------------------------------------------------------------------------------------------

# CAN BE WRITTEN IN DATABRICKS

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
#- 2. Function Imports
#-----------------------------------------------------------------------------------------------------

# queries
%run ./sql/sql_queries

# functions
%run ./src/fitter/cov

%run ./src/fitter/decomposed

%run ./src/fitter/residuals

%run ./src/pre_processing

%run ./src/post_processing

#------------------------------------------------------------------------------------------------------
#- 3. Parameters
#-----------------------------------------------------------------------------------------------------
 
run_config = {
"GROUP_BY_DIMENSIONS":  ["material", "location", "sales_org", "distribution_channel"],
"RESPONSES":    {
                 "clean":   "CLEAN_SOH",
                 "unclean": "UNCLEAN_SOH",
                 "month":   "month_date"
                 },
"coefficient_of_variation_bucket": {
                                    "cov":  [50, 150.0, float("inf")],
                                    "wfa":  [0.3, 0.6, 1.0]
                                    },
"RESULT_TABLE":                     {
                                    "ANC": "apollo.ANC_XYZ",
                                    "BABY": "apollo.BABY_XYZ",
                                    "FOOD": "apollo.FOOD_XYZ"
                                    },
"CUTOFF_DATE":                     "2020-04-01"
}

# parameters
GROUP = run_config['GROUP_BY_DIMENSIONS']
RESPONSE = run_config['RESPONSES']['unclean']
MONTH = run_config['RESPONSES']['month']
cov_range = run_config['coefficient_of_variation_bucket']['cov']
wfa_range = run_config['coefficient_of_variation_bucket']['wfa']
RESULT_TABLE = run_config['RESULT_TABLE']['FOOD']
CUTOFF_DATE = run_config['CUTOFF_DATE']
INPUT_DATA = food_sql

print(CUTOFF_DATE)
print(GROUP)

#------------------------------------------------------------------------------------------------------
#- 4. Data Imports
#-----------------------------------------------------------------------------------------------------

# query to db
dataset = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", INPUT_DATA).load().toPandas()

#------------------------------------------------------------------------------------------------------
#- 5. Pre-Processing
#-----------------------------------------------------------------------------------------------------

# data pre-process: base table MATLOC: spark data frame, table name, return df object instead writing to db, last cut off date, aggregation level
dataset_process = pre_process(dataset, GROUP, CURRENT_DATE)

# parallelize and pass response variable
RDD = sc.parallelize(dataset_process, 100).map(lambda data: decomposed(data, RESPONSE, MONTH)).map(lambda data: residuals(data, RESPONSE, MONTH)).map(lambda data: cov(data, RESPONSE, MONTH)).collect()

# reset index after parallelized code
df = pd.concat(RDD, sort = True, ignore_index = True).reset_index()

# XYZ label
df['resXYZ'] = xyz_label(df, 'residual_cov', cov_range)
df['rawcovXYZ'] = xyz_label(df, 'raw_cov', cov_range)
df['descovXYZ'] = xyz_label(df, 'deseasonalized_cov', cov_range)

#------------------------------------------------------------------------------------------------------
#- 6. Post-Processing
#-----------------------------------------------------------------------------------------------------

df_clean = clean_XYZ(df,['descovXYZ','rawcovXYZ','resXYZ'])

#------------------------------------------------------------------------------------------------------
#- 7. Database
#-----------------------------------------------------------------------------------------------------

# write to db
hot_table = RESULT_TABLE

df_final = spark.createDataFrame(df_clean)

df_final.write.jdbc(url=jdbcUrl, table=hot_table, mode='overwrite', properties=connection)
