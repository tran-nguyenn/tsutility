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
#- 2. Parameters
#-----------------------------------------------------------------------------------------------------

run_config = {
"GROUP_BY_DIMENSIONS":  ["material", "location"],
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
                                    "ANC": "apollo.ANC_XYZ"
                                    },
"CURRENT_DATE":                     "2020-01-01"
"TIME_SERIES_DB":                   {
                                    "db_flag": "yes",
                                    "db_name": "apollo.XYZ_ANC_TIME_SERIES"
                                    }
}

# parameters
GROUP = run_config['GROUP_BY_DIMENSIONS']
RESPONSE = run_config['RESPONSES']['unclean']
MONTH = run_config['RESPONSES']['month']
cov_range = run_config['coefficient_of_variation_bucket']['cov']
wfa_range = run_config['coefficient_of_variation_bucket']['wfa']
RESULT_TABLE = run_config['RESULT_TABLE']['ANC']
CURRENT_DATE = run_config['CURRENT_DATE']
TIME_SERIES_FLAG = run_config['db_flag']
TIME_SERIES_DB_NAME = run_config['db_name']

print(GROUP_AGG_MATLOC)


#------------------------------------------------------------------------------------------------------
#- 3. Function Imports
#-----------------------------------------------------------------------------------------------------

# queries
#%run ./sql/sql_queries

%run ./sql/fact/anc/order_mat_lctn_mnth

# functions
#%run ./src/tscalc

#%run ./src/tsxyz

#%run ./src/post_process

#%run ./src/utils

#%run ./src/pre_processing2

#------------------------------------------------------------------------------------------------------
#- 4. Data Imports
#-----------------------------------------------------------------------------------------------------

# query to db
dataset = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", xyz_base_anc).load()

#------------------------------------------------------------------------------------------------------
#- 5. Pre-Processing
#-----------------------------------------------------------------------------------------------------

# data pre-process: base table MATLOC: spark data frame, table name, return df object instead writing to db, last cut off date, aggregation level
dataset_process = pre_process(dataset, GROUP, CURRENT_DATE)

# parallelize and pass response variable
RDD = sc.parallelize(dataset_process, 100).map(lambda data: decomposed(data, RESPONSE)).map(lambda data: cov(data, RESPONSE)).collect()

# reset index after parallelized code
df = pd.concat(RDD, sort = True, ignore_index = True).reset_index()

# output time series results to database
if(TIME_SERIES_FLAG == "yes"):
    # write to db
    time_series_table = spark.createDataFrame(df)
    time_series_table.write.jdbc(url=jdbcUrl, table=TIME_SERIES_DB_NAME, mode='overwrite', properties=connection)

# Creating the XYZ labels (edit this such that it's only one table name)
df['covXYZ'] = xyz_label(df, 'coeff_of_variation', cov_range])
df['rawcovXYZ'] = xyz_label(df, 'raw_cov', cov_range)
df['descovXYZ'] = xyz_label(df, 'deseasonalized_cov', cov_range)
df['dsXYZ'] = xyz_label(df, 'winning_model_wfa', wfa_range)

#------------------------------------------------------------------------------------------------------
#- 6. Post-Processing
#-----------------------------------------------------------------------------------------------------

df_clean = clean_XYZ(df,['bm_wfa_bucket','descovXYZ','rawcovXYZ','covXYZ'])

#------------------------------------------------------------------------------------------------------
#- 7. Database
#-----------------------------------------------------------------------------------------------------

# write to db
hot_table = RESULT_TABLE

df_clean.write.jdbc(url=jdbcUrl, table=hot_table, mode='overwrite', properties=connection)
