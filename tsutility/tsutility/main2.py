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
"RESPONSES":    {"clean":   "CLEAN_SOH",
                 "unclean": "UNCLEAN_SOH",
                 "month":   "month_date"
                 },
"coefficient_of_variation_bucket": {"cov":  [50, 150.0, float("inf")],
                                    "wfa":  [0.3, 0.6, 1.0]
                                    },
"RESULT_TABLE":                     "apollo.ANC_XYZ",
}

# parameters
GROUP = run_config['GROUP_BY_DIMENSIONS']
RESPONSE = run_config['RESPONSES']['clean']
MONTH = run_config['RESPONSES']['month']
cov_range = run_config['coefficient_of_variation_bucket']['cov']
wfa_range = run_config['coefficient_of_variation_bucket']['wfa']

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
dataset_process = pre_process(dataset, GROUP, '2020-01-01')

RDD = sc.parallelize(dataset_process, 100).map(_decomposed).map(cov).collect()

df = pd.concat(RDD, sort = True, ignore_index = True).reset_index()

df_covXYZ = bucket_dependent(df, 'covXYZ', 'coeff_of_variation', cov_range])
df_rawcovXYZ = bucket_dependent(df_covXYZ, 'rawcovXYZ', 'raw_cov', cov_range)
df_descovXYZ = bucket_dependent(df_rawcovXYZ, 'descovXYZ', 'deseasonalized_cov', cov_range)
df_dsXYZ = bucket_dependent(df_descovXYZ, 'bm_wfa_bucket', 'winning_model_wfa', wfa_range)

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
def clean_XYZ(df,cols):
  """
  :param df: pandas DataFrame
  :param cols: list of columns to cleaned
  :return: pandas DataFrame
  """
  for i in cols:
    df = df.replace({i:{1:'X',2:'Y',3:'Z',0:'Z'}})
  return df

df_clean = clean_XYZ(df_filtered.toPandas(),['bm_wfa_bucket','descovXYZ','rawcovXYZ','covXYZ'])

#------------------------------------------------------------------------------------------------------
#- 8. Database
#-----------------------------------------------------------------------------------------------------

# write to db
hot_table = "apollo.XYZ_RF_ANC"

df_clean.write.jdbc(url=jdbcUrl, table=hot_table, mode='overwrite', properties=connection)
