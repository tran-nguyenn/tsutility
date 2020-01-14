import pyodbc

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

# aggregations
GROUP_AGG_MAT = ['Material']
GROUP_AGG_MATLOC = ['material', 'location', 'sales_org', 'distribution_channel']
GROUP_AGG_BASE = ["Distribution_Chanel", "Material", "Plant", "Sales_Organization"]
GROUP_AGG_CUSTOMER = ["Distribution_Chanel", "Material", "Plant", "Sales_Organization", "Strategic_Customer"]
GROUP_AGG_CATEGORY = ["Distribution_Channel", "Sales_Organization", "Product_Category"]

# queries
%run ./sql/sql_queries

# functions
%run ./src/tscalc

%run ./src/utils

%run ./src/pre_processing

%run ./src/tsxyz

# query to db
dataset = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", matloc_xyz).load()
dataset_food = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", matloc_food_xyz).load()
dataset_all_sales_org = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", matloc_all_sales_org_xyz).load()
product_table = spark.read.format("jdbc").option("url", jdbcUrl).option("username",username).option("password",password).option("query", product_query).load()
dataset = dataset.join(product_table, on = ['material', 'location'], how = 'left')
dataset_all = dataset_all_sales_org.join(product_table, on = ['material', 'location'], how = 'left')
dataset_food = dataset_food.join(product_table, on = ['material', 'location'], how = 'left')

# drop columns
columns_to_drop = ['run_date_ar', 'run_date_at', 'run_id_at', 'run_id_ar', 'material_ar', 'material_at', 'Sales_Organization', 'RUN_DATE', 'Plant']
dataset = dataset.drop(*columns_to_drop)

# data pre-process
# Base table MATLOC: spark data frame, table name, return df object instead writing to db, last cut off date, aggregation level
ts_cov = data_pre_process(dataset, 'apollo.XYZ_COV_ANC', 'db', '2020-01-01', GROUP_AGG_MATLOC)

# RF
#Categorical_Response, Quantatative_Response, features, cov_features, datacol = default_features()
mlResultsRF = tsxyz().random_forest(df = ts_cov)

# Add cov
mlResultscov_final = tsxyz().add_cov(mlResultsRF)

# filtering and cleaning
mlResultsFinal = tsxyz().filter_ml(mlResultscov_final)

# write to db
hot_table = "apollo.XYZ_RF_ANC"
mlResultsFinal.write.jdbc(url=jdbcUrl, table=hot_table, mode='overwrite', properties=connection)
