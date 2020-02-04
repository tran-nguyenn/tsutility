"""
Pre-processing data
"""
import numpy as np
from datetime import datetime

def group_for_parallel(df, group):
    """
    :param df: pandas dataframe
    :param group: group by statement
    :return: list of group by dataframes
    """
    target = 'SUM_UNCLEAN_SOH'
    groups = list()
    
    for k, v in df.groupby(group):
      if(v[target].sum() != 0):
        groups.append(v)
      else:
          
        pass

    return(groups)

def end_date(df, max_date):
    """
    :param df: pandas dataframe
    :return: cleaned pandas dataframe includes only forecast up to max date
    """
    date_variable = 'month_date'
    max_date = datetime.strptime(max_date, '%Y-%m-%d')
    df_end = df[df[date_variable] < max_date]
    
    return(df_end)
    
def remove_null_var(df):
    """
    :param df: pandas dataframe
    :return: removes rows with distr, division, mat, loc, product category, sales_org that are null
    """
    try:
      columns = ['distribution_channel', 'Division', 'Material', 'Plant', 'Product_Category', 'Sales_Organization']

      df = df[df[columns].notnull().all(1)]
    except:
        
      pass
    
    return(df)

def round_to_decimal(df):
  """
  :param df: pandas dataframe
  :return: columns rounded
  """
  df = df[['deseasonalized_cov', 'raw_cov', 'trend_cov', 'seasons_cov', 'residual_cov']].round(decimals=4)
  
  return(df)
  
def bucket_dependent(df, name, dependent, splits):
  """
  :param df: pandas dataframe
  :param name: name of dependent variable
  :param dependent: dependent variable
  :param splits: array of splits
  :return df: pandas dataframe with bucketed dependent variables
  """
  df[name] = np.searchsorted(splits, df[dependent].values)
  
  return(df)
  
def random_number(df):
  """
  :param df: pandas dataframe
  :return df: adds xyz_run_id
  """
  df['xyz_run_id'] = np.random.randint(100000, 99999, len(df)).astype(str)
  
  return(df)
  
###### Main pre-process function ######
def data_pre_process(df, table, db, last_date, group_agg, arrow):
  """
  :param df: spark dataset imported from db
  :param table: table name (string)
  :param db: write to db "db" or not "df"
  :param group_agg: list of group aggregation by case sensitive column name
  :param arrow: string for "true" or "false" set true for smaller datasets and false for larger datasets
  :return df_combined: returns combined pandas dataframe
  """
  # Needs to be here to convert spark to python object for big datasets
  #spark.conf.set("spark.sql.execution.arrow.enabled", arrow)
  
  # Pre-process data
  df = df.toPandas()

  # Forecast cutoff date
  df = end_date(df, last_date)
  
  # Create the group for parallelization
  list_dataframes = group_for_parallel(df, group_agg)
  
  # Parallelization
  RDD = sc.parallelize(list_dataframes, 100)
  
  # Map to partitioned data
  RDD_mapped = RDD.map(decomposed).map(cov)
  
  # Collect datasets
  df_calc = RDD_mapped.collect()
  
  # Concatenate the list of data frames into a single data set
  df_combined = pd.concat(df_calc, sort = True, ignore_index = True).reset_index()
  
  # Create categorical dependent variables
  df_covXYZ = bucket_dependent(df_combined, 'covXYZ', 'coeff_of_variation', [50, 150, float("inf")])
  df_rawcovXYZ = bucket_dependent(df_covXYZ, 'rawcovXYZ', 'raw_cov', [50, 150.0, float("inf")])
  df_descovXYZ = bucket_dependent(df_rawcovXYZ, 'descovXYZ', 'deseasonalized_cov', [50, 150.0, float("inf")])
  df_dsXYZ = bucket_dependent(df_descovXYZ, 'bm_wfa_bucket', 'winning_model_wfa', [0.3, 0.6, 1.0])
  
  # add xyz_run_id
  df_combined = random_number(df_dsXYZ)
  
  if(db == 'db'):
  
    # Rewrite Pandas Dataframe into Pyspark data frame
    df_spark = spark.createDataFrame(df_combined)

    # Write to DB @ aggregation level
    df_spark.write.jdbc(url = jdbcUrl, table = table, mode = 'overwrite', properties = connection)
    
    return(df_combined)
  
  elif(db == 'df'):  
    # Finished
    print("Finished calucating ACF to table: ")
    return(df_combined)