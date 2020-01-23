"""
Pre-processing data
"""
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

def remove_zeros(df):
    """
    :param df: pandas dataframe
    :return: cleaned pandas dataframe
    """
    target = 'SUM_UNCLEAN_SOH'
    df = df.loc[df[target].sum != 0]
    return(df)

def zero_padding(params):
  def _zero_padding(df):
      """
      :param df: pandas dataframe
      :param params: list of column names
      :return: 0 values will be padded with 0.00001
      """
      #print(params)
      df['test'] = df[params].apply(lambda x: x + 0.00001 if x == 0 else x)

      return(df)
  return(_zero_padding)

def zero_padding_test(df):
  df['test'] = df['trend'].apply(lambda x: x + 0.00001 if x.any() == 0 else x)
  return(df)
    
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
  spark.conf.set("spark.sql.execution.arrow.enabled", arrow)
  
  # Pre-process data
  df = df.toPandas()

  # Remove any null values at the aggregation level
  #df = remove_null_var(df)
  
  # Forecast cutoff date
  df = end_date(df, last_date)
  
  # Create the group for parallelization
  list_dataframes = group_for_parallel(df, group_agg)
  
  # Parallelization
  RDD = sc.parallelize(list_dataframes, 100)
  
  # Map to partitioned data
  RDD_mapped = RDD.map(_decomposed).map(cov)
  
  # Collect datasets
  df_calc = RDD_mapped.collect()
  
  # Concatenate the list of data frames into a single data set
  df_combined = pd.concat(df_calc, sort = True, ignore_index = True).reset_index()
  
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