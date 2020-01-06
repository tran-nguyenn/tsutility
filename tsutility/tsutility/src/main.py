def main(df, table, group_agg):
  # Pre-process data
  df = df.toPandas()
  
  # Remove any null values at the aggregation level
  df = remove_null_var(df)
  
  # Forecast cutoff date
  df = end_date(df, '2019-12-01')
  
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
  
  # Rewrite Pandas Dataframe into Pyspark data frame
  df_spark = spark.createDataFrame(df_combined)
  
  # Write to DB @ aggregation level
  df_spark.write.jdbc(url = jdbcUrl, table = table, mode = 'overwrite', properties = connection)
  
  # Finished
  print("Finished calucating ACF to table: ")
  return(df_spark)# -*- coding: utf-8 -*-

