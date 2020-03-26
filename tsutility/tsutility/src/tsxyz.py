class tsxyz():
  def __init__(self):
      """
      :param: Initially none
      :return Categorical_Response: categorical y varaible
      :return Quantatative_Response: quantatative y variable
      :return features: time series features
      :return cov_features: cov features
      :return datacol: data columns
      """
      self.Categorical_Response = "bm_wfa_bucket"
      self.Quantatative_Response = "winning_model_wfa"

      self.features = ['absolute_sum_of_changes','coeff_of_variation','count_above_mean','count_below_mean','cov_bucket','distribution_channel','earliest_date','first_location_of_maximum','first_location_of_minimum','has_duplicate','has_duplicate_max','has_duplicate_min','holdout_training_ratio','is_disco','kurtosis','last_location_of_maximum','last_location_of_minimum','latest_date','length','long_term_max','long_term_mean','long_term_min','long_term_stdev','longest_strike_above_mean','longest_strike_below_mean','maximum','mean','mean_abs_change','mean_change','mean_second_derivative_central','median','minimum','missing_periods','near_disco','new_item','not_enough_history','percentage_of_reoccurring_datapoints_to_all_datapoints','percentage_of_reoccurring_values_to_all_values','ratio_value_number_to_time_series_length','sample_entropy','skewness','standard_deviation','sum_of_reoccurring_data_points','sum_of_reoccurring_values','sum_values','time_series_length','time_series_length_bucket','time_series_length_in_years','training_length_bucket','training_length_in_years','variance','variance_larger_than_standard_deviation']

      self.cov_features = ['deseasonalized_cov', 'raw_cov', 'trend_cov', 'seasons_cov', 'residual_cov']

      self.datacol = [self.Categorical_Response] + [self.Quantatative_Response] + self.features + self.cov_features + ['material'] + ['location'] + ['sales_org'] + ['distribution_channel'] + ['product_category'] + ['division']

  def ExtractFeatureImp(self, featureImp, dataset, featuresCol):
      """
      :param featureImp:
      :param dataset:
      :param featuresCol:
      :return varlist: list of important features
      """
      list_extract = []
      for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
          list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
      varlist = pd.DataFrame(list_extract)
      varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])

      return(varlist.sort_values('score', ascending = False))

  def random_forest(self, df):
      """
      :param: list of pandas dataframes
      :return: spark dataframe
      """
      dataset = spark.createDataFrame(df)

      # bucketize the dependent variable
      #splits = [0.0, 0.3, 0.6, 1.0]
      #bucketizer = Bucketizer(splits=splits, inputCol = self.Quantatative_Response ,outputCol='bm_wfa_bucket')
      #bucketedData = bucketizer.setHandleInvalid('skip').transform(dataset)

      mlreadyData = dataset.select(*self.datacol)

      # one hot encoding and assembling
      encoding_var = [i[0] for i in mlreadyData.dtypes if (i[1]=='string') & (i[0]!=self.Categorical_Response) & (i[0]!=self.Quantatative_Response) & (i[0]!='material') &(i[0]!='location') & (i[0]!= 'sales_org') & (i[0]!='distribution_channel') & (i[0]!='product_category') & (i[0]!='division')]
      num_var = [i[0] for i in mlreadyData.dtypes if ((i[1]=='int') | (i[1]=='double') | (i[1]=='float')) & (i[0]!=self.Categorical_Response)& (i[0]!=self.Quantatative_Response) & (i[0]!='material') & (i[0]!='location') & (i[0]!= 'sales_org') & (i[0]!='distribution_channel') & (i[0]!='product_category') & (i[0]!='division')]

      string_indexes = [StringIndexer(inputCol = c, outputCol = 'IDX_' + c, handleInvalid = 'keep') for c in encoding_var]
      onehot_indexes = [OneHotEncoderEstimator(inputCols = ['IDX_' + c], outputCols = ['OHE_' + c]) for c in encoding_var]
      label_indexes = StringIndexer(inputCol = self.Categorical_Response, outputCol = 'label', handleInvalid = 'keep')
      assembler = VectorAssembler(inputCols = num_var + ['OHE_' + c for c in encoding_var], outputCol = "features")

      # drop na values
      mlreadyData = mlreadyData.dropna()

      ### Prepare for RF ###

      # split to train and test
      mlreadyTrain, mlreadyTest = mlreadyData.randomSplit([0.8,0.2],seed=2309)
      # Random Forest Classifier
      rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed = 2309,
                                  numTrees=10, cacheNodeIds = True, subsamplingRate = 0.7)
      # Create a pipeline
      pipe = Pipeline(stages = string_indexes + onehot_indexes + [assembler, label_indexes, rf])

      # fit the model
      rfModel = pipe.fit(mlreadyTrain)
      mlResultsTest = rfModel.transform(mlreadyTest)
      mlResultsTotal = rfModel.transform(mlreadyData)
      #mlResults = mlResultsTest.drop("rawPrediction", "probability","features")
      mlResults = mlResultsTest.drop("features")
      predictionAndLabel = mlResults.select("prediction", "label").rdd

      ### Evaluate Test Error ###

      # Select (prediction, true label) and compute test error
      evaluator = MulticlassClassificationEvaluator(
          labelCol="label", predictionCol="prediction", metricName="accuracy")
      accuracy = evaluator.evaluate(mlResults)
      print("Test Error = %g" % (1.0 - accuracy))

      modelsum = rfModel.stages[2]
      print(modelsum)  # summary only

      metrics = MulticlassMetrics(predictionAndLabel)

      # Overall statistics
      precision = metrics.precision()
      recall = metrics.recall()
      f1Score = metrics.fMeasure()
      print("Summary Stats")
      print("Precision = %s" % precision)
      print("Recall = %s" % recall)
      print("F1 Score = %s" % f1Score)

      # Statistics by class
      labels = mlResults.rdd.map(lambda lp: lp.label).distinct().collect()
      for label in sorted(labels):
          print("Class %s precision = %s" % (label, metrics.precision(label)))
          print("Class %s recall = %s" % (label, metrics.recall(label)))
          print("Class %s F1 Measure = %s" % (label, metrics.fMeasure(label, beta=1.0)))

      # Weighted stats
      print("Weighted recall = %s" % metrics.weightedRecall)
      print("Weighted precision = %s" % metrics.weightedPrecision)
      print("Weighted F(1) Score = %s" % metrics.weightedFMeasure())
      print("Weighted F(0.5) Score = %s" % metrics.weightedFMeasure(beta=0.5))
      print("Weighted false positive rate = %s" % metrics.weightedFalsePositiveRate)

      ### Confusion Matrix ###
      results_pd = mlResults.select(['label','prediction']).toPandas()
      cm = confusion_matrix(results_pd['label'].values,results_pd['prediction'].values)
      acc = accuracy_score(results_pd['label'].values,results_pd['prediction'].values)

      print('Confusion Matrix : \n\n {} Accuracy Score: \n\n {}'.format(cm,acc))

      return(mlResults)

  def add_cov(self, df):
      """
      :param df: spark dataset with RF features (x,y)
      :return mlResultscov_final: dataset with RF features plus cov
      """
      splits = [0.0, 40.0, 150.0, float("inf")]
      bucketizerCOV = Bucketizer(splits=splits, inputCol = 'coeff_of_variation' ,outputCol='covXYZ')
      bucketizerRAW = Bucketizer(splits=splits, inputCol = 'raw_cov' ,outputCol='rawcovXYZ')
      bucketizerDES = Bucketizer(splits=splits, inputCol = 'deseasonalized_cov' ,outputCol='descovXYZ')

      mlResultscov = bucketizerCOV.setHandleInvalid('skip').transform(df)
      mlResultsraw = bucketizerRAW.setHandleInvalid('skip').transform(mlResultscov)
      mlResultscov_final = bucketizerDES.setHandleInvalid('skip').transform(mlResultsraw)

      return(mlResultscov_final)

  def filter_columns(self, df):
      """
      :param df: spark dataframe which has RF and COV features
      :return mlResultsFinal: returns filtered pandas dataframe
      """
      # filter dataset
      #df = df.withColumnRenamed('prediction','dsXYZ')
      #keepCols =['material','location','sales_org','distribution_channel','division','product_category','winning_model_wfa','bm_wfa_bucket','coeff_of_variation','dsXYZ','covXYZ','rawcovXYZ', 'descovXYZ','raw_cov','deseasonalized_cov']
      #keepCols = ['material','location','sales_org','distribution_channel','division','product_category','winning_model_wfa','bm_wfa_bucket','coeff_of_variation','covXYZ','rawcovXYZ', 'descovXYZ','raw_cov','deseasonalized_cov', 'xyz_run_date', 'xyz_run_id']
      keepCols = ['material','location','sales_org','distribution_channel','division','product_category','winning_model_wfa','bm_wfa_bucket','coeff_of_variation','covXYZ','rawcovXYZ', 'descovXYZ','raw_cov','deseasonalized_cov', 'xyz_run_date', 'run_id']
      # rows are duplicated because of repeating time component
      mlResultsFinal = df.select(*keepCols).distinct()

      return(mlResultsFinal)


  def clean_bm_wfa_bucket(self, df, bucket):
      """
      :param df: spark dataframe after running XYZ classifer
      :param XYZ: column name for bm_wfa_bucket
      :return df: returns spark dataframe
      """
      # change bm_wfa_bucket from 2,1,0 X,Y,Z (originally wrong order)
      df_tmp =df.withColumn('bm_wfa_bucket_xyz',
                    when(col(bucket) == 1, 'Z').
                    when(col(bucket) == 2, 'Y').
                    when(col(bucket) == 3, 'X').
                    otherwise('None'))

      return(df_tmp)

    def clean_XYZ(df,cols):
      """
      :param df: pandas DataFrame
      :param cols: list of cov column names
      :return df: pandas DataFrame
      """
      for i in cols:
        df = df.replace({i:{1:'X',2:'Y',3:'Z',0:'Z'}})
      return df
