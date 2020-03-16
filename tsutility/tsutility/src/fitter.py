def filter_columns(self, df):
      """
      :param df: spark dataframe which has RF and COV features
      :return mlResultsFinal: returns filtered pandas dataframe
      """
      # filter dataset
      #df = df.withColumnRenamed('prediction','dsXYZ')
      #keepCols =['material','location','sales_org','distribution_channel','division','product_category','winning_model_wfa','bm_wfa_bucket','coeff_of_variation','dsXYZ','covXYZ','rawcovXYZ', 'descovXYZ','raw_cov','deseasonalized_cov']
      keepCols = ['material','location','sales_org','distribution_channel','division','product_category','winning_model_wfa','bm_wfa_bucket','coeff_of_variation','covXYZ','rawcovXYZ', 'descovXYZ','raw_cov','deseasonalized_cov', 'xyz_run_date', 'xyz_run_id']
      # rows are duplicated because of repeating time component
      mlResultsFinal = df.select(*keepCols).distinct()

      return(mlResultsFinal)

def clean_xyz(self, df, XYZ):
      """
      :param df: spark dataframe after running XYZ classifer
      :param XYZ: column name for XYZ
      :return df: returns spark dataframe
      """
      # change bm_wfa_bucket from 1,2,3 X,Y,Z (originally wrong order)
      df_tmp = df.withColumn(XYZ,
                    when(col(XYZ) == 1, 'X').
                    when(col(XYZ) == 2, 'Y').
                    when(col(XYZ) == 3, 'Z').
                    otherwise('None'))

      return(df_tmp)
