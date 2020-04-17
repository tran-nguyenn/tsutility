import pandas as pd
import numpy as np

def residuals(df, response, month_date):
    df.sort_values(by = month_date)

    try:
      np.seterr(divide = 'ignore', invalid = 'ignore')
      mean = df['residual'].mean(axis=0)
      std = df['residual'].std(axis=0)
      lower = mean - 3 * std
      upper = mean + 3 * std
      df['resid_mean'] = mean
      df['resid_std'] = std
      df['lower_limit'] = lower
      df['upper_limit'] = upper
      df['lower_flag'] = np.where(df['residual']<lower,1,0)
      df['upper_flag'] = np.where(df['residual']>upper,1,0)
      df['clean_flag'] = np.where(df['lower_flag']+df['upper_flag']==1,'Yes','No')
      df.drop(['lower_flag','upper_flag'],axis=1)
    except:
      pass

    return(df)
