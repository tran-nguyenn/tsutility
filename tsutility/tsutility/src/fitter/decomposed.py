"""
Decomposed time series and COV calculation at a specific aggregation level
"""

import pandas as pd
import numpy as np
from statsmodels.tsa.seasonal import seasonal_decompose

def decomposed(df, target, month_date):
    """
    :param df: pandas dataframe
    :param target: target column of data to calculate decomposed
    :return: list of decomposed calculated dataframes
    """
    # ignore warnings about NA or 0 division
    np.seterr(divide = 'ignore', invalid = 'ignore')

    # sort values by forecast date
    df = df.sort_values(by = [month_date])
    df_decompose = pd.DataFrame(df)
    df_decompose.index = pd.DatetimeIndex(df_decompose[month_date])

    try:
      if len(df) < 6:
        seasons, trend = np.zeros(len(df), dtype = float), np.zeros(len(df), dtype = float)
        observed = df[target]
        residual = observed - trend
        df['observed'] = observed
        df['trend'] = trend
        df['residual'] = residual
        df['seasons'] = seasons
        # changes zero values
        df['observed'] = df['observed'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
        df['residual'] = df['residual'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
        df['trend'] = df['trend'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
        df['seasons'] = df['seasons'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
      if len(df) < 12:
        seasons, trend = np.zeros(len(df), dtype = float), np.zeros(len(df), dtype = float)
        observed = df[target]
        residual = observed - trend
        df['observed'] = observed
        df['trend'] = trend
        df['residual'] = residual
        df['seasons'] = seasons
        # changes zero values
        df['observed'] = df['observed'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
        df['residual'] = df['residual'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
        df['trend'] = df['trend'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
        df['seasons'] = df['seasons'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
      elif len(df) >= 12:
        series = df_decompose[target]
        result = seasonal_decompose(series, model='additive', extrapolate_trend='freq')
        df['observed'] = result.observed
        df['trend'] = result.trend
        df['seasons'] = result.seasonal
        df['residual'] = result.resid
        # changes zero values
        df['observed'] = df['observed'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
        df['residual'] = df['residual'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
        df['trend'] = df['trend'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
        df['seasons'] = df['seasons'].apply(lambda x: x + 0.00001 if x == 0.0 else x)

    except:

      pass

    df = df.reset_index(drop=True)

    return(df)
