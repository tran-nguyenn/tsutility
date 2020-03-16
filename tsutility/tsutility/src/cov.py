"""
Decomposed time series and COV calculation at a specific aggregation level
"""

import pandas as pd
import numpy as np
from scipy.stats import variation
from statsmodels.tsa.seasonal import seasonal_decompose

def __init__(self, unclean_soh, clean_soh, month_date):
    """
    :param: intialize the string name of unclean, clean soh, and month date column
    """
    self.unclean_soh = unclean_soh
    self.clean_soh = clean_soh
    self.month_date = month_date

def decomposed(self, df):
    """
    :param df: pandas dataframe
    :param target: target column of data to calculate decomposed
    :return: list of decomposed calculated dataframes
    """
    # ignore warnings about NA or 0 division
    np.seterr(divide = 'ignore', invalid = 'ignore')

    # sort values by forecast date
    df = df.sort_values(by = [self.month_date])
    df_decompose = pd.DataFrame(df)
    df_decompose.index = pd.DatetimeIndex(df_decompose[self.month_date])

    try:
      if len(df) < 6:
        seasons, trend = np.zeros(len(df), dtype = float), np.zeros(len(df), dtype = float)
        observed = df[self.unclean_soh]
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
        observed = df[self.unclean_soh]
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
        series = df_decompose[self.unclean_soh]
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

def cov(self, df):
    """
    :param df: pandas dataframe
    :return: cov calculated dataframe
    """
    # ignore warnings about NA or 0 division
    np.seterr(divide = 'ignore', invalid = 'ignore')

    # sort values by forecast date / issues with index chaining
    df = df.sort_values(by = [self.month_date])

    try:
        df['deseasonalized_cov'] = variation(df['residual'] + df['trend'], axis = 0, nan_policy = 'omit') * 100
        df['raw_cov'] = variation(df[self.unclean_soh], axis = 0, nan_policy = 'omit') * 100
        df['trend_cov'] = variation(df['trend'], axis = 0, nan_policy = 'omit') * 100
        df['seasons_cov'] = variation(df['seasons'], axis = 0, nan_policy = 'omit') * 100
        df['residual_cov'] = variation(df['residual'], axis = 0, nan_policy = 'omit') * 100

    except:

        pass

    return(df)
