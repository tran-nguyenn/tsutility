"""
Autocorrleation calculation at a specific aggregation level
"""

import pandas as pd
import numpy as np
import seasonal
from seasonal import fit_seasons, adjust_seasons
import statsmodels
from statsmodels.tsa.stattools import acf
from scipy.stats import variation
from statsmodels.tsa.seasonal import seasonal_decompose

def autocorrelation(df):
  """
  :param df: pandas dataframe
  :return: list of acf calculated dataframes

  """
  # ignore warnings about NA or 0 division
  np.seterr(divide = 'ignore', invalid = 'ignore')
  
  # fill null values in SUM_UNCLEAN_SOH and SUM_CLEAN_SOH with 0
  df[['SUM_UNCLEAN_SOH', 'SUM_CLEAN_SOH']] = df[['SUM_UNCLEAN_SOH', 'SUM_CLEAN_SOH']].fillna(0)

  # ACF features
  acf_var = ['ACF_UNCLEAN_SOH', 'ACF_CLEAN_SOH', 'unclean_ci_lower', 'unclean_ci_upper', 'unclean_lj_lower', 'unclean_lj_upper', 'clean_ci_lower', 'clean_ci_upper', 'clean_lj_lower', 'clean_lj_upper']

  try:
    # sort by MONTH_DATE
    df = df.sort_values('MONTH_DATE')

    # length of the group wrt time
    length = len(df) - 1

    # autocorrelation unclean soh
    acf_est, acf_ci = acf(df['SUM_UNCLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)

    acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
    df['ACF_UNCLEAN_SOH'] = acf_est.astype(float)
    df['unclean_lj_lower'] = acf_lj_lower.astype(float)
    df['unclean_lj_upper'] = acf_lj_upper.astype(float)
    df[['unclean_ci_lower', 'unclean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=df.index).astype(float)

    # autocorrelation clean soh
    acf_est, acf_ci = acf(df['SUM_CLEAN_SOH'], nlags = length, fft=False, alpha = 0.05)
    acf_lj_lower, acf_lj_upper = (-2 / np.sqrt(length)), (2 / np.sqrt(length))
    df['ACF_CLEAN_SOH'] = acf_est.astype(float)
    df['clean_lj_lower'] = acf_lj_lower.astype(float)
    df['clean_lj_upper'] = acf_lj_upper.astype(float)
    df[['clean_ci_lower', 'clean_ci_upper']] = pd.DataFrame(acf_ci.tolist(), index=df.index).astype(float)

    # create the lag variable
    df.insert(0, 'LAG', range(0, length + 1))

    # replace nas with 0s
    df[acf_var] = df[acf_var].fillna(0)

  except:
    pass

  return(df)
  
def decomposed(df):
    """
    :param df: pandas dataframe
    :param target: target column of data to calculate decomposed
    :return: list of decomposed calculated dataframes
    """
    target = 'SUM_UNCLEAN_SOH'
    # ignore warnings about NA or 0 division
    np.seterr(divide = 'ignore', invalid = 'ignore')
    
    # sort values by forecast date
    df = df.sort_values(by = ['month_date'])
    
    try:
      if len(df) < 6:
        seasons, trend = np.zeros(len(df), dtype = float), np.zeros(len(df), dtype = float)
        adjusted = df[target]
      if len(df) < 12:
        seasons, trend = fit_seasons(df[target])
        adjusted = adjust_seasons(df[target], seasons = seasons)
      else:
        seasons, trend = fit_seasons(df[target], period = 12)
        adjusted = adjust_seasons(df[target], seasons = seasons)

      if seasons is None:
        seasons = np.zeros(len(df), dtype = float)
        adjusted = df[target]

      residual = adjusted - trend
      df['adjusted'] = adjusted
      # changes zero values
      df['adjusted'] = df['adjusted'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
      df['residual'] = residual
      # changes zero values
      df['residual'] = df['residual'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
      df['trend'] = trend
      # changes zero values
      df['trend'] = df['trend'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
      df['seasons'] = seasons
      # changes zero values
      df['seasons'] = df['seasons'].apply(lambda x: x + 0.00001 if x == 0.0 else x)
    
    except:
      pass
      #df['adjusted'] = np.nan
      #df['residual'] = np.nan
      #df['trend'] = np.nan
      #df['seasons'] = np.nan
      
    return(df)

def _decomposed(df):
    """
    :param df: pandas dataframe
    :param target: target column of data to calculate decomposed
    :return: list of decomposed calculated dataframes
    """
    target = 'SUM_UNCLEAN_SOH'
    # ignore warnings about NA or 0 division
    np.seterr(divide = 'ignore', invalid = 'ignore')
    
    # sort values by forecast date
    df = df.sort_values(by = ['month_date'])
    #date_idx = pd.date_range(df['month_date'].min(), df['month_date'].max(), freq = 'M')
    df_decompose = pd.DataFrame(df)
    #df_decompose.index = pd.DatetimeIndex(date_idx)
    df_decompose.index = pd.DatetimeIndex(df_decompose['month_date'])
    
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
  
def cov(df):
    """
    :param df: pandas dataframe
    :return: cov calculated dataframe
    """
    # ignore warnings about NA or 0 division
    np.seterr(divide = 'ignore', invalid = 'ignore')
    
    target = 'SUM_UNCLEAN_SOH'
    cleaned = 'SUM_CLEAN_SOH'
    
    # sort values by forecast date / issues with index chaining
    df = df.sort_values(by = ['month_date'])
    
    # check for consecutive zeros anywhere in the data pre-processing
    #df_clean = df
    
    try:
        df['deseasonalized_cov'] = variation(df['residual'] + df['trend'], axis = 0, nan_policy = 'omit') * 100
        df['raw_cov'] = variation(df[target], axis = 0, nan_policy = 'omit') * 100
        #df['cleaned_cov'] = variation(df[cleaned], axis = 0, nan_policy = 'omit') * 100
        df['trend_cov'] = variation(df['trend'], axis = 0, nan_policy = 'omit') * 100
        df['seasons_cov'] = variation(df['seasons'], axis = 0, nan_policy = 'omit') * 100
        df['residual_cov'] = variation(df['residual'], axis = 0, nan_policy = 'omit') * 100
        # columns to join
        #cols_to_use = ['deseasonalized_cov', 'raw_cov', 'trend_cov', 'seasons_cov', 'residual_cov', 'FORECAST_DATE']
        
        # merge only non-zero values
        #df = pd.merge(df, df_clean[cols_to_use], how = 'left', on = 'FORECAST_DATE')
    
    except:
        pass
    
    return(df)