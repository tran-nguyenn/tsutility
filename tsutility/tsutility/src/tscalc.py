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

def autocorrelation_agg(df):
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
    df = df.sort_values(by = ['FORECAST_DATE'])
      
    if np.all(df[target].head(6) == 0):
      seasons, trend = np.zeros(len(df)), np.zeros(len(df))
      adjusted = df[target]
    if np.all(df[target].head(12) == 0):
      seasons, trend = fit_seasons(df[target])
      adjusted = adjust_seasons(df[target], seasons = seasons)
    else:
      seasons, trend = fit_seasons(df[target], period = 12)
      adjusted = adjust_seasons(df[target], seasons = seasons)
    if seasons is None:
      seasons = np.zeros(len(df))
      adjusted = df[target]
    
    residual = adjusted - trend
    df['adjusted'] = adjusted
    df['residual'] = residual
    df['trend'] = trend
    df['seasons'] = seasons
      
    return(df)
    
def cov(df):
    """
    :param df: pandas dataframe
    :return: cov calculated dataframe
    """
    target = 'SUM_UNCLEAN_SOH'
    
    df['deseasonalized_cov'] = variation(df['residual'] + df['trend'], axis = 0) * 100
    df['raw_cov'] = variation(df[target], axis = 0) * 100
    df['trend_cov'] = variation(df['trend'], axis = 0) * 100
    df['season_cov'] = variation(df['seasons'], axis = 0) * 100
    df['residual_cov'] = variation(df['residual'], axis = 0) * 100
    
    return(df)
    
    
