"""
Autocorrleation calculation at a specific aggregation level
"""

import scipy
from statsmodels.tsa.stattools import acf, pacf
import pandas as pd
import numpy as np

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
