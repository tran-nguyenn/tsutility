"""
calculate the cov
"""

import pandas as pd
import numpy as np
from scipy.stats import variation

def cov(df, target, month_date):
    """
    :param df: pandas dataframe
    :return: cov calculated dataframe
    """
    # ignore warnings about NA or 0 division
    np.seterr(divide = 'ignore', invalid = 'ignore')

    # sort values by forecast date / issues with index chaining
    df = df.sort_values(by = [month_date])

    try:
        df['deseasonalized_cov'] = variation(df['residual'] + df['trend'], axis = 0, nan_policy = 'omit') * 100
        df['raw_cov'] = variation(df[target], axis = 0, nan_policy = 'omit') * 100
        df['residual_cov'] = variation(df['residual'], axis = 0, nan_policy = 'omit') * 100

    except:

        pass

    return(df)
