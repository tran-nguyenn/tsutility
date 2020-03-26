"""
Pre-processing data
"""
import numpy as np
from datetime import datetime

def group_for_parallel(df, target,  group):
    """
    :param df: pandas dataframe
    :param group: group by statement
    :return: list of group by dataframes
    """
    target = 'SUM_UNCLEAN_SOH'
    groups = list()

    for k, v in df.groupby(group):
      if(v[target].sum() != 0):
        groups.append(v)
      else:

        pass

    return(groups)

def end_date(df, month_var):
    """
    :param df: pandas dataframe
    :return: cleaned pandas dataframe includes only forecast up to max date
    """
    max_date = datetime.strptime(month_var, '%Y-%m-%d')
    df_end = df[df[date_variable] < max_date]

    return(df_end)

def remove_null_var(df):
    """
    :param df: pandas dataframe
    :return: removes rows with distr, division, mat, loc, product category, sales_org that are null
    """
    try:
        columns = ['distribution_channel', 'Division', 'Material', 'Plant', 'Product_Category', 'Sales_Organization']

        df = df[df[columns].notnull().all(1)]
    except:

        pass

    return(df)

def bucket_dependent(df, name, dependent, splits):
    """
    :param df: pandas dataframe
    :param name: name of dependent variable
    :param dependent: dependent variable
    :param splits: array of splits
    :return df: pandas dataframe with bucketed dependent variables
    """
    df[name] = np.searchsorted(splits, df[dependent].values)

return(df)

def pre_process(df, group_agg, last_date):
    """
    :param df: pandas dataframe
    :param group_agg: aggregation key (list)
    :param last date: date cutoff
    :return: list of grouped dataframes
    """
    # Forecast cutoff date
    df = end_date(df, last_date)

    # Create the group for parallelization
    list_dataframes = group_for_parallel(df, group_agg)

    return(list_dataframes)
