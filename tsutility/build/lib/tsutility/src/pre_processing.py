"""
Pre-processing data
"""
import numpy as np
from datetime import datetime

def group_for_parallel(df, response,  group):
    """
    :param df: pandas dataframe
    :param group: group by statement
    :return: list of group by dataframes
    """
    groups = list()

    for k, v in df.groupby(group):
      if(v[response].sum() != 0):
        groups.append(v)
      else:

        pass

    return(groups)

def end_date(df, date_variable, cutoff_date):
    """
    :param df: pandas dataframe
    :param date_variable: name of date variable (i.e. month/week)
    :param cutoff_date: cutoff date for the forecast
    :return: cleaned pandas dataframe includes only forecast up to max date
    """
    max_date = datetime.strptime(cutoff_date, '%Y-%m-%d')
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

def pre_process(df, response, group, date_variable, cutoff_date):
    """
    :param df: pandas dataframe
    :param group_agg: aggregation key (list)
    :param last date: date cutoff
    :return: list of grouped dataframes
    """
    # Forecast cutoff date
    df = end_date(df, date_variable, cutoff_date)

    # Create the group for parallelization
    list_dataframes = group_for_parallel(df, response, group)

    return(list_dataframes)
