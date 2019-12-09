"""
Utility functions used across platform
"""

import json
import os
import pandas as pd
import numpy as np

def read_params_in_from_json(filename):
    """
    :param filename: file name
    :return: dictionary of parameters
    """

    root = os.path.abspath(os.getcwd())
    credential_file_path = root + '\\seasonal_autocorrelation\\params\\aggregation\\' + filename
    json_string = open(credential_file_path).read()
    params = json.loads(json_string.replace('\n', ' ').replace('\t', ' '))

    return(params)

def group_for_parallel(df, group):
   """
   :param df: pandas dataframe
   :param group: group by statement
   :return: list of group by dataframes
   """

  groups = list()
  for k, v in df.groupby(group):
    groups.append(v)

  return(groups)
