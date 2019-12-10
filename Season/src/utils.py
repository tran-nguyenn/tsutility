"""
Utility functions used across platform
"""

import json
import os

def read_params_in_from_json(filename):
    """
    :param filename: file name
    :return: dictionary of parameters
    """

    root = os.path.abspath(os.getcwd())
    filename = 'aggregation.json'
    file_path = root + '\\Season\\params\\aggregation\\' + filename
    json_string = open(file_path).read()
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
