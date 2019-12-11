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
    file_path = root + '\\tsDataScience\\params\\' + filename
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

def read_sql(filename):
    """
    :param filename: file name
    :return: sql query as a string
    """
    
    root = os.path.abspath(os.getcwd())
    file_path = root + '\\tsDataScience\\sql\\' + filename
    sql = open(file_path, mode='r', encoding='utf-8-sig').read()
    sql = sql.replace('\n', ' ').replace('\t', ' ')
    
    return(sql)