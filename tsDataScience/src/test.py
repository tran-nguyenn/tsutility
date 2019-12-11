# -*- coding: utf-8 -*-
import Season.src.utils as utils
import os
import sys

sys.path.append(os.getcwd())

#run_config = Season.src.utils.read_params_in_from_json('aggregation.json')

root = os.path.abspath(os.getcwd())
filename = 'aggregation.json'
filename = 'queries.json'
queries_params = utils.read_params_in_from_json(filename)

filename = 'base_table.sql'
queries = utils.read_sql(filename)

