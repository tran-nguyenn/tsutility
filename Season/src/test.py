# -*- coding: utf-8 -*-
import Season.src.utils
import os
import sys
import json

sys.path.append(os.getcwd())

#run_config = Season.src.utils.read_params_in_from_json('aggregation.json')

root = os.path.abspath(os.getcwd())
filename = 'aggregation.json'
file_path = root + '\\Season\\params\\aggregation\\' + filename
json_string = open(file_path).read()
params = json.loads(json_string.replace('\n', ' ').replace('\t', ' '))