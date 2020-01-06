"""
TS feature extraction and bucketizing
"""

import tsfresh as ts
import numpy as np
import pandas as pd

def default_features():
    """
    :param: Initially none
    :return Categorical_Response: categorical y varaible
    :return Quantatative_Response: quantatative y variable
    :return features: time series features
    :return cov_features: cov features
    :return datacol: data columns
    """
    Categorical_Response = "bm_wfa_bucket"
    Quantatative_Response = "winning_model_wfa"
    
    features = ['absolute_sum_of_changes','coeff_of_variation','count_above_mean','count_below_mean','cov_bucket','distribution_channel' ,'earliest_date','first_location_of_maximum' ,'first_location_of_minimum' ,'has_duplicate'     ,'has_duplicate_max' ,'has_duplicate_min' ,'holdout_training_ratio','is_disco','kurtosis','last_location_of_maximum'     ,'last_location_of_minimum' ,'latest_date','length','long_term_max','long_term_mean','long_term_min' ,'long_term_stdev'   ,'longest_strike_above_mean','longest_strike_below_mean','maximum','mean' ,'mean_abs_change','mean_change'  ,'mean_second_derivative_central' ,'median','minimum','missing_periods','near_disco','new_item','not_enough_history'    ,'percentage_of_reoccurring_datapoints_to_all_datapoints','percentage_of_reoccurring_values_to_all_values'    ,'ratio_value_number_to_time_series_length','sample_entropy' ,'skewness','standard_deviation','sum_of_reoccurring_data_points'    ,'sum_of_reoccurring_values','sum_values','time_series_length','time_series_length_bucket','time_series_length_in_years'      ,'training_length_bucket','training_length_in_years','variance','variance_larger_than_standard_deviation','deseasonalized_cov','raw_cov', 'trend_cov', 'seasons_cov', 'residual_cov']
    
    cov_features = ['deseasonzlied_cov', 'raw_cov', 'trend_cov', 'seasons_cov', 'residual_cov', 'cleaned_cov']
    
    datacol = [Categorical_Response] + [Quantatative_Response] + features + ['material'] + ['location'] + ['sales_org'] + ['distribution_channel']

    return(Categorical_Response, Quantatative_Response, features, cov_features, datacol)    
    
    