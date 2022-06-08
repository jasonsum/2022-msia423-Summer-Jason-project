"""
Tests the functions contained in featurize module.
"""

import pytest

import numpy as np
import pandas as pd

from src.featurize import reformat_measures, one_hot_encode

# Test reformat_measures function
def test_reformat_measures():
    """
    Conducts happy path unit test for reformat_measures function.
    """

    # Define input dataframe
    df_in_values = [["New York", "Onondaga", 36067, 36067013200, 2958, 34.2, 81.3,
            10.4, 16.2, 36.4],
        ["Massachusetts", "Plymouth", 25023, 25023525104, 6289, 26.4,
            72.2, 6.9, 10.7, 29.2],
        ["Florida", "Miami-Dade", 12086, 12086009806, 2631, 36.4, 77.3,
            7.3, 24.3, 31.7]]
    df_in_index = [43633, 31974, 16353]
    df_in_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
        "TotalPopulation", "BPHIGH", "BPMED", "CANCER", "GHLTH", "HIGHCHOL"]
    df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)
    # Define expected output
    df_true = pd.DataFrame(
            [["New York", "Onondaga", 36067, 36067013200, 2958, 0.342, 0.813,
        0.10400000000000001, -1.643421765249699, 0.364],
       ["Massachusetts", "Plymouth", 25023, 25023525104, 6289, 0.264,
        0.722, 0.069, -2.121757746414593, 0.292],
       ["Florida", "Miami-Dade", 12086, 12086009806, 2631, 0.364, 0.773,
        0.073, -1.1363018100969002, 0.317]],
            index = [43633, 31974, 16353],
            columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
                       "TotalPopulation", "BPHIGH", "BPMED", 
                       "CANCER", "GHLTH", "HIGHCHOL"])

    # Create test output
    df_test = reformat_measures(df_in,
                                make_floats=["BPHIGH", "BPMED", "CANCER", "GHLTH", "HIGHCHOL"],
                                make_logit= "GHLTH")

    # Test equality
    pd.testing.assert_frame_equal(df_true, df_test)

def test_reformat_measures_key_err():
    """
    Conducts unhappy path unit test for reformat_measures function.

    Checks if KeyError raised for missing column.
    """

    # Define input dataframe
    df_in_values = [["New York", "Onondaga", 36067, 36067013200, 2958, 34.2, 81.3,
            10.4, 16.2, 36.4],
        ["Massachusetts", "Plymouth", 25023, 25023525104, 6289, 26.4,
            72.2, 6.9, 10.7, 29.2],
        ["Florida", "Miami-Dade", 12086, 12086009806, 2631, 36.4, 77.3,
            7.3, 24.3, 31.7]]
    df_in_index = [43633, 31974, 16353]
    df_in_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
        "TotalPopulation", "BPHIGH", "BPMED", "CANCER", "GHLTH", "HIGHCHOL"]
    df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)
    # Create test output
    with pytest.raises(KeyError):
        reformat_measures(df_in,
                          make_floats=["BPHIGH", "BPMED", "CANCER", "GHLTH", "HIGHCHOL"],
                          make_logit= "Not a column")

# Test one_hot_encode function
def test_one_hot_encode():
    """
    Conducts happy path unit test for one_hot_encode function.
    """
    # Define input dataframe
    df_in_values = [["New York", "Onondaga", 36067, 36067013200, 2958, 34.2, 81.3,
            10.4, 16.2, 36.4],
        ["Massachusetts", "Plymouth", 25023, 25023525104, 6289, 26.4,
            72.2, 6.9, 10.7, 29.2],
        ["California", "Los Angeles", 12086, 12086009806, 2631, 36.4, 77.3,
            7.3, 24.3, 31.7]]
    df_in_index = [43633, 31974, 16353]
    df_in_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
        "TotalPopulation", "BPHIGH", "BPMED", "CANCER", "GHLTH", "HIGHCHOL"]
    df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)
    # Define region_state_mapping
    states_region_mapping = {
        "California": "West","New York": "Northeast","Massachusetts": "Northeast"}


    # Define expected output
    df_true = pd.DataFrame(
            [["New York", "Onondaga", 36067, 36067013200, 2958, 34.2, 81.3,
        10.4, 16.2, 36.4, "Northeast", 1],
       ["Massachusetts", "Plymouth", 25023, 25023525104, 6289, 26.4,
        72.2, 6.9, 10.7, 29.2, "Northeast", 1],
       ["California", "Los Angeles", 12086, 12086009806, 2631, 36.4,
        77.3, 7.3, 24.3, 31.7, "West", 0]],
            index = [43633, 31974, 16353],
            columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
                       "TotalPopulation", "BPHIGH", "BPMED", "CANCER", "GHLTH",
                       "HIGHCHOL", "region", "Northeast"])

    # Create test output
    df_test = one_hot_encode(df_in, states_region_mapping)

    # Test equality
    pd.testing.assert_frame_equal(df_true, df_test)

def test_one_hot_encode_key_err():
    """
    Conducts unhappy path unit test for one_hot_encode function.

    Checks if KeyError raised for missing column.
    """
    # Define input dataframe
    df_in_values = [["New York", "Onondaga", 36067, 36067013200, 2958, 34.2, 81.3,
            10.4, 16.2, 36.4],
        ["Massachusetts", "Plymouth", 25023, 25023525104, 6289, 26.4,
            72.2, 6.9, 10.7, 29.2],
        ["California", "Los Angeles", 12086, 12086009806, 2631, 36.4, 77.3,
            7.3, 24.3, 31.7]]
    df_in_index = [43633, 31974, 16353]
    df_in_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
        "TotalPopulation", "BPHIGH", "BPMED", "CANCER", "GHLTH", "HIGHCHOL"]
    df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)
    # Define region_state_mapping
    states_region_mapping = {
        "California": "West","New York": "Northeast","Massachusetts": "Northeast"}

    # Create test output
    with pytest.raises(KeyError):
        one_hot_encode(df_in.drop("StateDesc"), states_region_mapping)
