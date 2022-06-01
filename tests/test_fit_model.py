"""
Tests the fit_model function in run_model module.
"""

import pytest

import numpy as np
import pandas as pd

from src.run_model import fit_model

# Define input dataframe
df_in_values = [[ 0.283     ,  0.2       , -1.11467689],
                [ 0.135     ,  0.178     , -1.70356676],
                [ 0.24      ,  0.157     , -1.38005604],
                [ 0.36      ,  0.155     , -0.95440403]]
df_in_index = [18149, 66384, 11855, 60766]
df_in_columns = ["ACCESS2",
                 "BINGE",
                 "GHLTH"]
df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)

def test_fit_model():
    """
    Conducts happy path unit test for fit_model function.
    """

    # Define expected output
    param_true = {"access2": 3.594, "binge": 2.582, "intercept": -2.648}

    # Create test output
    params_test, _ = fit_model(df_in,
                          features = ["ACCESS2",
                                      "BINGE"],
                          response = "GHLTH",
                          method = "linearregression",
                          fit_intercept=True)
    # Round to avoid floating point issues
    params_test = {key:round(value,3) for (key,value) in params_test.items()}
         

    # Test equality
    assert param_true == params_test

def test_fit_model_key_err():
    """
    Conducts unhappy path unit test for fit_model function.

    Checks if KeyError raised for missing column.
    """

    # Create test output
    with pytest.raises(KeyError):
        fit_model(df_in,
                  features = ["ACCESS2", "ARTHRITIS", "BINGE", "BPHIGH", "BPMED", "CANCER", "CASTHMA",
                              "CHD", "CHECKUP", "CHOLSCREEN", "COPD", "CSMOKING", "DEPRESSION",
                              "DIABETES", "HIGHCHOL", "KIDNEY", "OBESITY", "STROKE",
                              "Midwest", "Northeast", "South", "Southwest", "scaled_TotalPopulation"],
                  response = "Not a column",
                  method = "linearregression",
                  fit_intercept=True)