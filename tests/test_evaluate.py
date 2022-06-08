"""
Tests the functions contained in evaluate module.
"""

import pytest
import pandas as pd

from src.evaluate import capture_rmse

def test_capture_rmse():
    """
    Conducts happy path unit test for capture_rmse function.
    """
    # Define input dataframe
    df_in_values = [[ 0.086     ,  0.187     , -1.71126277, -1.74854762],
                    [ 0.152     ,  0.198     , -1.79877704, -1.83784209],
                    [ 0.364     ,  0.141     , -0.70366568, -0.68524022],
                    [ 0.194     ,  0.183     , -1.5231373 , -1.62628582],
                    [ 0.169     ,  0.18      , -1.21396637, -1.18164287]]
    df_in_index = [1065, 9835, 10353, 17803, 18479]
    df_in_columns = ["GHLTH",
                    "predictions",
                    "ACCESS2",
                    "BINGE"]
    df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)

    # Define expected output
    rmse_true = 0.02750

    # Create test output
    rmse_test  = capture_rmse(df_in,
                          true_col = "GHLTH",
                          pred_col = "predictions",
                          comp_prop = True)
    # Round to avoid floating point issues
    rmse_test  = round(rmse_test, 5)

    # Test equality
    assert rmse_true == rmse_test

def test_capture_rmse_key_err():
    """
    Conducts unhappy path unit test for capture_rmse function.

    Checks if KeyError raised for missing column.
    """

    df_in_values = [[ 0.086     ,  0.187     , -1.71126277, -1.74854762],
                    [ 0.152     ,  0.198     , -1.79877704, -1.83784209],
                    [ 0.364     ,  0.141     , -0.70366568, -0.68524022],
                    [ 0.194     ,  0.183     , -1.5231373 , -1.62628582],
                    [ 0.169     ,  0.18      , -1.21396637, -1.18164287]]
    df_in_index = [1065, 9835, 10353, 17803, 18479]
    df_in_columns = ["GHLTH",
                    "predictions",
                    "ACCESS2",
                    "BINGE"]
    df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)

    # Create test output
    with pytest.raises(KeyError):
        capture_rmse(df_in,
                     true_col = "GHLTH",
                     pred_col = "Not a column",
                     comp_prop = True)
