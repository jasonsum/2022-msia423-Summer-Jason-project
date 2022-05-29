"""
Tests the drop_null_responses function in clean module.
"""

import pytest

import numpy as np
import pandas as pd

from src.clean import drop_null_responses

# Define input dataframe
df_in_values = [["Michigan", "Wayne", 26163, 26163543900, 901,
        "{'type': 'Point', 'coordinates': [-83.2493016, 42.38181837]}",
        14.2, None, 10.1],
       ["Ohio", "Summit", 39153, 39153503300, 5606,
        "{'type': 'Point', 'coordinates': [-81.49716013, 41.04049422]}",
        13.1, 11.1, 21.7],
       ["South Carolina", "Orangeburg", 45075, 45075010200, 5097,
        "{'type': 'Point', 'coordinates': [-80.38981016, 33.3179319]}",
        12.2, 30.5, 13.2]]
df_in_index = [0,1,2]
df_in_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
                        "TotalPopulation", "Geolocation", "COPD", "GHLTH", "MHLTH"]
df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)

def test_drop_null_responses():
    """
    Conducts happy path unit test for drop_null_responses function.
    """

    # Define expected output
    df_true = pd.DataFrame(
            [["Ohio", "Summit", 39153, 39153503300, 5606,
        "{'type': 'Point', 'coordinates': [-81.49716013, 41.04049422]}",
        13.1, 11.1, 21.7],
       ["South Carolina", "Orangeburg", 45075, 45075010200, 5097,
        "{'type': 'Point', 'coordinates': [-80.38981016, 33.3179319]}",
        12.2, 30.5, 13.2]],
            index = [1,2],
            columns = ["StateDesc", 
                       "CountyName", 
                       "CountyFIPS", 
                       "LocationID",
                       "TotalPopulation",
                       "Geolocation", 
                       "COPD", 
                       "GHLTH", 
                       "MHLTH"])

    # Create test output
    df_test = drop_null_responses(df_in, "GHLTH")

    # Test equality
    pd.testing.assert_frame_equal(df_true, df_test)

def test_pivot_measures_key_err():
    """
    Conducts unhappy path unit test for drop_null_responses function.

    Checks if KeyError raised for missing column.
    """

    # Create test output
    with pytest.raises(KeyError):
        drop_null_responses(df_in.drop(["GHLTH"]), "GHLTH")
