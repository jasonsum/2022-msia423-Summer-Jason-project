"""
Tests the drop_invalid_measures function in clean module.
"""

import pytest

import numpy as np
import pandas as pd

from src.clean import drop_invalid_measures

# Define input dataframe
df_in_values = [["Michigan", "Wayne", 26163, 26163543900, 901,
        "{'type': 'Point', 'coordinates': [-83.2493016, 42.38181837]}",
        14.2, 12.1, 10.1],
       ["Ohio", "Summit", 39153, 39153503300, 5606,
        "{'type': 'Point', 'coordinates': [-81.49716013, 41.04049422]}",
        13.1, 11.1, 21.7],
       ["South Carolina", "Orangeburg", 45075, 45075010200, 5097,
        "{'type': 'Point', 'coordinates': [-80.38981016, 33.3179319]}",
        12.2, 30.5, 13.2]]
df_in_index = [0,1,2]
df_in_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
                        "TotalPopulation", "Geolocation", "DENTAL", "GHLTH", "MHLTH"]
df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)

def test_drop_invalid_measures():
    """
    Conducts happy path unit test for drop_invalid_measures function.
    """

    # Define expected output
    df_true = pd.DataFrame(
            [["Michigan", "Wayne", 26163, 26163543900, 901,
        "{'type': 'Point', 'coordinates': [-83.2493016, 42.38181837]}", 12.1],
       ["Ohio", "Summit", 39153, 39153503300, 5606,
        "{'type': 'Point', 'coordinates': [-81.49716013, 41.04049422]}", 11.1],
       ["South Carolina", "Orangeburg", 45075, 45075010200, 5097,
        "{'type': 'Point', 'coordinates': [-80.38981016, 33.3179319]}", 30.5]],
            index = [0,1,2],
            columns = ["StateDesc", 
                       "CountyName", 
                       "CountyFIPS", 
                       "LocationID",
                       "TotalPopulation",
                       "Geolocation", 
                       "GHLTH"])

    # Create test output
    df_test = drop_invalid_measures(df_in, ["DENTAL","MHLTH"])

    # Test equality
    pd.testing.assert_frame_equal(df_true, df_test)

def test_drop_invalid_measures_att_err():
    """
    Conducts unhappy path unit test for drop_invalid_measures function.

    Checks if AttributeError raised for missing column.
    """

    not_a_df = "This is not a dataframe."
    # Create test output
    with pytest.raises(AttributeError):
        drop_invalid_measures(not_a_df, ["DENTAL","MHLTH"])
