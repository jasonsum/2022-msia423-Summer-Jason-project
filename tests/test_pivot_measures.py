"""
Tests the pivot_measures function in clean module.
"""

import pytest

import numpy as np
import pandas as pd

from src.clean import pivot_measures

# Define input dataframe
df_in_values = [["Ohio", "Summit", 39153, 39153503300, 5606,
        "{'type': 'Point', 'coordinates': [-81.49716013, 41.04049422]}",
        "MHLTH", 21.7, "Health Status", "Mental Health",
        "Mental health not good for >=14 days among adults aged >=18 years"],
       ["South Carolina", "Orangeburg", 45075, 45075010200, 5097,
        "{'type': 'Point', 'coordinates': [-80.38981016, 33.3179319]}",
        "GHLTH", 30.5, "Health Status", "General Health",
        "Fair or poor self-rated health status among adults aged >=18 years"],
       ["Michigan", "Wayne", 26163, 26163543900, 901,
        "{'type': 'Point', 'coordinates': [-83.2493016, 42.38181837]}",
        "COPD", 14.2, "Health Outcomes", "COPD",
        "Chronic obstructive pulmonary disease among adults aged >=18 years"]]
df_in_index = [1114538, 1255710, 766068]
df_in_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
       "TotalPopulation", "Geolocation", "MeasureId", "Data_Value", "Category",
       "Short_Question_Text", "Measure"]
df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)

def test_pivot_measures():
    """
    Conducts happy path unit test for pivot_measures function.
    """

    # Define expected output
    df_true = pd.DataFrame(
            [["Michigan", "Wayne", 26163, 26163543900, 901,
        "{'type': 'Point', 'coordinates': [-83.2493016, 42.38181837]}",
        14.2, None, None],
       ["Ohio", "Summit", 39153, 39153503300, 5606,
        "{'type': 'Point', 'coordinates': [-81.49716013, 41.04049422]}",
        None, None, 21.7],
       ["South Carolina", "Orangeburg", 45075, 45075010200, 5097,
        "{'type': 'Point', 'coordinates': [-80.38981016, 33.3179319]}",
        None, 30.5, None]],
            index = [0,1,2],
            columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
                        "TotalPopulation", "Geolocation", "COPD", "GHLTH", "MHLTH"])

    # Create test output
    df_test = pivot_measures(df_in)

    # Test equality
    pd.testing.assert_frame_equal(df_true, df_test)

def test_pivot_measures_key_err():
    """
    Conducts unhappy path unit test for pivot_measures function.

    Checks if KeyError raised for missing column.
    """

    # Create test output
    with pytest.raises(KeyError):
        pivot_measures(df_in.drop(["MeasureId"]))


