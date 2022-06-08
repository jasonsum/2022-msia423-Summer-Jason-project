"""
Tests the functions contained in clean module.
"""

import pytest
import pandas as pd

from src.clean import validate_df, pivot_measures, drop_null_responses, drop_invalid_measures

# Define input dataframe for validate_df
df_validate_values = [["Massachusetts", "Hampden", 25013, 25013812500, 7665,
            "{'type': 'Point', 'coordinates': [-72.70927126, 42.14739821]}",
            "CSMOKING", 18.8, "Health Risk Behaviors", "Current Smoking",
            "Current smoking among adults aged >=18 years"],
        ["Illinois", "Cook", 17031, 17031400300, 1567,
            "{'type': 'Point', 'coordinates': [-87.61316825, 41.79314226]}",
            "DEPRESSION", 18.3, "Health Outcomes", "Depression",
            "Depression among adults aged >=18 years"],
        ["Virginia", "Arlington", 51013, 51013101404, 4255,
            "{'type': 'Point', 'coordinates': [-77.10506054, 38.88292427]}",
            "OBESITY", 19.1, "Health Outcomes", "Obesity",
            "Obesity among adults aged >=18 years"]]
df_validate_index = [687146, 471561, 1432558]
df_validate_columns = ["StateDesc",
                                "CountyName",
                                "CountyFIPS",
                                "LocationID",
                                "TotalPopulation",
                                "Geolocation",
                                "MeasureId",
                                "Data_Value",
                                "Category",
                                "Short_Question_Text",
                                "Measure"]
df_validate_in = pd.DataFrame(df_validate_values,
                              index=df_validate_index,
                              columns=df_validate_columns)

# Define input dataframe for pivot_measures
df_pivot_values = [["Ohio", "Summit", 39153, 39153503300, 5606,
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
df_pivot_index = [1114538, 1255710, 766068]
df_pivot_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
    "TotalPopulation", "Geolocation", "MeasureId", "Data_Value", "Category",
    "Short_Question_Text", "Measure"]
df_pivot_in = pd.DataFrame(df_pivot_values,
                           index=df_pivot_index,
                           columns=df_pivot_columns)

# Define input dataframe for drop_null_responses
df_drop_null_values = [["Michigan", "Wayne", 26163, 26163543900, 901,
        "{'type': 'Point', 'coordinates': [-83.2493016, 42.38181837]}",
        14.2, None, 10.1],
    ["Ohio", "Summit", 39153, 39153503300, 5606,
        "{'type': 'Point', 'coordinates': [-81.49716013, 41.04049422]}",
        13.1, 11.1, 21.7],
    ["South Carolina", "Orangeburg", 45075, 45075010200, 5097,
        "{'type': 'Point', 'coordinates': [-80.38981016, 33.3179319]}",
        12.2, 30.5, 13.2]]
df_drop_null_index = [0,1,2]
df_dro_null_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
                        "TotalPopulation", "Geolocation", "COPD", "GHLTH", "MHLTH"]
df_drop_null_in = pd.DataFrame(df_drop_null_values,
                               index=df_drop_null_index,
                               columns=df_dro_null_columns)

# Define input dataframe for drop_invalid_measures
df_drop_inv_values = [["Michigan", "Wayne", 26163, 26163543900, 901,
        "{'type': 'Point', 'coordinates': [-83.2493016, 42.38181837]}",
        14.2, 12.1, 10.1],
    ["Ohio", "Summit", 39153, 39153503300, 5606,
        "{'type': 'Point', 'coordinates': [-81.49716013, 41.04049422]}",
        13.1, 11.1, 21.7],
    ["South Carolina", "Orangeburg", 45075, 45075010200, 5097,
        "{'type': 'Point', 'coordinates': [-80.38981016, 33.3179319]}",
        12.2, 30.5, 13.2]]
df_drop_inv_index = [0,1,2]
df_drop_inv_columns = ["StateDesc", "CountyName", "CountyFIPS", "LocationID",
                        "TotalPopulation", "Geolocation", "DENTAL", "GHLTH", "MHLTH"]
df_drop_inv_in = pd.DataFrame(df_drop_inv_values, index=df_drop_inv_index, columns=df_drop_inv_columns)
not_a_df = "This is not a dataframe."

# Test validate_df function
def test_validate_df():
    """
    Conducts happy path unit test for validate_df function.
    """
    # Define input dataframe

    # Create test output
    assert validate_df(df_validate_in,
                       cols = {"StateDesc":"object",
                                  "CountyName":"object",
                                  "CountyFIPS":"int",
                                  "LocationID":"int",
                                  "TotalPopulation":"int",
                                  "Geolocation":"object",
                                  "MeasureId":"object",
                                  "Data_Value":"float",
                                  "Category":"object",
                                  "Short_Question_Text":"object",
                                  "Measure":"object"}
                                  ) is None

def test_validate_df_type_err():
    """
    Conducts unhappy path unit test for validate_df function.

    Checks for ValueError if data type mismatch occurs.
    """

    # Create test output
    with pytest.raises(TypeError):
        validate_df(df_validate_in,
                    cols = {"StateDesc":"int", # Should be object
                                    "CountyName":"object",
                                    "CountyFIPS":"int",
                                    "LocationID":"int",
                                    "TotalPopulation":"int",
                                    "Geolocation":"object",
                                    "MeasureId":"object",
                                    "Data_Value":"object",
                                    "Category":"object",
                                    "Short_Question_Text":"object",
                                    "Measure":"object"})

# Test pivot_measures function
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
    df_test = pivot_measures(df_pivot_in)

    # Test equality
    pd.testing.assert_frame_equal(df_true, df_test)

def test_pivot_measures_key_err():
    """
    Conducts unhappy path unit test for pivot_measures function.

    Checks if KeyError raised for missing column.
    """

    # Create test output
    with pytest.raises(KeyError):
        pivot_measures(df_pivot_in.drop(["MeasureId"]))


# Test drop_null_responses function


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
    df_test = drop_null_responses(df_drop_null_in, "GHLTH")

    # Test equality
    pd.testing.assert_frame_equal(df_true, df_test)

def test_drop_null_responses_key_err():
    """
    Conducts unhappy path unit test for drop_null_responses function.

    Checks if KeyError raised for missing column.
    """

    # Create test output
    with pytest.raises(KeyError):
        drop_null_responses(df_drop_null_in.drop(["GHLTH"]), "GHLTH")

# Test drop_invalid_measures function
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
    df_test = drop_invalid_measures(df_drop_inv_in, ["DENTAL","MHLTH"])

    # Test equality
    pd.testing.assert_frame_equal(df_true, df_test)

def test_drop_invalid_measures_att_err():
    """
    Conducts unhappy path unit test for drop_invalid_measures function.

    Checks if AttributeError raised for missing column.
    """

    # Create test output
    with pytest.raises(AttributeError):
        drop_invalid_measures(not_a_df, ["DENTAL","MHLTH"])
