"""
Tests the validate_df function in clean module.
"""

import pytest

import numpy as np
import pandas as pd

from src.clean import validate_df

# Define input dataframe
df_in_values = [["Massachusetts", "Hampden", 25013, 25013812500, 7665,
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
df_in_index = [687146, 471561, 1432558]
df_in_columns = ["StateDesc", 
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
df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)

def test_validate_df():
    """
    Conducts happy path unit test for validate_df function.
    """

    # Create test output
    assert validate_df(df_in,
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
        validate_df(df_in,
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
