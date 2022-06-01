"""
Tests the split_data function in train_test_split module.
"""

import pytest

import numpy as np
import pandas as pd

from src.train_test_split import split_data

# Define input dataframe
df_in_values = [[ 0.272     ,  0.254     ,  0.182     ,  0.386     ,  0.737     ,
         0.066     ,  0.1       ,  0.079     ,  0.75      ,  0.85      ,
         0.109     ,  0.256     ,  0.198     ,  0.141     , -0.97442191,
         0.335     ,  0.037     ,  0.349     ,  0.049     ,  0.        ,
         0.        ,  1.        ,  0.        ,  0.15587229],
       [ 0.292     ,  0.182     ,  0.177     ,  0.363     ,  0.684     ,
         0.032     ,  0.096     ,  0.056     ,  0.729     ,  0.826     ,
         0.07      ,  0.251     ,  0.184     ,  0.134     , -0.95939172,
         0.263     ,  0.033     ,  0.394     ,  0.039     ,  0.        ,
         0.        ,  1.        ,  0.        ,  0.05842871],
       [ 0.243     ,  0.165     ,  0.189     ,  0.269     ,  0.651     ,
         0.042     ,  0.106     ,  0.045     ,  0.683     ,  0.83      ,
         0.05      ,  0.148     ,  0.156     ,  0.118     , -1.13087315,
         0.275     ,  0.03      ,  0.308     ,  0.032     ,  0.        ,
         0.        ,  0.        ,  0.        ,  0.1170446 ],
       [ 0.172     ,  0.297     ,  0.171     ,  0.356     ,  0.77      ,
         0.08      ,  0.097     ,  0.073     ,  0.785     ,  0.857     ,
         0.094     ,  0.198     ,  0.197     ,  0.12      , -1.32492541,
         0.361     ,  0.032     ,  0.366     ,  0.038     ,  0.        ,
         0.        ,  1.        ,  0.        ,  0.09214889]]
df_in_index = [15510, 19119, 10311, 13791]
df_in_columns = ["ACCESS2", "ARTHRITIS", "BINGE", "BPHIGH", "BPMED", "CANCER", "CASTHMA",
       "CHD", "CHECKUP", "CHOLSCREEN", "COPD", "CSMOKING", "DEPRESSION",
       "DIABETES", "GHLTH", "HIGHCHOL", "KIDNEY", "OBESITY", "STROKE",
       "Midwest", "Northeast", "South", "Southwest", "scaled_TotalPopulation"]
df_in = pd.DataFrame(df_in_values, index=df_in_index, columns=df_in_columns)

def test_split_data():
    """
    Conducts happy path unit test for split_data function.
    """

    # Define expected output
    df_true = pd.DataFrame(
            [[ 0.172     ,  0.297     ,  0.171     ,  0.356     ,  0.77      ,
         0.08      ,  0.097     ,  0.073     ,  0.785     ,  0.857     ,
         0.094     ,  0.198     ,  0.197     ,  0.12      , -1.32492541,
         0.361     ,  0.032     ,  0.366     ,  0.038     ,  0.        ,
         0.        ,  1.        ,  0.        ,  0.09214889,  1        ],
       [ 0.272     ,  0.254     ,  0.182     ,  0.386     ,  0.737     ,
         0.066     ,  0.1       ,  0.079     ,  0.75      ,  0.85      ,
         0.109     ,  0.256     ,  0.198     ,  0.141     , -0.97442191,
         0.335     ,  0.037     ,  0.349     ,  0.049     ,  0.        ,
         0.        ,  1.        ,  0.        ,  0.15587229,  1        ],
       [ 0.243     ,  0.165     ,  0.189     ,  0.269     ,  0.651     ,
         0.042     ,  0.106     ,  0.045     ,  0.683     ,  0.83      ,
         0.05      ,  0.148     ,  0.156     ,  0.118     , -1.13087315,
         0.275     ,  0.03      ,  0.308     ,  0.032     ,  0.        ,
         0.        ,  0.        ,  0.        ,  0.1170446 ,  1        ],
       [ 0.292     ,  0.182     ,  0.177     ,  0.363     ,  0.684     ,
         0.032     ,  0.096     ,  0.056     ,  0.729     ,  0.826     ,
         0.07      ,  0.251     ,  0.184     ,  0.134     , -0.95939172,
         0.263     ,  0.033     ,  0.394     ,  0.039     ,  0.        ,
         0.        ,  1.        ,  0.        ,  0.05842871,  0        ]],
            index = [13791, 15510, 10311, 19119],
            columns = ["ACCESS2", "ARTHRITIS", "BINGE", "BPHIGH", "BPMED", "CANCER", "CASTHMA",
                       "CHD", "CHECKUP", "CHOLSCREEN", "COPD", "CSMOKING", "DEPRESSION",
                       "DIABETES", "GHLTH", "HIGHCHOL", "KIDNEY", "OBESITY", "STROKE",
                       "Midwest", "Northeast", "South", "Southwest", "scaled_TotalPopulation",
                       "training"])

    # Create test output
    df_test = split_data(df_in, test_size=0.25,random_state=42)

    # Test equality
    pd.testing.assert_frame_equal(df_true, df_test)

def test_split_data_val_err():
    """
    Conducts unhappy path unit test for split_data function.

    Checks if ValueError raised for missing column.
    """

    # Create test output
    with pytest.raises(ValueError):
        split_data(df_in, 
                   test_size=2.0, # test_size must be float between 0 and 1
                   random_state=42)
