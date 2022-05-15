"""
Module conducts predictive model training and provides trained model object
"""

import typing
import logging
import typing
import sys

import pandas as pd
from sklearn.linear_model import LinearRegression


def divide_x_y(training_df : pd.DataFrame,
               response : str,
               feature_list : typing.List[str]) -> typing.Tuple[pd.DataFrame, pd.Series]:

    X = training_df[feature_list]
    y = training_df[response]

    return (X, y)

def instantiate_linreg(**kwargs) -> LinearRegression:
    model = LinearRegression(**kwargs)

    return model