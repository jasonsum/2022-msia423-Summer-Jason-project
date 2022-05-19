"""
Module conducts predictive model training and provides trained model object
"""

import typing
import logging
import typing
import sys

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import sqlalchemy as sql
import sqlalchemy.exc
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base

logger = logging.getLogger(__name__)

def validate_clean(places_df: pd.DataFrame,
                   columns : typing.List[str]) -> None:
    """
    Performs validation of clean PLACES data imported from S3.

    Args:
        places_df (dataframe) : Dataframe of cleaned PLACES csv import.
                                See transform_data.py for transformations.
        columns (list[str]) : Required column names to be checked for presence.

    Returns:
        None
    """

    try:
        logger.info("Number of rows in training data is %i", len(places_df))
    except NameError:
        logger.error("Please confirm a clean dataframe was retrieved.")
    if not set(columns).issubset(places_df.columns):
             logger.error("Some necessary columns are missing."
                          "Please confirm that the following columns are present: %i",
                          columns)

def train_model(places_df: pd.DataFrame,
                method : str,
                features : typing.List[str],
                response : str,
                engine : typing.Optional[sql.engine.base.Engine],
                **kwargs) -> typing.Tuple:
    
    logger.info("%s model training", method)
    model = LinearRegression(**kwargs).fit(places_df[features], 
                                           places_df[response])
    coeffs : np.ndarray = model.coef_
    intercept : np.float64 = model.intercept_
    logger.info(coeffs)
    logger.info(intercept)
    return (coeffs, intercept)
