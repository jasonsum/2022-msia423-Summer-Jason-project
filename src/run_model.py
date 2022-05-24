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

from src.models import Parameters

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
        row_cnt = len(places_df)
        logger.info("Number of rows in training data is %i", row_cnt)
        if row_cnt == 0:
            logger.error("An empty dataframe has been passed for training.")
            raise ValueError
    except NameError:
        logger.error("Please confirm a clean dataframe was retrieved.")
    if not set(columns).issubset(places_df.columns):
             logger.error("Some necessary columns are missing."
                          "Please confirm that the following columns are present: %i",
                          columns)
             sys.exit(1)

def create_params(engine: sql.engine.base.Engine,
                  params : typing.Dict):
    """
    Adds model parameter values following model fit.

    Args:
        engine (sql.engine.base.Engine) : SQL Alchemy engine object.
        params (Dict) : Key: value pairs of parameter name: value.
                        Should correspond to Parameters table required columns.

    Returns:
        None

    """

    Base = declarative_base()

    # create a db session
    Session : sqlalchemy.orm.session.sessionmaker = sqlalchemy.orm.sessionmaker(bind=engine)
    session : sqlalchemy.orm.session.Session = Session()

    param_row = Parameters(**params)
    session.add(param_row)
    session.commit()
    logger.info("Model coefficients and intercept added.")


def add_params(engine_string : str,
               params : typing.Dict) -> None:
    """
    Populates rangeScaler table with field
    min and max value(s).

    Args:
        engine_string (str) : SQL Alchemy database URI path.
        params (Dict) : Key: value pairs of parameter name: value.
                        Should correspond to Parameters table required columns.

    Returns:
        None

    """
    engine : sql.engine.base.Engine = sql.create_engine(engine_string)

    # test database connection
    try:
        engine.connect()
    except sqlalchemy.exc.OperationalError as e:
        logger.error("Could not connect to database!")
        logger.debug("Database URI: %s", )
        raise e

    # add range row to table
    create_params(engine,
                  params)

def fit_model(places_df: pd.DataFrame,
              method : str,
              features : typing.List[str],
              response : str,
              **kwargs) -> typing.Dict:

    logger.info("%s model training", method)
    model = LinearRegression(**kwargs).fit(places_df[features], 
                                   places_df[response])
    coeffs : np.ndarray = model.coef_
    intercept : np.float64 = model.intercept_
    feature_nms : typing.List[str] = model.feature_names_in_

    # package all parameters into dict
    # parameter table requires all lowercase
    params = dict(zip([x.lower() for x in feature_nms], coeffs))
    params['intercept'] = intercept
    logger.info("Model fitting parameters captured.")

    return params

