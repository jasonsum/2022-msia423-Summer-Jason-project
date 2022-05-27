"""
Module conducts predictive model training and provides trained model object
"""

import typing
import logging
import pickle

import boto3
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import sqlalchemy as sql
import sqlalchemy.exc
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base

from src.models import Parameters

logger = logging.getLogger(__name__)


def create_params(engine : sql.engine.base.Engine,
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
    Populates rangeScaler table with field min and max value(s).

    Args:
        engine_string (str) : SQL Alchemy database URI path.
        params (Dict) : {key:value} pairs of {parameter name:coefficient}.
                        Parameter names should correspond to Parameters table required columns.

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
              features : typing.List[str],
              response : str,
              method : typing.Optional[str] = None,
              **kwargs) -> typing.Tuple[typing.Dict, LinearRegression]:

    """
    Trains linear regression on provided data and returns coefficients of linear model.

    Args:
        places_df (dataframe) : Dataframe of PLACES features and response for training.
        method (str, optional) : Name of model to use in training.
                                 The parameter is used for information purposes only.
                                 Function uses out of the box scikit-learn LinearRegression.
        features (list[str]) : Column names in dataframe to be used as feature set.
        response (str) : Column name in dataframe to be used as target variable.
        kwargs (dict) : Additional parameters of sklearn.linear_model.LinearRegression.  

    Returns:
        dict of coefficient name : values
        trained model object

    """
    params = {}
    if not method:
        method=""
    logger.info("%s model training...", method)
    try:
        model = LinearRegression(**kwargs).fit(places_df[features], 
                                            places_df[response])
    except TypeError as t_err:
        logger.error("Params and columns must be valid for sklearn.linear_model.LinearRegression")
        raise TypeError("Params and columns must be valid for sklearn.linear_model.LinearRegression") from t_err
    except KeyError as k_err:
        logger.error("Provided column names could not be found.")
        raise KeyError("Provided column names could not be found.") from k_err
    except ValueError as v_err:
        logger.error("Features should be 2D and target 1D values.")
        raise ValueError("Features should be 2D and target 1D values.") from v_err
    except Exception as e:
        logger.error("Error occurred while trying to instantiate or train model: %s", e)
        raise Exception from e
    else:
        coeffs : np.ndarray = model.coef_
        intercept : np.float64 = model.intercept_ # Set to 0.0 if fit_intercept=False
        feature_nms : typing.List[str] = model.feature_names_in_

        # package all parameters into dict
        # parameter table requires all lowercase
        params = dict(zip([x.lower() for x in feature_nms], coeffs))
        params['intercept'] = intercept
        logger.info("Model coefficients successfully captured.")

    return params, model

def dump_model(trained_model : LinearRegression,
               save_path_name : str) -> None:

    """
    Pickles models and saves as trained model object.

    Args:
        instant_model (regressor object) : Trained model to be saved.
        save_path_name (str) : Path and filename to save model object.
    Returns:
        None; saves model to file
    """

    try:
        with open(save_path_name, "wb") as model_handle:
            pickle.dump(trained_model, model_handle)
    except FileNotFoundError as f_err:
        logger.error("Please provide a valid file path.")
        raise FileNotFoundError("Please provide a valid file path.") from f_err
    except boto3.exceptions.NoCredentialsError as c_err:  # type: ignore
        logger.error(
            'Please provide credentials AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables.'
            )
        raise boto3.exceptions.NoCredentialsError(  # type: ignore
            "Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials") from c_err
    except Exception as e:
        logger.error("Error occurred while trying to save file: %s", e)
        raise e
    else:
        logger.info("Model successfully saved.")

