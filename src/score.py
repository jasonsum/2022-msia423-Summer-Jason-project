"""
Module conducts test set predictions using imported trained model object.
"""

import logging
import typing
import pickle

import botocore
import pandas as pd
from sklearn.linear_model import LinearRegression

logger = logging.getLogger(__name__)

def import_model(save_path_name : str) -> LinearRegression:

    """
    Imports trained model object from path.

    Args:
        save_path_name (str) : Path and filename of saved model object.

    Returns:
        Trained LinearRegression model object
    """

    try:
        with open(save_path_name, "rb") as model_handle:
            trained_model : LinearRegression = pickle.load(model_handle)
    except FileNotFoundError as f_err:
        logger.error("A valid file path and name must be provided.")
        raise FileNotFoundError("A valid file path and name must be provided.") from f_err
    except botocore.exceptions.NoCredentialsError as c_err:  # type: ignore
        logger.error(
            "Please provide credentials AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables."
            )
        raise botocore.exceptions.NoCredentialsError(  # type: ignore
            "Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials") from c_err
    except Exception as e:
        logger.error("Error occurred while trying to import file: %s", e)
        raise Exception from e
    else:
        logger.info("Model object imported into memory.")
        return trained_model

def pred_responses(trained_model : LinearRegression,
                   test_df : pd.DataFrame,
                   features : typing.List[str]) -> pd.DataFrame:

    """
    Predicts responses of probabilities of X_test.

    Args:
        trained_model (classifier object) : Trained model to be saved.
        test_df (pandas dataframe) : Feature set to be used in predictions.
        features (list[str]) : Columns to be used as model features.

    Returns:
        Pandas dataframe of test set with prediction column
    """

    if len(test_df) == 0: # Test set could be 0
        logger.error("X_test must be a non-empty 2D dataset.")
        raise ValueError("X_test cannot be of length zero.")
    try:
        test_df["predictions"] = trained_model.predict(test_df[features])
    except ValueError as v_err:
        logger.error("X_test should be 2D of feature values.")
        raise ValueError("X_test should be 2D of feature values.") from v_err
    except KeyError as k_err:
        logger.error("The provided columns could not be found in the dataframe.")
        raise KeyError("The provided columns could not be found in the dataframe,") from k_err

    return test_df
