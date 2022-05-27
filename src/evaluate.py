"""
Module evaluates test set predictions.
"""

import logging

import pandas as pd
from sklearn.metrics import mean_squared_error

logger = logging.getLogger(__name__)

def evaluate_performance(test_df : pd.DataFrame,
                         true_col : str = "GHLTH",
                         pred_col : str = "prediction") -> pd.DataFrame:
    """
    Runs root mean squared error evaluation metrics on predictions against true values.

    Args:
        test_df (pandas dataframe) : Dataframe of true responses and prediction responses.
                                     Dataframe must contain true_col and pred_col as columns.
        true_col (str) : Dataframe column name containing true response values.
        pred_col (str) : Dataframe column name containing predicted response values.

    Returns:
        Pandas dataframe of RMSE
    """

    if len(test_df) == 0:
        logger.error("Empty vector passed as test set.")
        raise ValueError

    try:
        rmse = mean_squared_error(test_df[true_col], 
                                  test_df[pred_col],
                                  squared=False)
    except ValueError as v_err:
        logger.error("There was a datatype mismatch or null value found")
        raise ValueError from v_err
    except KeyError as k_err:
        logger.error("Test dataframe missing provided columns for prediction or true values.")
        raise KeyError(
            "Test dataframe missing provided columns for prediction or true values.") from k_err
    except Exception as e:
        logger.error("Error occurred while trying evaluate performance.")
        raise Exception("Error occurred while trying evaluate performance.") from e
    else:
        return pd.DataFrame({"rmse":[rmse]})


def visualize_performance(test_df : pd.DataFrame,
                          true_col : str = "GHLTH",
                          pred_col : str = "prediction",
                          save_file_path : str) -> None:
    """
    Creates scatter plot of predictions vs. actual responses.

    Args:
        test_df (pandas dataframe) : Dataframe of true responses and prediction responses.
                                     Dataframe must contain true_col and pred_col as columns.
        true_col (str) : Dataframe column name containing true response values.
        pred_col (str) : Dataframe column name containing predicted response values.

    Returns:
        None; saves plot
    """
