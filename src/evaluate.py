"""
Module evaluates test set predictions.
"""

import logging

import numpy as np
import botocore
import boto3
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.metrics import mean_squared_error

logger = logging.getLogger(__name__)

def capture_rmse(test_df : pd.DataFrame,
                 true_col : str = "GHLTH",
                 pred_col : str = "predictions",
                 comp_prop : bool = True) -> float:
    """
    Runs root mean squared error evaluation metrics on predictions against true values.

    Args:
        test_df (pandas dataframe) : Dataframe of true responses and prediction responses.
                                     Dataframe must contain true_col and pred_col as columns.
        true_col (str) : Dataframe column name containing true response values.
        pred_col (str) : Dataframe column name containing predicted response values.
        comp_bool (bool) : If True, log-odds predictions and true values will be
                           converted to probabilities before RMSE calculated.

    Returns:
        Pandas dataframe of RMSE
    """

    if len(test_df) == 0:
        logger.error("Empty vector passed as test set.")
        raise ValueError

    try:
        true = test_df[true_col].copy()
        preds = test_df[pred_col].copy()
        if comp_prop: # convert both to probability
            true = np.exp(true) / (1 + np.exp(true))
            preds = np.exp(preds) / (1 + np.exp(preds))
        rmse = mean_squared_error(true, preds, squared=False)
        logger.info("Model RMSE of %f captured.",rmse)
        if rmse > 0.1:
            logger.warning("Model RMSE exceeds 10%.")
    except ValueError as v_err:
        logger.error("There was a datatype mismatch or null value found.")
        raise ValueError from v_err
    except KeyError as k_err:
        logger.error("Test dataframe missing provided columns for prediction or true values.")
        raise KeyError(
            "Test dataframe missing provided columns for prediction or true values.") from k_err
    except Exception as e:
        logger.error("Error occurred while trying evaluate performance.")
        raise Exception("Error occurred while trying evaluate performance.") from e
    else:
        return rmse


def visualize_performance(test_df : pd.DataFrame,
                          save_file_path : str,
                          rmse : bool = True,
                          true_col : str = "GHLTH",
                          pred_col : str = "predictions",
                          comp_prop : bool = True,
                          **kwargs) -> None:
    """
    Creates scatter plot of predictions vs. actual responses.

    Args:
        test_df (pandas dataframe) : Dataframe of true responses and prediction responses.
                                     Dataframe must contain true_col and pred_col as columns.
        save_file_path (str) : Url to save file, such as s3 bucket address.
        rmse (bool) : Adds rmse as text to plot if True.
                      Default to True.
        true_col (str) : Dataframe column name containing true response values.
                         Default to "GHLTH".
        pred_col (str) : Dataframe column name containing predicted response values.
                         Defaults to "prediction".
        comp_bool (bool) : If True, log-odds predictions and true values will be
                           converted to probabilities before RMSE calculated.

    Returns:
        None; saves plot
    """

    if rmse:
        rmse_txt = str(round(capture_rmse(test_df, true_col, pred_col, comp_prop),5))
    else:
        rmse_txt = ""
    true = test_df[true_col].copy()
    preds = test_df[pred_col].copy()
    if comp_prop: # convert both to probability
        true = np.exp(true) / (1 + np.exp(true))
        preds = np.exp(preds) / (1 + np.exp(preds))
    try:
        plt.figure(figsize = (8,8))
        plt.scatter(true,
                    preds,
                    alpha = 0.2,
                    edgecolors="darkblue",
                    **kwargs)
        plt.title("Predicted vs. Actual Responses", fontsize=22)
        plt.xlabel("Actual Responses", fontsize=18)
        plt.ylabel("Predicted Responses", fontsize=18)
        plt.text(0.1, 0.5, f"RMSE: {rmse_txt}", fontsize = 20)
        plt.savefig(save_file_path)
    except KeyError as k_err:
        logger.error("Test dataframe missing provided columns for prediction or true values.")
        raise KeyError(
            "Test dataframe missing provided columns for prediction or true values.") from k_err
    except FileNotFoundError as f_err:
        logger.error("A valid file path and name must be provided.")
        raise FileNotFoundError("A valid file path and name must be provided.") from f_err
    except boto3.exceptions.NoCredentialsError as c_err:  # type: ignore
        logger.error(
            "Please provide credentials AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables."
            )
        raise boto3.exceptions.NoCredentialsError(  # type: ignore
            "Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials") from c_err
    except Exception as e:
        logger.error("Error occurred while trying to generate plot.")
        raise Exception("Error occurred while trying to generate plot.") from e
    else:
        logger.info("Plot successfully saved.")
