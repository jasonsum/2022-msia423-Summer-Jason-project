"""
Module splits training and testing datasets
"""

import typing
import logging
import typing
import sys

import pandas as pd
import sklearn.model_selection

logger = logging.getLogger(__name__)

def split_data(places_df: pd.DataFrame,
               test_size : float) -> pd.DataFrame:
    """
    Adds a dummy (1/0) column to datafrme indicating training or testing set.

    New column `training` will have value 1 for training rows and 0 for testing rows.

    Args:
        places_df (dataframe) : Dataframe of features and target.
                                See transform_data.py for transformations.
        test_size (float) : Proportion of data to be used as test set.
                            Value should be be between [0,1).
                            If 0 is provided, `training` will only contain 1.

    Returns:
        pandas dataframe
    """

    combined_df = pd.DataFrame()
    places_df["training"] = 1
    if test_size == 0:
        combined_df = places_df.copy()
        logger.warning("Test_size of 0 selected. All rows marked for training.")
    else:
        try:
            train, test = sklearn.model_selection.train_test_split(places_df,
                                                                test_size=test_size)
            test["training"] = 0
            combined_df = pd.concat([train, test], axis=0)  # type: ignore
        except TypeError:
            logger.error("Value of test_size must be a float.")
        except ValueError:
            logger.error("Value of test_size must be between 0 and 1.")

    return combined_df  # type: ignore