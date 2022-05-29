"""
Module imports raw data, conducts minor data transformations and cleaning
in preparation of featurization and modeling.
"""

import typing
import logging
import typing

import pandas as pd
import numpy as np
import boto3

from src.models import scalerRanges

logger = logging.getLogger(__name__)

def import_file(file_path : str,
                columns : typing.Optional[typing.List[str]] = None,
                sep : str = ",",
                **kwargs) -> pd.DataFrame:
    """
    Imports CDC PLACES data from provided location.

    Function imports csv data and generates
    pandas dataframe from entire csv.
    Pandas dataframe has columns passed through columns parameter.

    Args:
        s3path (str) : Url of s3 bucket or location
        columns (list[str], Optional) : Columns of dataframe to include.
                                        Defaults to None.
        sep (str) : Delimeter character.
                    Defaults to "," for csv.
        kwargs (dict) : Additional parameters of pandas.read_csv.

    Returns:
        pandas dataframe: PLACES data

    """

    places = pd.DataFrame()
    try:
        places : pd.DataFrame = pd.read_csv(file_path,
                                            usecols = columns, #type:ignore
                                            sep=sep,
                                            **kwargs)
    except boto3.exceptions.NoCredentialsError as c_err:  # type: ignore
        logger.error(
            "Please provide credentials AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables."
            )
        raise boto3.exceptions.NoCredentialsError(  # type: ignore
            "Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials.") from c_err
    except FileNotFoundError as f_err:
        logger.error("Please provide a valid file location to import data.")
        raise FileNotFoundError("Please provide a valid file location to import data.") from f_err
    except KeyError as k_err:
        logger.error("The selected columns were not found in the file.")
        raise KeyError("The selected columns were not found in the file.") from k_err
    except ValueError as v_err:
        logger.error("There was a problem reading the file.")
        raise ValueError("There was a problem reading the file.") from v_err
    except Exception as e:
        logger.error("Error occurred while trying to import data from file: %s", e)
        raise Exception from e
    else:
        if "Data_Value" in places.columns: # Only used for raw data cleaning
            # Drop row with null data_value
            places = places.drop(places.loc[places.Data_Value.isna()].index, axis=0)
        logger.info("Data file successfully imported, valid row count is %i.", places.shape[0])

    return places

def validate_df(df : pd.DataFrame,
                cols : typing.Dict[str,str]) -> None:
    """
    Valides that dataframe has required columns and that columns are of
    the provided data types.

    Function will also ensure passed dataframe has length greater than 0.

    Args:
        df (pandas dataframe) : Dataframe to validate.
        cols (dict[str:str]) : Dictionary with key of column name and
                               value of column type.

    Returns:
        None
    """

    if len(df) == 0 or sum(df.duplicated())>0: # Cannot be 0 length or have duplicate rows
        logger.error("The dataframe has no records or duplicate records.")
        raise ValueError("The dataframe has no records or duplicate records.")

    for col, dtype in cols.items():
        try:
            if not df[col].dtype == dtype: # Column doesn't match provided data types
                logger.error("Column %s does not match data type %s.",col, dtype)
                raise TypeError("A column data type mismatch has occurred.")
            if sum(df[col].isna())>0:
                logger.error("Column %s contains null values.", col)
                raise ValueError("A column has null values.")
        except KeyError as missing_col: # Missing column
            logger.error("Required column %s type not present in dataframe.", col)
            raise KeyError ("Please ensure provided columns are present.") from missing_col

def pivot_measures (places_df : pd.DataFrame) -> pd.DataFrame:
    """
    Transposes dataframe of PLACES data from row-wise measures to column-wise.

    Function produces a dataframe of one row per county with columns containing
    the value of measures. 

    Args:
        places_df (dataframe) : Dataframe from PLACES csv import.
                                See import_places return.

    Returns:
        pandas dataframe: PLACES dataframe pivoted to one row per county

    """
    
    places_pivot = pd.DataFrame()
    try:
        places_pivot = pd.pivot(places_df,
                                index = ["StateDesc", "CountyName", "CountyFIPS",
                                        "LocationID", "TotalPopulation", "Geolocation"],
                                columns = "MeasureId",
                                values = "Data_Value").reset_index()
    except TypeError as t_err:
        logger.error("Column Data_Value must be numeric.")
        raise TypeError("Column Data_Value must be numeric.") from t_err
    except AttributeError as a_err:
        logger.error("First argument must be a dataframe.")
        raise AttributeError("First argument must be a dataframe.") from a_err
    except KeyError as k_err:
        logger.error("Required columns are missing.")
        logger.error("Please confirm columns StateDesc, CountyName, CountyFIPS, LocationID,\
                        TotalPopulation, Geolocation, MeasureId, Data_Value are present.")
        raise KeyError("Provided column not found in dataframe.") from k_err
    else:
        places_pivot = places_pivot.rename_axis(None, axis=1)

    return places_pivot  # type: ignore

def drop_null_responses(places_pivot : pd.DataFrame,
                        response : str) -> pd.DataFrame:
    """
    Removes rows that have null response column values.

    Args:
        places_pivot (dataframe) : PLACES dataframe pivoted to one row per county
                                   See pivot_measures return.
        response (str) : Column to be used as response variable.
                         Rows with null values in this column will be removed.

    Returns:
        pandas dataframe: pivoted PLACES dataframe without null response rows

    """

    initial_count = places_pivot.shape[0]
    try:
        places_pivot.dropna(subset = [response],
                            inplace=True,
                            axis=0)
        new_count = places_pivot.shape[0]

        places_pivot.drop(places_pivot.loc[places_pivot[response].isnull()].index, axis=0, inplace=True)
    except KeyError as k_err:
        logger.error("Column passed as response could not be found.")
        raise KeyError("Provided column not found in dataframe.") from k_err
    except AttributeError as a_err:
        logger.error("First argument must be a dataframe.")
        raise AttributeError("First argument must be a dataframe.") from a_err
    else:
        logger.info("Number of null response records dropped: %i",initial_count - new_count)

    return places_pivot


def drop_invalid_measures(places_pivot : pd.DataFrame,
                          invalid_measures : typing.List[str]) -> pd.DataFrame:

    """
    Removes invalid PLACES measurement columns.

    Some measured are not consistently obtained during the CDC survey. 
    Others introduce multicollinearity among predictors.

    Args:
        places_pivot (dataframe) : PLACES dataframe pivoted to one row per county
                                   See pivot_measures return.
        invalid_measures (list[str]) : Measure column names to be dropped.

    Returns:
        pandas dataframe: pivoted PLACES dataframe without subset of measures

    """
    try:
        places_pivot.drop(invalid_measures,
                          axis=1,
                          inplace=True,
                          errors="ignore")

        places_pivot.reset_index(drop=True, inplace=True)
    except KeyError as k_err:
        logger.error("Columns passed as invalid could not be found.")
        raise KeyError("Provided column not found in dataframe.") from k_err
    except AttributeError as a_err:
        logger.error("First argument must be a dataframe.")
        raise AttributeError("First argument must be a dataframe.") from a_err
    
    return places_pivot

def prep_data(places_df : pd.DataFrame,
              response : str,
              invalid_measures : typing.List[str]) -> pd.DataFrame:
    """
    Helper function that conducts data cleaning of PLACES data.

    Function pivots to create one row per
    county, and removes null resopnses and invalid measures.

    Args:
        places_df (dataframe) : Dataframe from PLACES csv import.
                                See import_file return.
        response (str) : Column to be used as response variable.
                         Rows with null values in this column will be removed.
        invalid_measures (list[str]) : Measure column names to be dropped.

    Returns:
        pandas dataframe: PLACES dataframe with one row per county

    """
    places_pivot = pd.DataFrame()
    try:
        places_pivot = pivot_measures(places_df) # Make one county per row
        places_pivot = drop_null_responses(places_pivot, response)
        places_pivot = drop_invalid_measures(places_pivot, invalid_measures)
    except KeyError as k_err:
        logger.error("Columns passed for transformation could not be found.")
        raise KeyError("Columns passed for transformation could not be found.") from k_err
    except TypeError as v_err:
        logger.error("Column Data_Value must be numeric.")
        raise TypeError("Column Data_Value must be numeric.") from v_err

    return places_pivot