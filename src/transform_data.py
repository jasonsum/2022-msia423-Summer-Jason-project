"""
Module imports raw data from s3, conducts data transformations in preparation of modeling, 
and writes clean to s3.
"""

import typing
import logging
import typing

import pandas as pd
import numpy as np
import botocore
from sodapy import Socrata
import sqlalchemy as sql
import sqlalchemy.exc
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base

from src.models import scalerRanges

logger = logging.getLogger(__name__)

def import_file(file_path : str,
                columns : typing.Optional[typing.List[str]] = None,
                sep : str = ',',
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
                    Defaults to ',' for csv.
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
            'Please provide credentials AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables.'
            )
        raise boto3.exceptions.NoCredentialsError(  # type: ignore
            "Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials.") from c_err
    except FileNotFoundError as f_err:
        logger.error("Please provide a valid file location to import data.")
        raise FileExistsError("Please provide a valid file location to import data.") from f_err
    except KeyError as k_err:
        logger.error('The selected columns were not found in the file.')
        raise KeyError('The selected columns were not found in the file.') from k_err
    except ValueError as v_err:
        logger.error('There was a problem reading the file.')
        raise ValueError('There was a problem reading the file.') from v_err
    except Exception as e:
        logger.error("Error occurred while trying to import data from file: %s", e)
        raise Exception from e
    else:
        if 'Data_Value' in places.columns: # Only used for raw data cleaning
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
                                index = ['StateDesc', 'CountyName', 'CountyFIPS',
                                        'LocationID', 'TotalPopulation', 'Geolocation'],
                                columns = 'MeasureId',
                                values = 'Data_Value').reset_index()
    except TypeError as t_err:
        logger.error("Column Data_Value must be numeric.")
        raise TypeError("Column Data_Value must be numeric.") from t_err
    except KeyError as k_err:
        logger.error("Required columns are missing.")
        logger.error("Please confirm columns StateDesc, CountyName, CountyFIPS, LocationID,\
                        TotalPopulation, Geolocation, MeasureId, Data_Value are present.")
        raise KeyError("Provided column not found in dataframe.") from k_err
    else:
        places_pivot = places_pivot.rename_axis(None, axis=1)

    return places_pivot

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
    
    return places_pivot

def create_range(engine : sql.engine.base.Engine,
                 valuename : str,
                 max : float,
                 min : float) -> None:
    """
    Adds min-max values of features for scaling.

    Args:
        engine (sql.engine.base.Engine) : SQL Alchemy engine object.
        valuename (str) : Field name being scaled.
        max (float) : Maximum value of field.
        min (float) : Minimum value of field.

    Returns:
        None

    """

    Base = declarative_base()

    # create a db session
    Session : sqlalchemy.orm.session.sessionmaker = sqlalchemy.orm.sessionmaker(bind=engine)
    session : sqlalchemy.orm.session.Session = Session()

    sc_range = scalerRanges(valuename = valuename,
                            max_value = max,
                            min_value = min)
    session.add(sc_range)
    session.commit()
    logger.info("Scaling range added.")

def add_range(engine_string : str,
              valuename : str,
              max : float,
              min : float) -> None:
    """
    Populates rangeScaler table with field
    min and max value(s).

    Args:
        engine_string (str) : SQL Alchemy database URI path.
        valuename (str) : Field name being scaled.
        max (float) : Maximum value of field.
        min (float) : Minimum value of field.

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
    create_range(engine,
                 valuename,
                 max,
                 min)


def reformat_measures(places_pivot : pd.DataFrame,
                      make_floats : typing.List[str],
                      make_logit : typing.Optional[str]
                      ) -> pd.DataFrame:
    """
    Conducts minor data transformations to measures of places_pivot.

    Transforms integer proportions with floats [0,1] representing decimal proportions.
    Creates a log-odds column to be used in regression of a proportion. 

    Args:
        places_pivot (dataframe) : Pivoted dataframe of PLACES data.
                                   See pivot_places return.
        make_floats (list[str]) : List of PLACES measure names to
                                  reformat to [0,1] proportions.
        make_logit (str) : Field name to transform into log-odds.
                           Should be used to transform response variable into log-odds.

    Returns:
        pandas dataframe: PLACES dataframe with reformatted column measures

    """
    try:
        places_pivot[make_floats] = places_pivot[make_floats] / 100 # Reformat to [0,1]
        # Create logit of response for [0,1] regression
        if make_logit:
            places_pivot[make_logit] = np.log(places_pivot[make_logit] / (1 - places_pivot[make_logit]))
    except KeyError as k_err:
        logger.error("Columns passed for transformation could not be found.")
        raise KeyError("Columns passed for transformation could not be found.") from k_err
    except ValueError as v_err:
        logger.error("Columns passed for transformation must be numeric.")
        raise KeyError("Columns passed for transformation must be numeric.") from v_err

    return places_pivot

def scale_values(engine_string : str,
                 places_pivot : pd.DataFrame,
                 columns : typing.Union[str, typing.List[str]]) -> pd.DataFrame:
    """
    Scales columns using min-max scaling.

    Min-max scales columns to a range of [0,1]. Min and max values are added, with
    column name as reference, to scaler_ranges table.

    Args:
        engine_string (str) : SQL Alchemy database URI path.
        places_pivot (dataframe) : Pivoted dataframe of PLACES data.
                                   See pivot_places return.
        min_max_scale (str) : Field name(s) to scale to [0,1] using min-max scaling.

    Returns:
        pandas dataframe: PLACES dataframe with reformatted column measures

    """
    
    if isinstance(columns, str): # Create iterable of columns if only one passed
        columns = [columns]
    try: 
        for col in columns: 
            min_value = places_pivot[col].min()
            max_value = places_pivot[col].max()
            places_pivot['scaled_' + col] = (places_pivot[col]-min_value) / (max_value - min_value)
            add_range(engine_string,
                      col,
                      min_value,
                      max_value)
    except KeyError as k_err:
        logger.error("Columns passed for transformation could not be found.")
        raise KeyError("Columns passed for transformation could not be found.") from k_err
    except ValueError as v_err:
        logger.error("Columns passed for transformation must be numeric.")
        raise KeyError("Columns passed for transformation must be numeric.") from v_err
    return places_pivot


def one_hot_encode(places_pivot : pd.DataFrame,
                   states_to_regions : typing.Dict[str, str]) -> pd.DataFrame:
    """
    Adds 1/0 dummy columns corresponding to each state's region.

    Adds 'Northwest', 'Southwest', 'Northeast', 'Midwest' binary
    columns to dataframe. 'West' is omitted to avoid perfect multicollinearity.

    Args:
        places_pivot (dataframe) : Pivoted dataframe of PLACES data.
                                   See pivot_places return.
        states_to_regions (dict[str,str]) : Key[str] is state name.
                                            Value[str] is region name.

    Returns:
        pandas dataframe: PLACES dataframe with regions one-hot encoded

    """

    try:
        places_pivot['region'] = places_pivot['StateDesc'].map(states_to_regions)
        if sum(places_pivot['region'].isna()) > 0:
            logger.warning("Unmapped state names exist.")
    except KeyError as k_err:
        logger.error("Columns passed for transformation could not be found.")
        raise KeyError("Columns passed for transformation could not be found.") from k_err
    else:
    # Create 1/0 encoded categorical variables for regions, dropping West
        places_pivot = places_pivot.join(pd.get_dummies(places_pivot['region']).drop('West', axis=1))
    return places_pivot

def prep_data(places_df : pd.DataFrame,
              response : str,
              invalid_measures : typing.List[str],
              make_floats : typing.List[str],
              make_logit : typing.Optional[str]) -> pd.DataFrame:
    """
    Helper function that conducts data transformations of PLACES data.

    Function pivots to create one row per
    county, and performs minor filtering and transformations.

    Args:
        places_df (dataframe) : Dataframe from PLACES csv import.
                                See import_file return.
        response (str) : Column to be used as response variable.
                         Rows with null values in this column will be removed.
        invalid_measures (list[str]) : Measure column names to be dropped.
        make_floats (list) : List of PLACES measure names to
                             reformat to [0,1] proportions.
        make_logit (str) : Field name to transform into log-odds.
                           Should be used to transform response variable into log-odds.

    Returns:
        pandas dataframe: PLACES dataframe with one row per county

    """
    places_pivot = pd.DataFrame()
    try:
        places_pivot = pivot_measures(places_df) # Make one county per row
        places_pivot = drop_null_responses(places_pivot, response)
        places_pivot = drop_invalid_measures(places_pivot, invalid_measures)
        # Make proportions floats and log-transform field(s)
        places_pivot = reformat_measures(places_pivot, make_floats,
                                        make_logit)
    except KeyError as k_err:
        logger.error("Columns passed for transformation could not be found.")
        raise KeyError("Columns passed for transformation could not be found.") from k_err
    except ValueError as v_err:
        logger.error("Columns passed for transformation must be numeric.")
        raise KeyError("Columns passed for transformation must be numeric.") from v_err

    return places_pivot