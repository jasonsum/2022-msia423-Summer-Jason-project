"""
Module uses clean data, and conducts minor featurization in preparation of modeling.
"""

import typing
import logging

import pandas as pd
import numpy as np
import sqlalchemy as sql
import sqlalchemy.exc
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base

from src.models import scalerRanges

logger = logging.getLogger(__name__)

def create_range(engine : sql.engine.base.Engine,
                 valuename : str,
                 max_val : float,
                 min_val : float) -> None:
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
                            max_value = max_val,
                            min_value = min_val)
    session.add(sc_range)
    session.commit()
    logger.info("Scaling range added to database.")

def add_range(engine_string : str,
              valuename : str,
              max_val : float,
              min_val : float) -> None:
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
                 max_val,
                 min_val)

def scale_values(engine_string : typing.Union[str, None],
                 places_pivot : pd.DataFrame,
                 columns : typing.Union[str, typing.List[str]]) -> pd.DataFrame:
    """
    Scales columns using min-max scaling.

    Min-max scales columns to a range of [0,1]. Min and max values are added, with
    column name as reference, to scaler_ranges table.

    Args:
        engine_string (str, Optional) : SQL Alchemy database URI path.
                                        If no engine_string is passed, 
                                        nothing will be captured in database.
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
            places_pivot["scaled_" + col] = (places_pivot[col]-min_value) / (max_value - min_value)
            if engine_string is not None: # Don't write to DB is no engine string passed
                add_range(engine_string,
                          col,
                          min_value,
                          max_value)
            else:
                logger.warning("Scaling params not recorded in database.")
    except KeyError as k_err:
        logger.error("Columns passed for transformation could not be found.")
        raise KeyError("Columns passed for transformation could not be found.") from k_err
    except TypeError as v_err:
        logger.error("Columns passed for transformation must be numeric.")
        raise TypeError("Columns passed for transformation must be numeric.") from v_err
    return places_pivot

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
    except TypeError as v_err:
        logger.error("Columns passed for transformation must be numeric.")
        raise TypeError("Columns passed for transformation must be numeric.") from v_err

    return places_pivot

def one_hot_encode(places_pivot : pd.DataFrame,
                   states_to_regions : typing.Dict[str, str]) -> pd.DataFrame:
    """
    Adds 1/0 dummy columns corresponding to each state"s region.

    Adds "Northwest", "Southwest", "Northeast", "Midwest" binary
    columns to dataframe. "West" is omitted to avoid perfect multicollinearity.

    Args:
        places_pivot (dataframe) : Pivoted dataframe of PLACES data.
                                   See pivot_places return.
        states_to_regions (dict[str,str]) : Key[str] is state name.
                                            Value[str] is region name.

    Returns:
        pandas dataframe: PLACES dataframe with regions one-hot encoded

    """

    try:
        places_pivot["region"] = places_pivot["StateDesc"].map(states_to_regions)
        if sum(places_pivot["region"].isna()) > 0:
            logger.warning("Unmapped state names exist.")
    except KeyError as k_err:
        logger.error("Column(s) region or StateDesc are missing from dataframe.")
        raise KeyError("Column(s) region or StateDesc are missing from dataframe.") from k_err
    else:
    # Create 1/0 encoded categorical variables for regions, dropping West
        places_pivot = places_pivot.join(pd.get_dummies(places_pivot["region"], dtype=int).drop("West", axis=1))
    return places_pivot
