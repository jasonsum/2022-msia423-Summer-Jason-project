"""
Module imports PLACES data and conducts minor transformations in preparation of modeling
"""

import typing
import getpass
import logging

import pandas as pd
import numpy as np
from sodapy import Socrata

logger = logging.getLogger(__name__) 

places_file_path : str = '../data/places_raw_data.csv'

measures : typing.List[str] = ['ACCESS2', 'ARTHRITIS', 'BINGE','BPHIGH', 'BPMED',
                                'CANCER', 'CASTHMA', 'CHD', 'CHECKUP', 'CHOLSCREEN',
                                'COPD', 'CSMOKING', 'DEPRESSION', 'DIABETES',
                                'GHLTH', 'HIGHCHOL', 'KIDNEY', 'LPA', 'MHLTH',
                                'OBESITY', 'PHLTH', 'STROKE']

# sourced from https://gist.github.com/sfirrin/fd01d87f022d80e98c37a045c14109fe
states_region_mapping : typing.Dict[str, str] = {
    'Washington': 'West', 'Oregon': 'West', 'California': 'West', 'Nevada': 'West',
    'Idaho': 'West', 'Montana': 'West', 'Wyoming': 'West', 'Utah': 'West',
    'Colorado': 'West', 'Alaska': 'West', 'Hawaii': 'West', 'Maine': 'Northeast',
    'Vermont': 'Northeast', 'New York': 'Northeast', 'New Hampshire': 'Northeast',
    'Massachusetts': 'Northeast', 'Rhode Island': 'Northeast', 'Connecticut': 'Northeast',
    'New Jersey': 'Northeast', 'Pennsylvania': 'Northeast', 'North Dakota': 'Midwest',
    'South Dakota': 'Midwest', 'Nebraska': 'Midwest', 'Kansas': 'Midwest',
    'Minnesota': 'Midwest', 'Iowa': 'Midwest', 'Missouri': 'Midwest', 'Wisconsin': 'Midwest',
    'Illinois': 'Midwest', 'Michigan': 'Midwest', 'Indiana': 'Midwest', 'Ohio': 'Midwest',
    'West Virginia': 'South', 'District of Columbia': 'South', 'Maryland': 'South',
    'Virginia': 'South', 'Kentucky': 'South', 'Tennessee': 'South', 'North Carolina': 'South',
    'Mississippi': 'South', 'Arkansas': 'South', 'Louisiana': 'South', 'Alabama': 'South',
    'Georgia': 'South', 'South Carolina': 'South', 'Florida': 'South', 'Delaware': 'South',
    'Arizona': 'Southwest', 'New Mexico': 'Southwest', 'Oklahoma': 'Southwest',
    'Texas': 'Southwest'}

def import_places_api() -> pd.DataFrame:
    """
    Retrieves CDC PLACES data via Socrata API.

    Function uses CDC's Socrata APIs to import 2019 PLACES data.
    User will be prompted to enter Socrata App Token, username, and password.
    Resulting pandas dataframe contains columns:
    'StateDesc', 'CountyName', 'CountyFIPS', 'LocationID',
    'TotalPopulation', 'Geolocation', 'MeasureId', 'Data_Value',
    'Category', 'Short_Question_Text', 'Measure'.

    Args:
        None

    Returns:
        pandas dataframe: PLACES data from API

    """
    client = Socrata("chronicdata.cdc.gov",
                     getpass.getpass(prompt = 'App Token: '),
                     getpass.getpass(prompt = "username: "),
                     getpass.getpass(prompt = "password: "))
    socrata_dataset_identifier : str = "cwsq-ngmh"
    socrata_query : str = """
    select 
        StateDesc,
        CountyName,
        CountyFIPS,
        LocationID,
        TotalPopulation,
        Geolocation,
        MeasureId,
        Data_Value,
        Category,
        Short_Question_Text,
        Measure
    where
        year = '2019'
        and data_value is not null
    limit 3000000
    """
    # API suggestions sourced from https://dev.socrata.com/foundry/chronicdata.cdc.gov/cwsq-ngmh
    data : list[list[str]] = client.get(socrata_dataset_identifier,
                                        query = socrata_query,
                                        exclude_system_fields = True)
    data_df : pd.DataFrame = pd.DataFrame.from_records(data)
    return data_df

def import_places(use_api : bool = True,
                  file_path : typing.Optional[str] = None) -> pd.DataFrame:
    """
    Imports CDC PLACES data from API or csv.

    Function imports and generates pandas dataframe with columns
    'StateDesc', 'CountyName', 'CountyFIPS', 'LocationID',
    'TotalPopulation', 'Geolocation', 'MeasureId', 'Data_Value',
    'Category', 'Short_Question_Text', 'Measure'. If use_API = True,
    a dataframe will be generated via API and user will be prompted
    for credentials.

    Args:
        use_api (bool) : Uses Socrata API to generate data if True.
                         Defaults to True.
        file_path (str) : Relative or absolute path to PLACES csv file.
                          Defaults to None

    Returns:
        pandas dataframe: PLACES data

    """
    if use_api:
        places : pd.DataFrame = import_places_api()
    else:
        places : pd.DataFrame = pd.read_csv(file_path,
                                            usecols = ['StateDesc',
                                                    'CountyName',
                                                    'CountyFIPS',
                                                    'LocationID',
                                                    'TotalPopulation',
                                                    'Geolocation',
                                                    'MeasureId',
                                                    'Data_Value',
                                                    'Category',
                                                    'Short_Question_Text',
                                                    'Measure'])  # type: ignore
    # Drop row with NA data_value
    places = places.drop(places.loc[places.Data_Value.isna()].index, axis=0)
    logger.info("Imported places row count: {}".format(places.shape[0]))

    return places


def pivot_measures (places_df : pd.DataFrame) -> pd.DataFrame:
    """
    Transposes dataframe of PLACES data from row-wise measures to column-wise.

    Function produces a dataframe of one row per county with columns containing
    the value of measures. Some known measures are removed due to lack of data.
    Any county with a missing 'GHTLH' value is removed.

    Args:
        places_df (dataframe) : Dataframe from PLACES csv import.
                                See import_places return.

    Returns:
        pandas dataframe: PLACES dataframe pivoted to one row per county

    """
    places_pivot = pd.pivot(places_df,
                           index = ['StateDesc', 'CountyName', 'CountyFIPS',
                                   'LocationID', 'TotalPopulation', 'Geolocation'],
                           columns = 'MeasureId',
                           values = 'Data_Value').reset_index()
    places_pivot = places_pivot.rename_axis(None, axis=1)

    places_pivot.drop(places_pivot.loc[places_pivot.GHLTH.isnull()].index, axis=0, inplace=True)
    null_rows = places_pivot.loc[places_pivot.GHLTH.isnull()].shape[0]
    logger.info("Number of null GHLTH records dropped: {}".format(null_rows))

    places_pivot.drop(['TEETHLOST', 'SLEEP', 'MAMMOUSE', 'DENTAL', 'COREW',
                       'COREM', 'COLON_SCREEN', 'CERVICAL'],
                      axis=1,
                      inplace=True)

    places_pivot.reset_index(drop=True, inplace=True)
    return places_pivot


def reformat_measures(places_pivot : pd.DataFrame,
                      measures_names : typing.List[str]) -> pd.DataFrame:
    """
    Conducts minor data transformations to measures and population columns of places_pivot.

    Replaces whole number measure data values, which represent proportions, to float [0,1].
    Adds the log-odds of 'GHLTH' as a column to be used as future response variable.
    Creates a [0,1] min-max scaled version of 'TotalPopulation'.

    Args:
        places_pivot (dataframe) : Pivoted dataframe of PLACES data.
                                   See pivot_places return.
        measure_names (list) : List of PLACES measure names to reformat to [0,1].

    Returns:
        pandas dataframe: PLACES dataframe with one row per county

    """
    places_pivot[measures_names] = places_pivot[measures_names] / 100 # Reformat to [0,1]
    # Create logit of GHTLH for logistic response variable
    places_pivot['logit_GHLTH'] = np.log(places_pivot['GHLTH'] / (1 - places_pivot['GHLTH']))

    # Standardize population to [0,1]
    places_pivot['scaled_TotalPopulation'] = (places_pivot['TotalPopulation'] \
                                              - places_pivot['TotalPopulation'].min())  \
                                           / (places_pivot['TotalPopulation'].max() \
                                              - places_pivot['TotalPopulation'].min())
    return places_pivot


def add_regions(places_pivot : pd.DataFrame,
                states_to_regions : typing.Dict[str, str]) -> pd.DataFrame:
    """
    Adds 1/0 dummy columns corresponding to each state's region.

    Adds 'Northwest', 'Southwest', 'Northeast', 'Midwest' binary
    columns to dataframe. 'West' is omitted to avoid perfect multicollinearity.

    Args:
        places_pivot (dataframe) : Pivoted dataframe of PLACES data.
                                   See pivot_places return.
        states_to_regions (dict) : Key[str] is state name.
                                   Value[str] is region name.

    Returns:
        pandas dataframe: PLACES dataframe with one row per county

    """

    places_pivot['region'] = places_pivot['StateDesc'].map(states_to_regions)
    if sum(places_pivot['region'].isna()) > 0:
        logger.warning("Unmapped state names exist.")

    # Create 1/0 encoded categorical variables for regions, dropping West
    places_pivot = places_pivot.join(pd.get_dummies(places_pivot['region']).drop('West', axis=1))
    return places_pivot

def retrieve_data(states_to_regions : typing.Dict[str, str],
                  use_api : bool = True,
                  file_path : typing.Optional[str] = None) -> pd.DataFrame:
    """
    Helper function that imports PLACES data and performs minor transformations.

    Function imports PLACES data from file_path, pivots to create one row per
    county, and performs minor filtering and transformations.

    Args:
        file_path (str) : Relative or absolute path to PLACES csv file.
        states_to_regions (dict) : Key[str] is state name.
                                   Value[str] is region name.

    Returns:
        pandas dataframe: PLACES dataframe with one row per county

    """
    places_df = import_places(use_api, file_path)
    places_pivot = pivot_measures(places_df)
    places_pivot = reformat_measures(places_pivot, measures)
    places_pivot = add_regions(places_pivot,
                               states_to_regions)
    return places_pivot