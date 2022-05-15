"""
Module retrieves PLACES data from API, uploads to S3, and imports from S3
"""

import logging
import os
import time
import sys
import typing

import requests
import pandas as pd
import boto3
import botocore
from sodapy import Socrata

logger = logging.getLogger(__name__)

def import_places_api(app_token,
                      socrata_username,
                      socrata_password,
                      socrata_dataset_identifier: str = "cwsq-ngmh",
                      attempts: int = 4) -> pd.DataFrame:
    """
    Retrieves CDC PLACES data via Socrata API.

    Function uses CDC's Socrata APIs to import 2019 PLACES data.
    User will be prompted to enter Socrata App Token, username, and password.
    Resulting pandas dataframe contains columns:
    'StateDesc', 'CountyName', 'CountyFIPS', 'LocationID',
    'TotalPopulation', 'Geolocation', 'MeasureId', 'Data_Value',
    'Category', 'Short_Question_Text', 'Measure'.

    Args:
        socrata_dataset_identifier (str) : Keyword phrase corresponding to PLACES schema.
                                           Defaults to "cwsq-ngmh".
        attempts (int) : Number of tries to to attempt API request before termination

    Returns:
        pandas dataframe: PLACES data from API

    """
    data_df = pd.DataFrame() # empty dataframe to capture api data
    wait = 5 # seconds to wait between api call (increases exponentially)
    for i in range(attempts):
        try:
            client = Socrata("chronicdata.cdc.gov",
                             app_token,
                             socrata_username,
                             socrata_password)
            
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
            logger.info("API connection succesful. %i rows of SHAPE data imported", data_df.shape[0])

        except ConnectionError:
            if i + 1 < attempts:
                logger.warning("There was a connection error during attempt %i of %i. "
                               "Waiting %i seconds then trying again.",
                               i + 1, attempts, wait)
                time.sleep(wait)
                wait = wait * 2
            else:
                logger.error(
                    "Exiting. There was a connection error."
                    "The maximum number of attempts (%i) have been made to connect."
                    "Please check your connection then try again.",
                    attempts)
                sys.exit(1)
        except requests.exceptions.HTTPError:
            logger.error("An error has occurred with the socrata_dataset_identifier or query."
                         "Please review the SOCRATA (get) request.")
            sys.exit(1)
        except Exception as e:
            logger.error("Exiting due to error: %s", e)
            sys.exit(1)
    
    return data_df


def upload_to_s3_pandas(input_df: pd.DataFrame,
                        s3path: str,
                        sep: str = ',') -> None:
    """
    Uploads pandas dataframe to s3path.

    Args:
        input_df (pandas dataframe) : Dataframe to be uploaded as csv to s3 bucket.
        s3path (str) : Url of s3 bucket
        sep (str) : Delimeter character.
                    Defaults to ';' to avoid inferring incorrect character.

    Returns:
        pandas dataframe: PLACES data from API

    """

    try:
        input_df.to_csv(s3path, sep=sep)
    except botocore.exceptions.NoCredentialsError:
        logger.error('Please provide AWS credentials via AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables.')
    else:
        logger.info('PLACES data uploaded from to %s', s3path)