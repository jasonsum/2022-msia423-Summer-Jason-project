"""
Module retrieves PLACES data from API, uploads to S3, and imports from S3.
"""

import logging
import time

import requests
import pandas as pd
import boto3
from sodapy import Socrata

logger = logging.getLogger(__name__)

def import_places_api(url : str,
                      app_token : str,
                      socrata_username : str,
                      socrata_password : str,
                      dataset_identifier : str = "cwsq-ngmh",
                      attempts : int = 4) -> pd.DataFrame:
    """
    Retrieves CDC PLACES data via Socrata API.

    Function uses CDC's Socrata APIs to import 2019 PLACES data.
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
            client = Socrata(url,
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
            logger.info("Retrieving data...could take a few minutes...")
            data : list[list[str]] = client.get(dataset_identifier,
                                                query = socrata_query,
                                                exclude_system_fields = True)
            data_df : pd.DataFrame = pd.DataFrame.from_records(data)
            logger.info("API connection successful. %i rows of SHAPE data imported.", data_df.shape[0])

        except ConnectionError as c_err:
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
                raise requests.exceptions.ConnectionError("There was a connection error.") from c_err
        except requests.exceptions.HTTPError as h_err:
            logger.error("An error has occurred with the socrata_dataset_identifier or query."
                         "Please review the SOCRATA (get) request.")
            raise requests.exceptions.HTTPError from h_err
        except Exception as e:
            logger.error("Exiting due to error: %s", e)
            raise Exception from e
        else:
            break
    
    return data_df


def upload_file(input_df : pd.DataFrame,
                save_file_path : str,
                sep : str = ',') -> None:
    """
    Uploads pandas dataframe to file path.

    Args:
        input_df (pandas dataframe) : Dataframe to be uploaded as csv.
        save_file_path (str) : Url to save file, such as s3 bucket address.
        sep (str) : Delimeter character.
                    Defaults to ','.

    Returns:
        None; uploads file to location

    """

    try:
        input_df.to_csv(save_file_path, sep=sep)
    except boto3.exceptions.NoCredentialsError as c_err:  # type: ignore
        logger.error(
            'Please provide credentials AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables.'
            )
        raise boto3.exceptions.NoCredentialsError(  # type: ignore
            "Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials") from c_err
    except FileNotFoundError as f_err:
        logger.error("Please provide a valid file location to persist data.")
        raise FileExistsError("Please provide a valid file location to persist data.") from f_err
    except Exception as e:
            logger.error("Exiting due to error: %s", e)
            raise Exception from e
    else:
        logger.info('PLACES data uploaded to %s', save_file_path)