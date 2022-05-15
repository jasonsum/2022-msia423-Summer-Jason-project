"""Configures the subparsers for receiving command line arguments for each
 stage in the model pipeline and orchestrates their execution."""
import argparse
import logging
import logging.config
import os

from config.flaskconfig import SQLALCHEMY_DATABASE_URI
from config.config import SOCRATA_DATASET_IDENTIFIER
from src.retrieve_data import import_places_api, upload_to_s3_pandas
from src.models import create_db

logging.config.fileConfig('config/logging/local.conf')
logger = logging.getLogger('places-health-pipeline')

if __name__ == '__main__':

    # Add parsers for both creating a database and adding songs to it
    parser = argparse.ArgumentParser(
        description="Create database, populate database, and/or train model")
    subparsers = parser.add_subparsers(dest='subparser_name')

    # Sub-parser for creating a database
    sp_create = subparsers.add_parser("create_db",
                                      description="Create database")
    sp_create.add_argument("--engine_string", default=SQLALCHEMY_DATABASE_URI,
                           help="SQLAlchemy connection URI for database")

    # Sub-parser for ingesting data from API and placing in S3
    sp_ingest = subparsers.add_parser("ingest",
                                      description="Retrieve data from API and add to S3")
    # AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY should also be passed as env variables
    sp_ingest.add_argument('--s3path', default='s3://2022-msia423-summer-jason/data/raw/places_raw_data.csv',
                        help="If used, will load data via pandas")

    # Sub-parser for model training pipeline
    sp_create = subparsers.add_parser("train_model",
                                      description="Import from S3, create trained model, populate RDS parameters table")
    sp_create.add_argument("--engine_string", default=SQLALCHEMY_DATABASE_URI,
                           help="SQLAlchemy connection URI for database")

    args = parser.parse_args()
    sp_used = args.subparser_name
    if sp_used == 'create_db':
        engine_string = os.getenv("SQLALCHEMY_DATABASE_URI")
        if engine_string is None:
            raise RuntimeError("SQLALCHEMY_DATABASE_URI environment variable not set; exiting")
        create_db(engine_string)
        # engine_string = "mysql+pymysql://user:password@host:3306/msia423_db"

    elif sp_used == 'ingest':
        socrata_token = os.getenv("SOCRATA_TOKEN")
        socrata_username = os.getenv("SOCRATA_USERNAME")
        socrata_password = os.getenv("SOCRATA_PASSWORD")
        raw_data = import_places_api(socrata_token,
                                     socrata_username,
                                     socrata_password,
                                     socrata_dataset_identifier=SOCRATA_DATASET_IDENTIFIER)
        upload_to_s3_pandas(raw_data,
                            args.s3path)
    else:
        parser.print_help()
