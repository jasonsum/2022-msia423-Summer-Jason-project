"""Configures the subparsers for receiving command line arguments for each
 stage in the model pipeline and orchestrates their execution."""
import argparse
import logging
import logging.config
import os
import yaml
import sys

from config.config import SOCRATA_DATASET_IDENTIFIER
from config.config import SQLALCHEMY_DATABASE_URI
from config.config import SOCRATA_TOKEN, SOCRATA_USERNAME, SOCRATA_PASSWORD
from src.retrieve_data import import_places_api, upload_to_s3_pandas
from src.models import create_db
from src.add_definitions import add_references
from src.transform_data import import_from_s3, prep_data, scale_values, one_hot_encode
from src.run_model import validate_clean, train_model
from data.reference.state_region_mapping import states_region_mapping

logging.config.fileConfig('config/logging/local.conf')
logger = logging.getLogger('places-health-pipeline')

if __name__ == '__main__':

    with open('config/model-config.yaml', 'r') as f:
        model_cfg = yaml.load(f, Loader = yaml.FullLoader)

    # Add parsers for both creating a database and adding songs to it
    parser = argparse.ArgumentParser(
        description="Create database, populate database, and/or train model")
    subparsers = parser.add_subparsers(dest='subparser_name')

    # Sub-parser for creating a database
    sp_create = subparsers.add_parser("create_db",
                                      description="Create database")
    sp_create.add_argument("--engine_string", default=SQLALCHEMY_DATABASE_URI,
                           help="SQLAlchemy connection URI for database")
    sp_create.add_argument("--definitions", action='store_true', default=False,
                           help="Add definitions to Measures table")
    
    # Sub-parser for adding measure definitions
    sp_create = subparsers.add_parser("define",
                                      description="Add measure definitions")
    sp_create.add_argument("--engine_string", default=SQLALCHEMY_DATABASE_URI,
                           help="SQLAlchemy connection URI for database")

    # Sub-parser for ingesting data from API and placing in S3
    sp_ingest = subparsers.add_parser("ingest",
                                      description="Retrieve data from API and add to S3")
    sp_ingest.add_argument('--s3path', default='s3://2022-msia423-summer-jason/data/raw/places_raw_data.csv',
                        help="If used, will load data via pandas")
    
    # Sub-parser for ingesting raw data from S3 and placing clean data in S3
    sp_clean = subparsers.add_parser("clean",
                                      description="Create clean data from raw data")
    sp_clean.add_argument('--s3path', default='s3://2022-msia423-summer-jason/data/raw/places_raw_data.csv',
                        help="If used, will load data via pandas")
    sp_clean.add_argument('--s3dest', default='s3://2022-msia423-summer-jason/data/clean/places_data_clean.csv',
                        help="If used, will load data via pandas")
    sp_clean.add_argument("--engine_string", default=SQLALCHEMY_DATABASE_URI,
                           help="SQLAlchemy connection URI for database")
    

    # Sub-parser for model training pipeline
    sp_train = subparsers.add_parser("train_model",
                                      description="Import from S3, create trained model, populate RDS parameters table")
    sp_train.add_argument('--s3path', default='s3://2022-msia423-summer-jason/data/raw/places_data_clean.csv',
                        help="If used, will load data via pandas")
    sp_train.add_argument("--engine_string", default=SQLALCHEMY_DATABASE_URI,
                           help="SQLAlchemy connection URI for database")

    args = parser.parse_args()
    sp_used = args.subparser_name
    if sp_used == 'create_db':
        if args.engine_string is None:
            logger.error("SQLALCHEMY_DATABASE_URI environment variable not set; exiting")
            sys.exit(1)
        create_db(args.engine_string)
        if args.definitions:
            add_references(args.engine_string)
    
    elif sp_used == 'define':
        if args.engine_string is None:
            logger.error("SQLALCHEMY_DATABASE_URI environment variable not set; exiting")
            sys.exit(1)
        add_references(args.engine_string)

    elif sp_used == 'ingest':
        raw_data = import_places_api(SOCRATA_TOKEN, 
                                     SOCRATA_USERNAME, 
                                     SOCRATA_PASSWORD,
                                     socrata_dataset_identifier=SOCRATA_DATASET_IDENTIFIER)
        upload_to_s3_pandas(raw_data,
                            args.s3path)
    
    elif sp_used == 'clean':
        transform_data = model_cfg['transform_data']
        places_df = import_from_s3(args.s3path,
                                   **transform_data['import_from_s3'])
        places_pivot = prep_data(places_df,
                                 **transform_data['prep_data'])
        if transform_data['one_hot_encode']['states_region']:
            places_pivot = one_hot_encode(places_pivot,
                                          states_to_regions = states_region_mapping)
        if transform_data['scale_values']['columns']:
            if args.engine_string is None:
                logger.error("SQLALCHEMY_DATABASE_URI environment variable not set; exiting")
                sys.exit(1)
            places_pivot = scale_values(args.engine_string,
                                        places_pivot,
                                        **transform_data['scale_values'])
        upload_to_s3_pandas(places_pivot,
                            args.s3dest)

    
    elif sp_used == 'train_model':
        train_model = model_cfg['train_model']
        # Model parameters will be written to parameters table post-training
        engine_string = os.getenv("SQLALCHEMY_DATABASE_URI")
        if engine_string is None:
            raise RuntimeError("SQLALCHEMY_DATABASE_URI environment variable not set; exiting")
        places_df = import_from_s3(args.s3path,
                                   train_model['features'] + [train_model['response']])
        validate_clean(places_df, train_model['features'] + [train_model['response']])
        coeffs, intercept = train_model(places_df,
                             train_model['method'],
                             train_model['features'],
                             train_model['response'],
                             engine_string,
                             **train_model['params'])
        logger.info("HELLO")

    else:
        parser.print_help()
