"""Configures the subparsers for receiving command line arguments for each
 stage in the model pipeline and orchestrates their execution."""
import argparse
import logging
import logging.config
import os
import yaml
import sys

# Configurations
import config.config as config

# Modules
from src.models import create_db
from src.add_definitions import add_references
from src.retrieve_data import import_places_api, upload_file
from src.transform_data import import_file, validate_df, prep_data, scale_values, one_hot_encode
from src.run_model import fit_model, add_params, dump_model
from src.train_test_split import split_data
from src.score import import_model, pred_responses

# References
from data.reference.state_region_mapping import states_region_mapping

"""
Expected environment variables:
API:
SOCRATA_TOKEN, SOCRATA_USERNAME, SOCRATA_PASSWORD
Database:
SQLALCHEMY_DATABASE_URI
S3:
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
"""

logging.config.fileConfig('config/logging/local.conf')
logger = logging.getLogger('model_pipeline')

if __name__ == '__main__':

    # Create parsers to determine transaction
    parser = argparse.ArgumentParser(
        description="Build database, acquire data, transform data\
                     train model, score model, and/or evaluate model.")

    parser.add_argument('step', help='Which step to run', choices=['create_db', 'ingest', 
                                                                   'transform', 'train', 'score'])
    parser.add_argument("--engine_string", default=config.SQLALCHEMY_DATABASE_URI,
                           help="SQLAlchemy connection URI for database")
    parser.add_argument('--config', default='config/model-config.yaml', help='Path to configuration file')
    parser.add_argument('--input', '-i', default=None, help='Path to retrieve input file')
    parser.add_argument('--output', '-o', default=None, help='Path to save transaction output file')
    parser.add_argument('--model', '-m', default=None, help='Path to trained model object')
    args = parser.parse_args()

    # Load configuration file
    with open('config/model-config.yaml', 'r') as f:
        mdl_config = yaml.load(f, Loader = yaml.FullLoader)
    
    # Create database
    if args.step == 'create_db':
        if config.SQLALCHEMY_DATABASE_URI is None:
            if args.engine_string:
                engine_string = args.engine_string
            else:
                logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
                logger.error("Otherwise, use engine_string argument; exiting.")
                sys.exit(1)
        else:
            engine_string = config.SQLALCHEMY_DATABASE_URI
        
        create_db(engine_string) # Create tables
        add_references(engine_string) # Add metric definitions
    
    # Get raw data from API
    elif args.step == "ingest":
        raw_data = import_places_api(**mdl_config['data']['import_places_api'],
                                     app_token=config.SOCRATA_TOKEN,   # type: ignore
                                     socrata_username=config.SOCRATA_USERNAME,   # type: ignore
                                     socrata_password=config.SOCRATA_PASSWORD)  # type: ignore
        upload_file(raw_data, args.output)
    
    # Clean and transform data into feature set
    elif args.step == "transform":
        transform_data = mdl_config['transform_data']
        places_df = import_file(args.input, **transform_data['import_file'])
        validate_df(places_df, **transform_data['validate_df'])
        places_pivot = prep_data(places_df, **transform_data['prep_data'])
        if transform_data['one_hot_encode']['states_region']:
            places_pivot = one_hot_encode(places_pivot,
                                          states_to_regions = states_region_mapping)
        if transform_data['scale_values']['columns']:
            # Capture correct RDS
            if config.SQLALCHEMY_DATABASE_URI is None:
                if args.engine_string:
                    engine_string = args.engine_string
                else:
                    logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
                    logger.error("Otherwise, use engine_string argument; exiting.")
                    sys.exit(1)
            else:
                engine_string = config.SQLALCHEMY_DATABASE_URI
            places_pivot = scale_values(engine_string,
                                        places_pivot,
                                        **transform_data['scale_values'])
        upload_file(places_pivot, args.output)
    
    # Train model
    elif args.step == "train":
        # Capture correct RDS
        if config.SQLALCHEMY_DATABASE_URI is None:
            if args.engine_string:
                engine_string = args.engine_string
            else:
                logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
                logger.error("Otherwise, use engine_string argument; exiting.")
                sys.exit(1)
        else:
            engine_string = config.SQLALCHEMY_DATABASE_URI
        
        # Import data
        train_model = mdl_config['train_model']
        places_df = import_file(args.input,
                                train_model['features'] + [train_model['response']])
        validate_df(places_df, **train_model['validate_df'])

        # Split into train-test
        combined_df = split_data(places_df, **mdl_config['train_test_split'])
        upload_file(combined_df, args.output) # Save train-test dataframe

        # Train model
        training_set = combined_df.loc[combined_df.training == 1].copy()
        params, model = fit_model(training_set,
                           train_model['method'],
                           train_model['features'],
                           train_model['response'],
                           **train_model['params'])
        add_params(engine_string, params)
        dump_model(model,args.model)

    # Score model
    elif args.step == "score":
        # Import model and re-create test set
        model = import_model(args.model)
        combined_df = import_file(args.input)
        validate_df(combined_df, **mdl_config['score']['validate_df'])
        test_df = combined_df.loc[combined_df.training == 0].copy()
        test_df = pred_responses(model, 
                                 test_df,
                                 mdl_config['train_model']['features'])
        upload_file(test_df, args.output) # Save test predictions dataframe
    
    else:
        parser.print_help()

