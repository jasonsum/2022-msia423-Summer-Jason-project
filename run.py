"""
Configures the subparsers for receiving command line arguments for each
stage in the model pipeline and orchestrates their execution.
"""

import argparse
import logging
import logging.config
import yaml
import sys

# Configurations
import config.config as config

# Modules
from src.models import create_db
from src.add_definitions import add_references
from src.retrieve_data import import_places_api, upload_file
from src.clean import import_file, validate_df, prep_data
from src.featurize import reformat_measures, scale_values, one_hot_encode
from src.run_model import fit_model, add_params, dump_model
from src.train_test_split import split_data
from src.score import import_model, pred_responses
from src.evaluate import visualize_performance

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
        description="Build and populate database, acquire data, clean, featurize\
                     train model, score model, and/or evaluate model.")

    parser.add_argument('step', help='Which step to run', choices=['create_db', 'add_measures', 'ingest', 'clean', 
                                                                   'featurize', 'train', 'score', 'evaluate'])
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
            logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
            sys.exit(1)
        create_db(config.SQLALCHEMY_DATABASE_URI) # Create tables
    
    # Population measurement definitions
    elif args.step == "add_measures":
        if config.SQLALCHEMY_DATABASE_URI is None:
            logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
            sys.exit(1)
        add_references(config.SQLALCHEMY_DATABASE_URI) # Add metric definitions
    
    # Get raw data from API
    elif args.step == "ingest":
        raw_data = import_places_api(**mdl_config['data']['import_places_api'],
                                     app_token=config.SOCRATA_TOKEN,   # type: ignore
                                     socrata_username=config.SOCRATA_USERNAME,   # type: ignore
                                     socrata_password=config.SOCRATA_PASSWORD)  # type: ignore
        upload_file(raw_data, args.output)
    
    # Clean raw data
    elif args.step == "clean":
        clean_data = mdl_config['clean']
        places_df = import_file(args.input, **clean_data['import_file'])
        validate_df(places_df, **clean_data['validate_df'])
        places_pivot = prep_data(places_df, **clean_data['prep_data'])
        upload_file(places_pivot, args.output)
    
    # Featurize clean data
    elif args.step == "featurize":
        featurize_data = mdl_config['featurize']
        places_pivot = import_file(args.input, **featurize_data['import_file'])
        validate_df(places_pivot, **featurize_data['validate_df'])
        places_pivot = reformat_measures(places_pivot,  **featurize_data['reformat_measures'])
        if featurize_data['one_hot_encode']['states_region']:
            places_pivot = one_hot_encode(places_pivot,
                                          states_to_regions = states_region_mapping)
        if featurize_data['scale_values']['columns']:
            # Capture correct RDS
            if config.SQLALCHEMY_DATABASE_URI is None:
                logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
                sys.exit(1)
            places_pivot = scale_values(config.SQLALCHEMY_DATABASE_URI,
                                        places_pivot,
                                        **featurize_data['scale_values'])
        upload_file(places_pivot, args.output)
    
    # Train model
    elif args.step == "train":
        # Capture correct RDS
        if config.SQLALCHEMY_DATABASE_URI is None:
            logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
            sys.exit(1)
        
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
        add_params(config.SQLALCHEMY_DATABASE_URI, params)
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
    
    # Evaluate performance
    elif args.step == "evaluate":
        test_df = import_file(args.input)
        validate_df(test_df, **mdl_config['evaluate']['validate_df'])
        visualize_performance(test_df,
                             save_file_path = args.output,
                             **mdl_config['evaluate']['visualize_performance'])
    else:
        parser.print_help()

