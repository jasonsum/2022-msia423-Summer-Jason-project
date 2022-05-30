"""
Configures the subparsers for receiving command line arguments for each
stage in the model pipeline and orchestrates their execution.
"""

import argparse
import logging
import logging.config
import sys

import yaml
import requests
import botocore
import sqlalchemy.exc

# Configurations
from config import config

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

logging.config.fileConfig("config/logging/local.conf")
logger = logging.getLogger("model_pipeline")

if __name__ == "__main__":

    # Create parsers to determine transaction
    parser = argparse.ArgumentParser(
        description="Build and populate database, acquire data, clean, featurize\
                     train model, score model, and/or evaluate model.")

    parser.add_argument("step", help="Which step to run", choices=["create_db", "add_measures", "ingest", "clean",
                                                                   "featurize", "train", "score", "evaluate"])
    parser.add_argument("--config", default="config/model-config.yaml", help="Path to configuration file")
    parser.add_argument("--input", "-i", default=None, help="Path to retrieve input file")
    parser.add_argument("--output", "-o", default=None, help="Path to save transaction output file")
    parser.add_argument("--model", "-m", default=None, help="Path to trained model object")
    args = parser.parse_args()

    # Load configuration file
    try:
        with open(args.config, "r") as f:
            mdl_config = yaml.load(f, Loader = yaml.FullLoader)
    except FileNotFoundError:
        logger.error("Please provide a valid configuration file; exiting.")
        sys.exit(1)

    # Create database
    if args.step == "create_db":
        if config.SQLALCHEMY_DATABASE_URI is None:
            logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
            sys.exit(1)
        try:
            create_db(config.SQLALCHEMY_DATABASE_URI) # Create tables
        except sqlalchemy.exc.OperationalError:
            logger.error("A connection error has occurred. Unable to create database.")
            sys.exit(1)

    # Population measurement definitions
    elif args.step == "add_measures":
        if config.SQLALCHEMY_DATABASE_URI is None:
            logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
            sys.exit(1)
        try:
            add_references(config.SQLALCHEMY_DATABASE_URI) # Add metric definitions
        except sqlalchemy.exc.OperationalError:
            logger.error("A connection error has occurred. Unable to update database.")
            sys.exit(1)
        except sqlalchemy.exc.IntegrityError:
            logger.error("A primary key violation has occurred. Please ensure table is empty.")
            sys.exit(1)

    # Get raw data from API
    elif args.step == "ingest":
        if not mdl_config["ingest"]:
            logger.error("Configuration file is missing section for selected step; exiting.")
            sys.exit(1)
        try:
            raw_data = import_places_api(**mdl_config["ingest"]["import_places_api"],
                                        app_token=config.SOCRATA_TOKEN,   # type: ignore
                                        socrata_username=config.SOCRATA_USERNAME,   # type: ignore
                                        socrata_password=config.SOCRATA_PASSWORD)  # type: ignore
        except requests.exceptions.ConnectionError:
            logger.error("A connection error has occurred; exiting.")
            sys.exit(1)
        except requests.exceptions.HTTPError:
            logger.error("An error has occurred with the socrata_dataset_identifier or query; exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error("An error has occurred while trying to acquire data: %s", e)
            logger.error("The application is exiting.")
            sys.exit(1)
        else:
            try:
                upload_file(raw_data, args.output)
            except FileNotFoundError:
                logger.error("An invalid file location has been provided; exiting.")
                sys.exit(1)
            except botocore.exceptions.NoCredentialsError:  # type: ignore
                logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
                sys.exit(1)
            except Exception as e:
                logger.error("An error has occurred while trying to upload file: %s.", e)
                logger.error("The application is exiting.")
                sys.exit(1)

    # Clean raw data
    elif args.step == "clean":
        # Set config
        if not mdl_config["clean"]:
            logger.error("Configuration file is missing section for selected step; exiting.")
            sys.exit(1)
        clean_data = mdl_config["clean"]

        # Import file
        try:
            places_df = import_file(args.input, **clean_data["import_file"])
        except botocore.exceptions.NoCredentialsError:  # type: ignore
            logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
            sys.exit(1)
        except FileNotFoundError:
            logger.error("An invalid file location has been provided; exiting.")
            sys.exit(1)
        except KeyError:
            logger.error("A desired column is missing from the file; exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error("There was a problem importing the file: %s.", e)
            logger.error("The application is exiting.")
            sys.exit(1)
        else:
            # Validate data
            try:
                validate_df(places_df, **clean_data["validate_df"])
            except ValueError:
                logger.error(
                    "Dataframe has no records, duplicate records, or contains null values; exiting.")
                sys.exit(1)
            except KeyError:
                logger.error("A desired column is missing from the file; exiting.")
                sys.exit(1)
            except TypeError:
                logger.error("A column data type mismatch has occurred; exiting.")
                sys.exit(1)
            else:
                # Clean and save
                try:
                    places_pivot = prep_data(places_df, **clean_data["prep_data"])
                    upload_file(places_pivot, args.output)
                except KeyError:
                    logger.error("Required or provided column(s) are missing from the dataframe; exiting.")
                    sys.exit(1)
                except TypeError:
                    logger.error("Data_Value column must be numeric and Column identifiers string; exiting.")
                    sys.exit(1)
                except FileNotFoundError:
                    logger.error("An invalid file location has been provided; exiting.")
                    sys.exit(1)
                except botocore.exceptions.NoCredentialsError:  # type: ignore
                    logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
                    sys.exit(1)
                except AttributeError:
                    logger.error("First argument must be a dataframe; exiting.")
                    sys.exit(1)
                except Exception as e:
                    logger.error("There was a problem saving to file: %s.", e)
                    logger.error("The application is exiting.")
                    sys.exit(1)

    # Featurize clean data
    elif args.step == "featurize":
        # Set config
        if not mdl_config["featurize"]:
            logger.error("Configuration file is missing section for selected step; exiting.")
            sys.exit(1)
        featurize_data = mdl_config["featurize"]

        # Import file
        try:
            places_pivot = import_file(args.input, **featurize_data["import_file"])
        except botocore.exceptions.NoCredentialsError:  # type: ignore
            logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
            sys.exit(1)
        except FileNotFoundError:
            logger.error("An invalid file location has been provided; exiting.")
            sys.exit(1)
        except KeyError:
            logger.error("A desired column is missing from the file; exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error("There was a problem importing the file: %s.", e)
            logger.error("The application is exiting.")
            sys.exit(1)

        # Validate data
        else:
            try:
                validate_df(places_pivot, **featurize_data["validate_df"])
            except ValueError:
                logger.error(
                    "Dataframe has no records, duplicate records, or contains null values; exiting.")
                sys.exit(1)
            except KeyError:
                logger.error("A desired column is missing from the file; exiting.")
                sys.exit(1)
            except TypeError:
                logger.error("A column data type mismatch has occurred; exiting.")
                sys.exit(1)
            else:
                #  Featurize data
                try:
                    places_pivot = reformat_measures(places_pivot,  **featurize_data["reformat_measures"])
                    if featurize_data["one_hot_encode"]["states_region"]:
                        places_pivot = one_hot_encode(places_pivot,
                                                    states_to_regions = states_region_mapping)
                    if featurize_data["scale_values"]["columns"]:
                        # Capture correct RDS
                        if config.SQLALCHEMY_DATABASE_URI is None:
                            logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
                            sys.exit(1)
                        places_pivot = scale_values(config.SQLALCHEMY_DATABASE_URI,
                                                    places_pivot,
                                                    **featurize_data["scale_values"])
                except KeyError:
                    logger.error("Please check your columns. Missing missing column(s) necessary\
                                  for featurization are missing; exiting.")
                    sys.exit(1)
                except TypeError:
                    logger.error("A non-numeric column has been passed for quantitative transformation; exiting.")
                    sys.exit(1)
                else:
                    # Save to file
                    try:
                        upload_file(places_pivot, args.output)
                    except FileNotFoundError:
                        logger.error("An invalid file location has been provided; exiting.")
                        sys.exit(1)
                    except botocore.exceptions.NoCredentialsError:  # type: ignore
                        logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
                        sys.exit(1)
                    except Exception as e:
                        logger.error("There was a problem saving to file: %s.", e)
                        logger.error("The application is exiting.")
                        sys.exit(1)

    # Train model
    elif args.step == "train":
        # Set config
        if not mdl_config["clean"]:
            logger.error("Configuration file is missing section for selected step; exiting.")
            sys.exit(1)
        train_model = mdl_config["train_model"]

        # Capture correct RDS
        if config.SQLALCHEMY_DATABASE_URI is None:
            logger.error("Specify SQLALCHEMY_DATABASE_URI environment variable.")
            sys.exit(1)

        # Import file
        try:
            places_df = import_file(args.input,
                                    train_model["features"] + [train_model["response"]])
        except botocore.exceptions.NoCredentialsError:  # type: ignore
            logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
            sys.exit(1)
        except FileNotFoundError:
            logger.error("An invalid file location has been provided; exiting.")
            sys.exit(1)
        except KeyError:
            logger.error("A desired column is missing from the file; exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error("There was a problem importing the file: %s.", e)
            logger.error("The application is exiting.")
            sys.exit(1)
        else:
            # Validate data
            try:
                validate_df(places_df, **train_model["validate_df"])
            except ValueError:
                logger.error(
                    "Dataframe has no records, duplicate records, or contains null values; exiting.")
                sys.exit(1)
            except KeyError:
                logger.error("A desired column is missing from the file; exiting.")
                sys.exit(1)
            except TypeError:
                logger.error("A column data type mismatch has occurred; exiting.")
                sys.exit(1)
            else:
                try:
                    # Split into train-test and save file
                    combined_df = split_data(places_df, **mdl_config["train_test_split"])
                    upload_file(combined_df, args.output) # Save train-test dataframe
                except TypeError:
                    logger.error("test_size must be a float and random_state an integer; exiting")
                    sys.exit(1)
                except ValueError:
                    logger.error("Value of test_size must be between 0 and 1; exiting")
                    sys.exit(1)
                except FileNotFoundError:
                    logger.error("An invalid file location has been provided; exiting.")
                    sys.exit(1)
                except botocore.exceptions.NoCredentialsError:  # type: ignore
                    logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
                    sys.exit(1)
                except Exception as e:
                    logger.error("There was a problem saving to file: %s.", e)
                    logger.error("The application is exiting.")
                    sys.exit(1)
                else:
                    # Train model, add params to DB, save trained model object
                    try:
                        training_set = combined_df.loc[combined_df.training == 1].copy()
                        params, model = fit_model(training_set,
                                                  features = train_model["features"],
                                                  response = train_model["response"],
                                                  method = train_model["method"],
                                                  **train_model["params"])
                        add_params(config.SQLALCHEMY_DATABASE_URI, params)
                        dump_model(model,args.model)
                    except TypeError:
                        logger.error("Passed params and columns must be valid for \
                                      sklearn.linear_model.LinearRegression; exiting.")
                        sys.exit(1)
                    except ValueError:
                        logger.error("Features should be 2D and target 1D values; exiting.")
                        sys.exit(1)
                    except KeyError:
                        logger.error("Required or entered columns are missing from dataframe; exiting.")
                        sys.exit(1)
                    except sqlalchemy.exc.OperationalError:
                        logger.error("A connection error has occurred. Unable to update database.")
                        sys.exit(1)
                    except sqlalchemy.exc.IntegrityError:
                        logger.error("A primary key violation has occurred. Please ensure table is empty.")
                        sys.exit(1)
                    except FileNotFoundError:
                        logger.error("An invalid file location has been provided; exiting.")
                        sys.exit(1)
                    except Exception as e:
                        logger.error("There was a problem during model training and capturing: %s.", e)
                        logger.error("The application is exiting.")
                        sys.exit(1)

    # Score model
    elif args.step == "score":
        # Check config
        if not mdl_config["score"] and mdl_config["train_model"]:
            logger.error("Configuration file is missing section for selected step; exiting.")
            sys.exit(1)

        # Import model and data file
        try:
            model = import_model(args.model)
            combined_df = import_file(args.input)
        except botocore.exceptions.NoCredentialsError:  # type: ignore
            logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
            sys.exit(1)
        except FileNotFoundError:
            logger.error("An invalid file location has been provided; exiting.")
            sys.exit(1)
        except KeyError:
            logger.error("A desired column is missing from the file; exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error("There was a problem importing the file: %s.", e)
            logger.error("The application is exiting.")
            sys.exit(1)
        else:
            # Validate data
            try:
                validate_df(combined_df, **mdl_config["score"]["validate_df"])
            except ValueError:
                logger.error(
                    "Dataframe has no records, duplicate records, or contains null values; exiting.")
                sys.exit(1)
            except KeyError:
                logger.error("A desired column is missing from the file; exiting.")
                sys.exit(1)
            except TypeError:
                logger.error("A column data type mismatch has occurred; exiting.")
                sys.exit(1)
            else:
                # Generate predictions and save
                try:
                    test_df = combined_df.loc[combined_df.training == 0].copy()
                    # Generate predictions
                    test_df = pred_responses(model,
                                            test_df,
                                            mdl_config["train_model"]["features"])
                    upload_file(test_df, args.output) # Save test predictions dataframe

                except ValueError:
                    logger.error("X_test should be 2D of feature values; exiting.")
                    sys.exit(1)
                except KeyError:
                    logger.error("The provided columns could not be found in the dataframe; exiting.")
                    sys.exit(1)
                except FileNotFoundError:
                    logger.error("An invalid file location has been provided; exiting.")
                    sys.exit(1)
                except botocore.exceptions.NoCredentialsError:  # type: ignore
                    logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
                    sys.exit(1)
                except Exception as e:
                    logger.error("There was a problem saving to file: %s.", e)
                    logger.error("The application is exiting.")
                    sys.exit(1)

    # Evaluate performance
    elif args.step == "evaluate":
        # Check config
        if not mdl_config["evaluate"]:
            logger.error("Configuration file is missing section for selected step; exiting.")
            sys.exit(1)

        # Import file
        try:
            test_df = import_file(args.input)
        except botocore.exceptions.NoCredentialsError:  # type: ignore
            logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
            sys.exit(1)
        except FileNotFoundError:
            logger.error("An invalid file location has been provided; exiting.")
            sys.exit(1)
        except KeyError:
            logger.error("A desired column is missing from the file; exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error("There was a problem importing the file: %s.", e)
            logger.error("The application is exiting.")
            sys.exit(1)
        else:
            # Validate data
            try:
                validate_df(test_df, **mdl_config["evaluate"]["validate_df"])
            except ValueError:
                logger.error(
                    "Dataframe has no records, duplicate records, or contains null values; exiting.")
                sys.exit(1)
            except KeyError:
                logger.error("A desired column is missing from the file; exiting.")
                sys.exit(1)
            except TypeError:
                logger.error("A column data type mismatch has occurred; exiting.")
                sys.exit(1)
            else:
                # Generate performance eval
                try:
                    visualize_performance(test_df,
                                        save_file_path = args.output,
                                        **mdl_config["evaluate"]["visualize_performance"])
                except ValueError:
                    logger.error("There was a datatype mismatch or null value found; exiting.")
                    sys.exit(1)
                except KeyError:
                    logger.error("Test dataframe missing provided columns for prediction or true values; exiting.")
                    sys.exit(1)
                except TypeError:
                    logger.error("A column data type mismatch has occurred; exiting.")
                    sys.exit(1)
                except FileNotFoundError:
                    logger.error("An invalid file location has been provided; exiting.")
                    sys.exit(1)
                except botocore.exceptions.NoCredentialsError:  # type: ignore
                    logger.error("Missing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY credentials; exiting.")
                    sys.exit(1)
                except Exception as e:
                    logger.error("There was a problem saving to file: %s.", e)
                    logger.error("The application is exiting.")
                    sys.exit(1)
    else:
        parser.print_help()
