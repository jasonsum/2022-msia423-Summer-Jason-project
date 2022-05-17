import os

## Database
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI") # should be passed as environment variable
if SQLALCHEMY_DATABASE_URI is None:
    SQLALCHEMY_DATABASE_URI = 'sqlite:///data/places.db' 

## API ##
SOCRATA_DATASET_IDENTIFIER = "cwsq-ngmh"
SOCRATA_TOKEN = os.getenv("SOCRATA_TOKEN")
SOCRATA_USERNAME = os.getenv("SOCRATA_USERNAME")
SOCRATA_PASSWORD = os.getenv("SOCRATA_PASSWORD")

# S3
# model pipeline - s3 path should be input/output
# for app s3 should be environemnt variable