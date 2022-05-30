# Identifying Leading Indicators of Population Health

Project Creator & Developer: Jason Summer

# Project Charter

## Background

In 2015, the Centers for Disease Control and Prevention (CDC), CDC Foundation, and the Robert Wood Johnson Foundation began the 500 Cities Project, an effort to provide population health estimates for small geographic areas for health officials, policy makers, nonprofits, and other organizations seeking to improve health. In 2016, the partnership expanded its geographic breadth of health estimates with PLACES (Population Level Analysis and Community Estimates), enabling much larger scale analysis of health measures nationwide. 

## Vision

To improve population health demographics and outcomes, this effort seeks to prioritize key contributors and leading indicators while highlighting at-risk geographical areas as indicated by poor population health statuses and outcomes.

## Mission

Machine learning modeling is used as a mechanism for both highlighting key contributors of health statuses and estimating population health risk. More specifically, machine learning models leverage 2019 [PLACES](https://chronicdata.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-Census-Tract-D/cwsq-ngmh) data to predict population proportions with poor/fair health by understanding the relationship among 18 health measures, population sizes, and geographical areas. 

The selected model is served to users through an open web application, wherein users can select various health outcomes, population sizes, and geographical areas to receive a predicted population proportion with poor/fair health. More importantly, the application seeks to accomplish the two key tasks - highlight the impact to predicted population proportions based on changing user inputs and provide a proactive tool for users to identify populations that may be at-risk based on trending figures. 

## Success Criteria

RMSE (root mean squared error) of the predicted population proportion with poor/fair health is the selected metric to gauge a more traditional success of the preferred machine learning model. The initial threshold set for success is a RMSE value of 2.5 percentage points in regards to the predicted proportion. This margin of error corresponds to the confidence interval of the original labels found in PLACES, which are estimated by the data compilers.

The overall business success of the web application is based on usage and viewership. To better articulate and measure such goals, return users (within 1 week) and number of predictions generated per user session should be used. 

## Project Structure Overview

The project is broken into two isolated but related components. The first component is a predictive model pipeline that leverages the raw 2019 [PLACES](https://chronicdata.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-Census-Tract-D/cwsq-ngmh) data mentioned previously to create a measurable and reproducible model. The steps for this first component, explicitly, are database initialization, initial database population, raw data acquisition, data cleaning, data featurization, model training, prediction generation, and prediction evaluation. 

The second component, is the open web application referenced previously. The web application can be executed via a local host by re-initializing the same database created for the model pipeline. Simiarly, the trained model can be re-created to render predictions on the locally served web app.

# Table of Contents
* [Directory structure ](#Directory-structure)
* [Running the model pipeline ](#Directory-structure)
	* [1. Initialize the database ](#1.-Initialize-the-database)
	* [2. Acquire raw data ](#1.-Initialize-the-database)
	* [3. Clean and featurize ](#1.-Initialize-the-database)
	* [4. Train model and evaluate performance ](#1.-Initialize-the-database)
* [Running the app ](#Running-the-app)
	* [Building the image ](#1.-Initialize-the-database)
	* [Running the app ](#1.-Initialize-the-database)
	* [Kill the container ](#2.-Configure-Flask-app)
* [Testing](#Testing)
* [Mypy](#Mypy)
* [Pylint](#Pylint)


## Directory structure 

```
├── README.md                         <- You are here
├── app
│   ├── static/                       <- CSS that remain static
│   ├── templates/                    <- HTML (or other code) that is templated and changes based on a set of inputs    
│
├── config                            <- Directory for configuration files 
│   ├── local/                        <- Directory for keeping environment variables and other local configurations that *do not sync** to Github 
│   ├── logging/                      <- Configuration of python loggers
│   ├── flaskconfig.py                <- Configurations for Flask API 
│   ├── config.py                     <- Configurations for capturing environment variable passed explicitly to run.py 
│   ├── model-config.py               <- Model pipelineConfigurations for capturing environment variable passed explicitly to run.py 
│
├── data                              <- Folder that contains data used or generated. Only the external/, sample/, and reference/ are tracked by git. 
│   ├── external/                     <- External data sources, usually reference data,  will be synced with git
│   ├── sample/                       <- Sample data used for code development and testing, will be synced with git
│   ├── reference/                    <- Data mapping values and other reference material necessary for execution
│
├── deliverables/                     <- Presentation materials for stakeholder consumption 
│
├── docs/                             <- Sphinx documentation based on Python docstrings. Optional for this project.
|
├── dockerfiles/                      <- Directory for all project-related Dockerfiles 
│   ├── Dockerfile.app                <- Dockerfile for building image to run web app.
│   ├── Dockerfile.run                <- Dockerfile for building image to execute run.py. 
│   ├── Dockerfile.test               <- Dockerfile for building image to run unit tests.
│
├── Makefile/                         <- Organizes execution of pipeline tasks and dependencies.
│
│
├── models/                           <- Trained model objects (TMOs), model predictions, and/or model summaries.
│
├── notebooks/
│   ├── archive/                      <- Open directory for previous notebooks.
│   ├── deliver/                      <- Open directory for notebooks to share with others.
│   ├── develop/                      <- Open directory for active development.
│
├── reference/                        <- CDC measure information
│
├── src/                              <- Source data for the project; Python modules executed via run.py and app.py  
│
├── test/                             <- Files necessary for running unit tests on pipeline 
│
├── app.py                            <- Flask wrapper for running the web app 
├── run.py                            <- Simplifies the execution of modules contained in src/  
├── requirements.txt                  <- Python package dependencies 
```
## Running the model pipeline 
All steps in the pipeline can be run from docker. Alternatively, Make can be used to run every step in the pipeline or the entire pipeline.  Please use one or the other in each step. Running both is unnecessary. If you do not wish to run every step individually, you may execute the entire pipeline, starting from raw data acquisition through to model evaluation using the below commands.

```bash
 make pipeline
```

Before running the above, please note that there are a couple requirements, as noted in the Makefile. These requirements are illustrated in the respective step below; however, they are summarised here.
 - Database string must be set as environment variable SQLALCHEMY_DATABASE_URI.
 - AWS credentials (if you use S3) for raw data ingestion must be set as environment variables AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY.
 - A token, username, and password for (free) Socrata API must be obtained [here](https://chronicdata.cdc.gov/profile/edit/developer_settings) and set as environment variables SOCRATA_TOKEN, SOCRATA_USERNAME, SOCRATA_PASSWORD.

### 1. Initialize the database 
#### Create local sub-directories 

To ensure certain sub-directories are available within the docker container, run the below from the root of the repo): 

```bash
 mkdir -p data/clean/
 mkdir -p models/
```
Or you may use the below, Make command.

Make:
```bash
 make dirs
```
#### Build the image 

To build the image, run from this directory (the root of the repo): 

Docker:

```bash
 docker build -f dockerfiles/Dockerfile -t final-project .
```

Make:
```bash
 make image
```
#### Create the database 

First ensure a database (local or remote) has been configured as environment variable, SQLALCHEMY_DATABASE_URI. To build the database, run the below: 

Docker:

```bash
 docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(pwd)",target=/app/ final-project create_db
```
The `--mount` argument allows the app to access your local directory  and render outputs there after the Docker container finishes.

Make:
```bash
 make database
```
#### Populate the database 

To add initial records to the database, run the below statement. This will populate CDC measurement definitions which are used in the web app.

Docker:

```bash
 docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(pwd)",target=/app/ final-project add_measures
```
Make:
```bash
 make add_measures
```

### 2. Acquire raw data 

The data acquistion is configured to source the data from a CDC API (maintained by Socrata) and place the result as a csv in an S3 bucket. The API, a token, username, and password must be obtained and passed as environment variable. One can register and generate these credentials for free [here](https://chronicdata.cdc.gov/profile/edit/developer_settings). Once you've obtained these credentials, set them as environment variables SOCRATA_TOKEN, SOCRATA_USERNAME, and SOCRATA_PASSWORD, respectively.

To use an S3 bucket as a destination, ensure your AWS credentials are set as environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

Docker:

```bash
 docker run -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e SOCRATA_TOKEN -e SOCRATA_USERNAME -e SOCRATA_PASSWORD --mount type=bind,source="$(pwd)",target=/app/ final-project ingest --config=config/model-config.yaml --output=${S3_BUCKET}raw/raw_places.csv
```
The `--config` argument should be used for the pipeline configuration provided in the repository. If you do not wish to use an S3 bucket, you may change the `--output` argument to a different destination and remove the AWS credentials in the above command.

Make:
```bash
 make raw
```
Again, the Makefile is configured to use S3 as a destination. To change to a different S3 bucket, change the S3_BUCKET variable at top of the Makefile. If you do not wish to use S3 as a destination, again remove the AWS credentials from the corresonding Makefile command and change the `--output` value in the Makefile.

### 3. Clean and featurize
#### Clean the raw data

The raw data will be imported from the previous step's `--output` destination. Similar to the previous step, AWS credentials should be set as environment variable or removed if you do not wish to a S3 bucket. 

To clean the raw data, run the below statement:

Docker:

```bash
 docker run -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY --mount type=bind,source="$(pwd)",target=/app/ final-project clean --config=config/model-config.yaml --input=${S3_BUCKET}raw/raw_places.csv --output=data/clean/clean.csv
```
Make:
```bash
 make clean
```
#### Featurize the data

The clean data will be imported from the previous step's `--output` destination. During featurization, some columns may be scaled using min-max scaling. Their corresonding minimum and maximums will be captured in a database table, which requires the same SQLALCHEMY_DATABASE_URI environment variable to be set as was done previously (if it is not already done so).

To creates the feature set, run the below statement:

Docker:

```bash
 docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(pwd)",target=/app/ final-project featurize --config=config/model-config.yaml --input=data/clean/clean.csv --output=data/clean/featurized.csv
```
Make:
```bash
 make features
```
### 4. Train model and evaluate performance
#### Train the model

The feature data will be imported from the previous step's `--output` destination. During training, model parameters will be written to a database table, which requires the same SQLALCHEMY_DATABASE_URI environment variable to be set as was done previously (if it is not already done so). The training step will output two artifacts - a feature and response set which includes identification of training vs. test and a trained model object.  

To execute the training step, run the below statement:

Docker:

```bash
 docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(pwd)",target=/app/ final-project train --config=config/model-config.yaml --input=data/clean/featurized.csv --output=data/clean/train_test.csv --model=models/model.sav
```
Make:
```bash
 make model
```

#### Generate predictions using model

Both outputs, `--output` and `--model` from the previous section will be imported to generate predictions on the test set. 

To execute the scoring step, run the below statement:

Docker:

```bash
 docker run --mount type=bind,source="$(pwd)",target=/app/ final-project score --config=config/model-config.yaml --input=data/clean/train_test.csv --output=data/clean/score.csv --model=models/model.sav
```
Make:
```bash
 make score
```

#### Evalute predictions from model

The scores from the previous step's `--output` destination will be used to generate evaluation metrics. 

To execute the evaluation step, run the below statement:

Docker:

```bash
 docker run --mount type=bind,source="$(pwd)",target=/app/ final-project evaluate --config=config/model-config.yaml --input=data/clean/score.csv --output=models/performance.png
```
Make:
```bash
 make performance
```

## Running the app
Before launching the app locally, ensure the model pipeline steps have been run, at least through to training. The app uses the same database created and populated during the above model pipeline steps. Below are the steps to serving the web app locally. Please note that the model used by the app will be trained only on the training data and not the proportion of data set aside as test data, as noted in the configuration file (`test_size`). This can be updated directly and pipeline re-run to train on more or all of the data.

#### Build the image 

To build the image, run from this directory (the root of the repo): 

```bash
 docker build -f dockerfiles/Dockerfile.app -t final-project-app .
```

#### Running the app

To run the Flask app, run: 

```bash
 docker run -e SQLALCHEMY_DATABASE_URI --name flask-app --mount type=bind,source="$(pwd)",target=/app/ -p 5001:5000 final-project-app
```
You should be able to access the app at http://127.0.0.1:5001/ in your browser (Mac/Linux should also be able to access the app at http://127.0.0.1:5001/ or localhost:5001/) .

The arguments in the above command do the following: 

* The `-p 5001:5000` argument maps your computer's local port 5001 to the Docker container's port 5000 so that you can view the app in your browser. If your port 5000 is already being used for someone, you can use `-p 5001:5000` (or another value in place of 5001) which maps the Docker container's port 5000 to your local port 5001.

Note: If `PORT` in `config/flaskconfig.py` is changed, this port should be changed accordingly (as should the `EXPOSE 5000` line in `dockerfiles/Dockerfile.app`)

#### Kill the container 

Once finished with the app, the container can be killed with the below command: 

```bash
docker kill flask-app 
```

## Testing

To execute unit tests (with docker), first create the docker image with the below. 

    
```bash
docker build -f dockerfiles/Dockerfile.test -t final-project-test . 
```

Next, the below command will execute the unit tests in tests/ from the container.

```bash
docker run final-project-test
```
 Alternatively, to execute the same unit testing procedure with Makefiles. Run the below statements.
```bash
make test-image
```
```bash
make unit-tests
```