S3_BUCKET = s3://2022-msia423-summer-jason/data/raw
LOCAL_DATA_PATH = data/clean/
LOCAL_DATA_RAW = data/raw/
LOCAL_MODEL_PATH = models/
MODEL_CONFIG = config/model-config.yaml

.PHONY: image database add_measures raw clean features features-recorded train-test model train-recorded score performance test-image unit-tests remove-local dirs just-pipeline acquisition+pipeline pipeline+db all

# Directory commands
dirs:
	mkdir -p ${LOCAL_DATA_PATH}
	mkdir -p ${LOCAL_MODEL_PATH}
	mkdir -p ${LOCAL_DATA_RAW}

remove-local:
	rm -f ${LOCAL_MODEL_PATH}* ${LOCAL_DATA_PATH}*

# Docker image for pipeline
image: dirs
	docker build -f dockerfiles/Dockerfile -t final-project .

# Database commands
database:
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project create_db

add_measures: database
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project add_measures

# Raw data acquisition
raw:
	docker run -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e SOCRATA_TOKEN -e SOCRATA_USERNAME -e SOCRATA_PASSWORD --mount type=bind,source="$(shell pwd)",target=/app/ final-project ingest --config=${MODEL_CONFIG} --output=${S3_BUCKET}places_raw_data.csv

# Model pipeline commands (clean -> model evaluation)
${LOCAL_DATA_PATH}clean.csv:
	docker run -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY --mount type=bind,source="$(shell pwd)",target=/app/ final-project clean --config=${MODEL_CONFIG} --input=${S3_BUCKET}places_raw_data.csv --output=${LOCAL_DATA_PATH}clean.csv

clean: ${LOCAL_DATA_PATH}clean.csv

${LOCAL_DATA_PATH}featurized.csv: ${LOCAL_DATA_PATH}clean.csv
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project featurize --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}clean.csv --output=${LOCAL_DATA_PATH}featurized.csv

features: ${LOCAL_DATA_PATH}featurized.csv

${LOCAL_DATA_PATH}train_test.csv ${LOCAL_MODEL_PATH}model.sav &: ${LOCAL_DATA_PATH}featurized.csv
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project train --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}featurized.csv --output=${LOCAL_DATA_PATH}train_test.csv --model=${LOCAL_MODEL_PATH}model.sav

train-test: ${LOCAL_DATA_PATH}train_test.csv

model: ${LOCAL_MODEL_PATH}model.sav

${LOCAL_DATA_PATH}score.csv: ${LOCAL_DATA_PATH}train_test.csv ${LOCAL_MODEL_PATH}model.sav
	docker run --mount type=bind,source="$(shell pwd)",target=/app/ final-project score --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}train_test.csv --output=${LOCAL_DATA_PATH}score.csv --model=${LOCAL_MODEL_PATH}model.sav

score: ${LOCAL_DATA_PATH}score.csv

${LOCAL_MODEL_PATH}performance.png: ${LOCAL_DATA_PATH}score.csv
	docker run --mount type=bind,source="$(shell pwd)",target=/app/ final-project evaluate --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}score.csv --output=${LOCAL_MODEL_PATH}performance.png

performance: ${LOCAL_MODEL_PATH}performance.png

# Model pipeline steps that include RDS writing
features-recorded:
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project featurize --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}clean.csv --output=${LOCAL_DATA_PATH}featurized.csv --write

train-recorded:
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project train --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}featurized.csv --output=${LOCAL_DATA_PATH}train_test.csv --model=${LOCAL_MODEL_PATH}model.sav --write

# Full process commands
# Reads raw from local, trains, evaluates model; nothing written to DB
just-pipeline: dirs image clean features train-test model score performance

# just-pipeline with raw acquisition; no DB operations 
acquisition+pipeline: dirs image raw clean features train-test model score performance

# all model operations while creating and writing to DB; no data acquisition
pipeline+db: dirs image database add_measures clean features-recorded train-recorded model score performance

# data acquisition, all model operations, all DB operations
all: dirs image database add_measures raw clean features-recorded train-recorded score performance

# Unit testing
test-image:
	docker build -f dockerfiles/Dockerfile.test -t final-project-test .

unit-tests:
	docker run final-project-test
