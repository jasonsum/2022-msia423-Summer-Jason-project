S3_BUCKET = s3://2022-msia423-summer-jason/data/
LOCAL_DATA_PATH = data/clean/
LOCAL_MODEL_PATH = model/
MODEL_CONFIG = config/model-config.yaml

.PHONY: image database add_measures raw clean features train_test model score performance test-image unit-tests remove-local make-dirs

make-dirs:
	mkdir -p ${LOCAL_DATA_PATH}
	mkdir -p ${LOCAL_MODEL_PATH}

image: make-dirs
	docker build -f dockerfiles/Dockerfile -t final-project .

database:
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project create_db

add_measures: database
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project add_measures

raw:
	docker run -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e SOCRATA_TOKEN -e SOCRATA_USERNAME -e SOCRATA_PASSWORD --mount type=bind,source="$(shell pwd)",target=/app/ final-project ingest --config=${MODEL_CONFIG} --output=${S3_BUCKET}raw/raw_places.csv

${LOCAL_DATA_PATH}clean.csv: raw
	docker run -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY --mount type=bind,source="$(shell pwd)",target=/app/ final-project clean --config=${MODEL_CONFIG} --input=${S3_BUCKET}raw/raw_places.csv --output=${LOCAL_DATA_PATH}clean.csv

clean: ${LOCAL_DATA_PATH}clean.csv

${LOCAL_DATA_PATH}featurized.csv: ${LOCAL_DATA_PATH}clean.csv
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project featurize --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}clean.csv --output=${LOCAL_DATA_PATH}featurized.csv

features: ${LOCAL_DATA_PATH}featurized.csv

${LOCAL_DATA_PATH}train_test.csv ${LOCAL_MODEL_PATH}model.sav &: ${LOCAL_DATA_PATH}featurized.csv
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project train --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}featurized.csv --output=${LOCAL_DATA_PATH}train_test.csv --model=${LOCAL_MODEL_PATH}model.sav

train_test: ${LOCAL_DATA_PATH}train_test.csv

model: ${LOCAL_MODEL_PATH}model.sav

${LOCAL_DATA_PATH}score.csv: ${LOCAL_DATA_PATH}train_test.csv ${LOCAL_MODEL_PATH}model.sav
	docker run --mount type=bind,source="$(shell pwd)",target=/app/ final-project score --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}train_test.csv --output=${LOCAL_DATA_PATH}score.csv --model=${LOCAL_MODEL_PATH}model.sav

score: ${LOCAL_DATA_PATH}score.csv

${LOCAL_MODEL_PATH}performance.png: ${LOCAL_DATA_PATH}score.csv
	docker run --mount type=bind,source="$(shell pwd)",target=/app/ final-project evaluate --config=${MODEL_CONFIG} --input=${LOCAL_DATA_PATH}score.csv --output=${LOCAL_MODEL_PATH}performance.png

performance: ${LOCAL_MODEL_PATH}performance.png

test-image:
	docker build -f dockerfiles/Dockerfile.test -t final-project-test .

unit-tests:
	docker run final-project-test

remove-local:
	rm -f ${LOCAL_MODEL_PATH}* ${LOCAL_MODEL_PATH}*

