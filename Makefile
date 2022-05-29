S3_BUCKET = s3://2022-msia423-summer-jason/data/
LOCAL_DATA_PATH = data/
LOCAL_MODEL_PATH = model/
LOCAL_MODEL_PERF_PATH = model/performance/
MODEL_CONFIG = config/model-config.yaml

.PHONY: image database add_measures raw clean

image:
	docker build -f dockerfiles/Dockerfile -t final-project .

database:
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project create_db

add_measures:
	docker run -e SQLALCHEMY_DATABASE_URI --mount type=bind,source="$(shell pwd)",target=/app/ final-project add_measures

raw:
	docker run -e SOCRATA_TOKEN -e SOCRATA_USERNAME -e SOCRATA_PASSWORD --mount type=bind,source="$(shell pwd)",target=/app/ final-project ingest --config=${MODEL_CONFIG} --output=${S3_BUCKET}raw/raw_places.csv

${LOCAL_DATA_PATH}clean/clean.csv:
	docker run --mount type=bind,source="$(shell pwd)",target=/app/ final-project clean --config=${MODEL_CONFIG} --input=${S3_BUCKET}raw/raw_places.csv --output=${LOCAL_DATA_PATH}clean/clean.csv

clean: ${LOCAL_DATA_PATH}clean/clean.csv