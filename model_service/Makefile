DOCKER_REMOTE_URI := "000000.dkr.ecr.eu-west-2.amazonaws.com/premium-prediction-model"
JSON_FILE := "config.json"

upload_config:
	aws s3 cp $(JSON_FILE) s3://$(BUCKET_NAME)/$(JSON_FILE)

build:
	docker build --build-arg BUCKET_NAME=${BUCKET_NAME} -t premium-prediction-model:v1 .

run_local:
	docker run -it \
	-e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
	-e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
	-e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
	-e PREFECT_CLOUD_API_KEY="${PREFECT_CLOUD_API_KEY}" \
	premium-prediction-model:v1

push:
	DOCKER_REMOTE_URI=$(DOCKER_REMOTE_URI) python3 docker_push.py

delete:
	docker image rm $(DOCKER_REMOTE_URI):v1