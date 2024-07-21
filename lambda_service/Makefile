build:
	docker build --build-arg BUCKET_NAME=${BUCKET_NAME} -t premium-prediction-lambda:v1 .

push:
	DOCKER_REMOTE_URI=$(DOCKER_REMOTE_URI) python3 docker_push.py

run_local:
	docker run -it \
	-p 8080:8080 \
	-e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
	-e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
	-e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
	premium-prediction-lambda:v1

delete:
	docker image rm $(DOCKER_REMOTE_URI):v1