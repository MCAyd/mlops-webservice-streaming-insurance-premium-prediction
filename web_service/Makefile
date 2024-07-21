build:
	docker build --build-arg BUCKET_NAME=${BUCKET_NAME} -t premium-prediction-webservice:v1 .

run_local:
	docker run -it \
	-p 8080:8080 \
	-e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
	-e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
	-e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
	premium-prediction-webservice:v1

delete:
	docker image rm premium-prediction-webservice:v1