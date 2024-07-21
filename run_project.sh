#MAKE SURE THAT YOU CREATE AN S3 BUCKET, ECR REPOSITORY 'premium-prediction-lambda'
#CREATE 'mlflow' AND 'postgresql' ROLES, CHANGE 'setup_aws.py' LINE 59 WITH YOUR SECURITY GROUP ID

export AWS_ACCESS_KEY_ID="your_access_key_id"
export AWS_SECRET_ACCESS_KEY="your_secret_access_key"
export AWS_DEFAULT_REGION="your_region"
export PREFECT_CLOUD_API_KEY="your_prefect_cloud_api_key"
export BUCKET_NAME="your_bucket_name"
export DOCKER_REMOTE_URI="your_docker_remote_uri" #for premium-prediction-lambda

cd lambda_service

echo "Running lambda_service build..."
make build
if [ $? -ne 0 ]; then
    echo "Error in build"
    exit 1
fi

echo "Running push..."
make push
if [ $? -ne 0 ]; then
    echo "Error in push"
    exit 1
fi

cd ..
cd web_service

echo "Running web_service build..."
make build
if [ $? -ne 0 ]; then
    echo "Error in build"
    exit 1
fi

cd ..
cd model_service

echo "Running upload_config..."
make upload_config
if [ $? -ne 0 ]; then
    echo "Error in upload_config"
    exit 1
fi

echo "Running model_service build..."
make build
if [ $? -ne 0 ]; then
    echo "Error in build"
    exit 1
fi

echo "Running run_local..."
make run_local 
if [ $? -ne 0 ]; then
    echo "Error in run_local"
    exit 1
fi

# On Prefect Cloud UI, you can now see scheduled runs of the `model-service-deployment`.
# If you don't want to wait first scheduled run, set up a quick run on UI.
# Once the initial run completed, go through a new terminal, head into `web_service`
# and command `make run_local`