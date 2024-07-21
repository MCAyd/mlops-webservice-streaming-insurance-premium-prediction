import base64
import boto3
import subprocess
import os

# Tempopary ID entries for AWS ECR
client = boto3.client('ecr')
response = client.get_authorization_token()
auth_data = response['authorizationData'][0]
token = auth_data['authorizationToken']
registry_url = auth_data['proxyEndpoint']

# Docker login with JWT
decoded_token = base64.b64decode(token).decode()
username, password = decoded_token.split(':')
subprocess.run(['docker', 'login', '-u', username, '--password-stdin', registry_url], input=password.encode())

# Add docker image and push it
REMOTE_URI=os.getenv("DOCKER_REMOTE_URI")
REMOTE_TAG="v1"
REMOTE_IMAGE=f"{REMOTE_URI}:{REMOTE_TAG}"
LOCAL_IMAGE="premium-prediction-lambda:v1"

subprocess.run(['docker', 'tag', LOCAL_IMAGE, REMOTE_IMAGE])
subprocess.run(['docker', 'push', REMOTE_IMAGE])