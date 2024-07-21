import os
import json
import boto3
import mlflow
import base64
import uuid

BUCKET_NAME = os.getenv('BUCKET_NAME')
s3 = boto3.client('s3')

uri_local_file_path = '/tmp/production_uri.json'
os.makedirs(os.path.dirname(uri_local_file_path), exist_ok=True)
s3.download_file(BUCKET_NAME, 'production_uri.json', uri_local_file_path)

with open(uri_local_file_path, 'r') as f:
    settings = json.load(f)
    os.environ["PRODUCTION_URI"] = settings["PRODUCTION_URI"]

aws_local_file_path = '/tmp/aws_settings.json'
os.makedirs(os.path.dirname(aws_local_file_path), exist_ok=True)
s3.download_file(BUCKET_NAME, 'aws_settings.json', aws_local_file_path)

with open(aws_local_file_path, 'r') as f:
    settings = json.load(f)
    os.environ["STREAM_OUTPUT_NAME"] = settings["STREAM_OUTPUT_NAME"]

PRODUCTION_URI = os.getenv("PRODUCTION_URI")
STREAM_OUTPUT_NAME = os.getenv("STREAM_OUTPUT_NAME")

logged_model = f'{PRODUCTION_URI}/model'
model = mlflow.pyfunc.load_model(logged_model)

kinesis = boto3.client('kinesis')

def predict(features):
    pred = model.predict(features)
    return float(pred[0])

def lambda_handler(event, context):

    predictions_events = []

    print(f"Received event: {event}")
     
    for record in event['Records']:

        data_encoded = record['kinesis']['data']
        # print(f"Data encoded: {data_encoded}")

        data_decoded = base64.b64decode(data_encoded).decode('utf-8')
        # print(f"Data decoded: {data_decoded}")
        
        data_dict = json.loads(data_decoded)

        prediction = predict(data_dict)

        prediction_event = {
        'model': PRODUCTION_URI,
        'version': 'Production Version',
        'prediction': {
            'premium_prediction': prediction,
            'prediction_uid': str(uuid.uuid4())   
        }
        }

        kinesis.put_record(
            StreamName=STREAM_OUTPUT_NAME,
            Data=json.dumps(prediction_event),
            PartitionKey='1'
            )
        
        predictions_events.append(prediction_event)
        
    return {
        'predictions': predictions_events
    }
