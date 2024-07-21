import os
import json
import boto3
import mlflow
import time
import base64
import uuid

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = '~/.aws/credentials'

with open('aws_settings.json', 'r') as f:
    settings = json.load(f)
    os.environ["BUCKET_NAME"] = settings["BUCKET_NAME"]
    os.environ["STREAM_INPUT_NAME"] = settings["STREAM_INPUT_NAME"]
    os.environ["STREAM_OUTPUT_NAME"] = settings["STREAM_OUTPUT_NAME"]

with open('production_uri.json', 'r') as f:
    settings = json.load(f)
    os.environ["PRODUCTION_URI"] = settings["PRODUCTION_URI"]

PRODUCTION_URI = os.getenv("PRODUCTION_URI")
STREAM_INPUT_NAME = os.getenv("STREAM_INPUT_NAME")
STREAM_OUTPUT_NAME = os.getenv("STREAM_OUTPUT_NAME")

kinesis = boto3.client('kinesis', region_name='eu-west-2')

logged_model = f'{PRODUCTION_URI}/model'
model = mlflow.pyfunc.load_model(logged_model)

def predict(features):
    pred = model.predict(features)
    return float(pred[0])

def lambda_handler():
    response = kinesis.describe_stream(StreamName=STREAM_INPUT_NAME)

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    shard_iterator_response = kinesis.get_shard_iterator(
    StreamName=STREAM_INPUT_NAME, 
    ShardId=my_shard_id,
    ShardIteratorType='LATEST')

    shard_iterator = shard_iterator_response['ShardIterator']

    # Get records
    while True:
        response = kinesis.get_records(
            ShardIterator=shard_iterator,
            Limit=1
        )

        if response['Records']:
            latest_record = response['Records'][0]
            print("Latest record: ", latest_record)
            break
        else:
            print("No records found, retrying...")
            time.sleep(1)
        
    response_data = response['Records'][0]['Data']
    data_decoded = base64.b64decode(response_data).decode('utf-8')
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

    print(prediction_event)
    
    kinesis.put_record(
        StreamName=STREAM_OUTPUT_NAME,
        Data=json.dumps(prediction_event),
        PartitionKey='1'
        )


lambda_handler()


