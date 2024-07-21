from flask import Flask, request, render_template, url_for, redirect
from flask_wtf.csrf import CSRFProtect
from elasticsearch import Elasticsearch
import time
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

import os
import json
import sys

import entry_forms as entry_forms

app = Flask(__name__)

def load_uris():
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    s3 = boto3.client('s3')
    file_name = 'aws_settings.json'
    try:
        s3.download_file(BUCKET_NAME, file_name, file_name)
        print(f"{file_name} downloaded successfully.")
    except (NoCredentialsError, PartialCredentialsError, ClientError, TypeError) as e:
        print(f"Failed to download {file_name}. Error: {e}")

    try:
        with open('aws_settings.json', 'r') as f:
            settings = json.load(f)
            os.environ["ES_SERVER_HOST"] = settings["ES_SERVER_HOST"]
            os.environ["STREAM_INPUT_NAME"] = settings["STREAM_INPUT_NAME"]
            os.environ["STREAM_OUTPUT_NAME"] = settings["STREAM_OUTPUT_NAME"]
    except (FileNotFoundError):
        print("AWS settings file not found. Please run the setup first.")

def get_customer_info(keyword=None):
    res = es.search(index=es_index, body={
    "query": {
        "bool": {
            "should": [
                {
                    "match": {
                        "ID": keyword
                    }
                }
            ]
        }
    }
    })

    if res['hits']['hits']:
        return res['hits']['hits'][0]['_source']
    
    return None

def parse_dates(date_birth, date_licence, year_matriculation):
    # Reference date
    reference_date_str = "2018-12-31"

    reference_date = datetime.fromisoformat(reference_date_str).date()
    try:
        date_birth = datetime.fromisoformat(date_birth).date()
        date_licence = datetime.fromisoformat(date_licence).date()
    except:
        pass

    # Calculate the differences in days
    days_since_birth = (reference_date - date_birth).days
    days_since_licence = (reference_date - date_licence).days

    # Converting days to years, rounding to 2 decimal places
    years_since_birth = round(days_since_birth / 365, 2)
    years_since_licence = round(days_since_licence / 365, 2)
    vehicle_age = reference_date.year - year_matriculation

    return years_since_birth, years_since_licence, vehicle_age

def put_record(customer_input):
    json_data = json.dumps(customer_input)

    response = kinesis.put_record(
    StreamName=STREAM_INPUT_NAME,
    Data=json_data,
    PartitionKey='1'
    )

    return response

def get_record():
    time.sleep(3)
    
    response = kinesis.describe_stream(StreamName=STREAM_OUTPUT_NAME)

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    timestamp = datetime.utcnow() - timedelta(minutes=5)

    shard_iterator_response = kinesis.get_shard_iterator(
    StreamName=STREAM_OUTPUT_NAME, 
    ShardId=my_shard_id,
    ShardIteratorType='AT_TIMESTAMP',
    Timestamp = timestamp
    )

    shard_iterator = shard_iterator_response['ShardIterator']

    response = kinesis.get_records(
    ShardIterator=shard_iterator
    )

    response_data = response['Records'][-1]['Data']
    decoded_data = response_data.decode('utf-8')  # Decode the byte data to a string

    # Convert the JSON string to a dictionary
    data_dict = json.loads(decoded_data)

    return data_dict

@app.route("/")
def home_page():

    return render_template('index.htm')

@app.route("/newc", methods=['GET', 'POST'])
def get_newc_info():

    form = entry_forms.newCustomerForm(request.form)

    if form.validate_on_submit():

        date_birth = form.date_birth.data
        date_licence = form.date_licence.data
        year_matriculation = form.year_matriculation.data

        years_since_birth, years_since_licence, vehicle_age = parse_dates(date_birth, date_licence, year_matriculation)

        customer_data = {
            'R_Claims_history': 0.0,
            'Type_risk': form.type_risk.data,
            'Area': form.area.data,
            'Second_driver':form.second_driver.data,
            'Power': form.power.data,
            'Cylinder_capacity': form.cylinder_capacity.data,
            'Value_vehicle': form.value_vehicle.data,
            'Type_fuel': form.type_fuel.data,
            'av_senior_year': 0.50,
            'customer_age': years_since_birth,
            'driving_licence_long': years_since_licence,
            'vehicle_age': vehicle_age,
            'policy_stability': 1.0,
            'product_stability': 1.0,
            'claims_per_policy': 0.0,
            'cost_per_claim': 0.0
        }

        put_record(customer_data)

        return redirect(url_for('premium_offer'))
    
    return render_template('newc.htm', form=form)

@app.route("/existingc", methods=['GET', 'POST'])
def get_existingc_info():

    customer_id = request.args.get('customer_id')
    customer_info = get_customer_info(customer_id)

    if not customer_info:
        return redirect(url_for("home_page"))
    
    form = entry_forms.existingCustomerForm(request.form)

    if form.validate_on_submit():

        date_birth = customer_info['Date_birth']
        date_licence = customer_info['Date_driving_licence']
        year_matriculation = form.year_matriculation.data

        years_since_birth, years_since_licence, vehicle_age = parse_dates(date_birth, date_licence, year_matriculation)
        
        customer_data = {
            'R_Claims_history': customer_info['R_Claims_history'],
            'Type_risk': form.type_risk.data,
            'Area': form.area.data,
            'Second_driver':form.second_driver.data,
            'Power': form.power.data,
            'Cylinder_capacity': form.cylinder_capacity.data,
            'Value_vehicle': form.value_vehicle.data,
            'Type_fuel': form.type_fuel.data,
            'av_senior_year': customer_info['av_senior_year'],
            'customer_age': years_since_birth,
            'driving_licence_long': years_since_licence,
            'vehicle_age': vehicle_age,
            'policy_stability': customer_info['policy_stability'],
            'product_stability': customer_info['product_stability'],
            'claims_per_policy': customer_info['claims_per_policy'],
            'cost_per_claim': customer_info['cost_per_claim']
        }

        put_record(customer_data)

        return redirect(url_for('premium_offer'))
    
    return render_template('existingc.htm', customer_info=customer_info, form=form)

@app.route("/premiumoffer", methods=['GET'])
def premium_offer():

    record = get_record()
    premium_amount = round(record['prediction']['premium_prediction'], 2)

    return render_template('premiumoffer.htm', prediction=premium_amount)

if __name__ == '__main__':
    load_uris()
    ES_SERVER_HOST = os.getenv("ES_SERVER_HOST")
    STREAM_INPUT_NAME = os.getenv("STREAM_INPUT_NAME")
    STREAM_OUTPUT_NAME = os.getenv("STREAM_OUTPUT_NAME")

    es = Elasticsearch(f"http://{ES_SERVER_HOST}:9200")
    es_index = "customer_records_db"

    kinesis = boto3.client('kinesis')

    if not ES_SERVER_HOST or not STREAM_INPUT_NAME or not STREAM_OUTPUT_NAME:
        print(f"""Please identify which AWS setting is not setup ES_SERVER_HOST, {ES_SERVER_HOST} // 
              STREAM_INPUT_NAME, {STREAM_INPUT_NAME} // STREAM_OUTPUT_NAME, {STREAM_OUTPUT_NAME}""")
        sys.exit(1)
 
    app.config['SECRET_KEY'] = os.urandom(24)
    csrf = CSRFProtect(app)    
    app.run(host='0.0.0.0', port=8080, debug=True)