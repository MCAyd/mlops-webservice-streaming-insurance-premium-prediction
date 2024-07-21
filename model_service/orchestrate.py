##importing operations 
import time
import os
import json

import pandas as pd
import numpy as np

from sklearn.feature_extraction import DictVectorizer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
import xgboost as xgb
from scipy.special import gammaln
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll import scope

import mlflow
from mlflow.tracking import MlflowClient
from mlflow.entities import ViewType
import psycopg2 ##pip install psycopg2-binary

from elasticsearch import Elasticsearch, helpers

from prefect import flow, task

from setup_aws import S3Manager
from setup_aws import DBManager
from setup_aws import EC2Manager
from setup_aws import KinesisManager

dropped_columns = ['Date_start_contract', 'Date_last_renewal', 'Date_next_renewal', 'Lapse', 
                   'Date_lapse', 'Max_products','Distribution_channel', 'Policies_in_force', 
                   'N_claims_history', 'Cost_claims_year', 'N_claims_year', 'Max_policies',
                   'Payment', 'Date_birth', 'Date_driving_licence', 'Year_matriculation', 'N_doors', 
                   'Length', 'Weight', 'Seniority', 'customer_year']

streamed_columns = ['Date_birth', 'Date_driving_licence', 'Type_risk', 'Area', 'Second_driver', 'Power', 
                    'Cylinder_capacity', 'Value_vehicle', 'Type_fuel', 'Year_matriculation']

es_columns = ['ID', 'Date_birth', 'Date_driving_licence', 'av_senior_year', 'R_Claims_history', 
              'policy_stability', 'product_stability', 'claims_per_policy', 'cost_per_claim']

categorical = ['Type_risk', 'Type_fuel']

target = 'Premium'
unique_id = 'ID'

# Download files from S3 and write training configuration
BUCKET_NAME = os.getenv('BUCKET_NAME')
S3Manager.download_files(BUCKET_NAME)
with open('config.json', 'r') as f:
    config = json.load(f)

# Extract parameters
retraining_cycle = config.get('retraining_cycle', 3600)
training_rounds = config.get('training_rounds', 3)
xgboost_rounds = config.get('xgboost_rounds', 200)
early_stopping_condition = config.get('early_stopping_condition', 50)
es_index = config.get('es_index', 'customer_records_db')
model_registry_name = config.get('model_registry_name', 'models_in_production')
search_space_dict = config['search_space']

# Calculate retraining frame
retraining_frame = int(time.time() - retraining_cycle) * 1000

# Parse search space
search_space = {
    'max_depth': scope.int(hp.quniform('max_depth', *search_space_dict['max_depth'])),
    'learning_rate': hp.loguniform('learning_rate', *search_space_dict['learning_rate']),
    'reg_alpha': hp.loguniform('reg_alpha', *search_space_dict['reg_alpha']),
    'reg_lambda': hp.loguniform('reg_lambda', *search_space_dict['reg_lambda']),
    'min_child_weight': hp.loguniform('min_child_weight', *search_space_dict['min_child_weight']),
    'objective': search_space_dict['objective'],
    'seed': search_space_dict['seed']
}

@task
def load_aws_settings():
    try:
        with open('aws_settings.json', 'r') as f:
            settings = json.load(f)
            os.environ["BUCKET_NAME"] = settings["BUCKET_NAME"]
            os.environ["MLFLOW_SERVER_HOST"] = settings["MLFLOW_SERVER_HOST"]
            os.environ["ES_SERVER_HOST"] = settings["ES_SERVER_HOST"]
    except (FileNotFoundError, json.JSONDecodeError):
        print("AWS settings file not found. Please run the setup first.")
        return None

@task
def set_aws():
    # Check if the MLFLOW_SERVER_HOST, ES_SERVER_HOST and BUCKET_NAME environment variables are set
    MLFLOW_SERVER_HOST = os.getenv("MLFLOW_SERVER_HOST")
    ES_SERVER_HOST = os.getenv("ES_SERVER_HOST")
    BUCKET_NAME = os.getenv("BUCKET_NAME")

    if not MLFLOW_SERVER_HOST or not ES_SERVER_HOST:

        dbcreator = DBManager()
        dbcreator.create_postgredb()
        backend_store = dbcreator.get_db_endpoint()

        MLFLOW_SERVER_HOST, ES_SERVER_HOST = EC2Manager.create_instance(backend_store, BUCKET_NAME)

        KINESIS_STREAM_INPUT_NAME = KinesisManager.create_stream_input()
        KINESIS_STREAM_OUTPUT_NAME = KinesisManager.create_stream_output()

        # Save the hosts in a file for later use
        with open('aws_settings.json', 'w') as f:
            json.dump({"BUCKET_NAME": BUCKET_NAME, "MLFLOW_SERVER_HOST": MLFLOW_SERVER_HOST, 
                       "ES_SERVER_HOST": ES_SERVER_HOST, "STREAM_INPUT_NAME": KINESIS_STREAM_INPUT_NAME,
                       "STREAM_OUTPUT_NAME": KINESIS_STREAM_OUTPUT_NAME}, f)

    print(f"Host servers are ready at {MLFLOW_SERVER_HOST} and {ES_SERVER_HOST}")
    # MLFlow tracking server hosted on AWS EC2
    mlflow.set_tracking_uri(f"http://{MLFLOW_SERVER_HOST}:5000")
    mlflow.set_experiment("project-experiment")
    mlflow_client = MlflowClient(tracking_uri=f"http://{MLFLOW_SERVER_HOST}:5000")

    # Elasticsearch server hosted on other AWS EC2 instance
    es = Elasticsearch(f"http://{ES_SERVER_HOST}:9200")

    return mlflow_client, es

@task(retries=3, retry_delay_seconds=2)
def read_data(location: str) -> pd.DataFrame:
    #read from snowflake not in local
    df = pd.read_csv(f'{location}/data.csv', delimiter=';')

    return df

@task
def df_preprocess(df: pd.DataFrame):
    ## data preprocessing for training
    df['Date_start_contract'] = pd.to_datetime(df['Date_start_contract'], format='%d/%m/%Y')
    df['Date_last_renewal'] = pd.to_datetime(df['Date_last_renewal'], format='%d/%m/%Y')
    df['Date_birth'] = pd.to_datetime(df['Date_birth'], format='%d/%m/%Y')
    df['Date_driving_licence'] = pd.to_datetime(df['Date_driving_licence'], format='%d/%m/%Y')
    
    ## feature engineering
    df['customer_year'] = ((df['Date_last_renewal'] - df['Date_start_contract']).dt.days / 365).round(2)
    df['av_senior_year'] = (((df['Seniority']**(2/3)) + (df['customer_year']**2)) / 2).round(2)
    df['customer_age'] = ((df['Date_last_renewal'] - df['Date_birth']).dt.days / 365).round(2)
    df['driving_licence_long'] = ((df['Date_last_renewal'] - df['Date_driving_licence']).dt.days / 365).round(2)
    df['vehicle_age'] = (df['Date_last_renewal'].dt.year - df['Year_matriculation'])
    
    df['policy_stability'] = df['Policies_in_force'] / df['Max_policies']
    df['product_stability'] = df['Policies_in_force'] / df['Max_products']
    df['claims_per_policy'] = df['N_claims_history'] / df['Max_policies']
    df['cost_per_claim'] = np.where(df['N_claims_year'] == 0, 0, df['Cost_claims_year'] / df['N_claims_year'])
    
    df_ = df.copy()

    for column in dropped_columns:
        df = df.drop(column, axis=1)

    df = df.drop(unique_id, axis=1)
    df[categorical] = df[categorical].astype(str)

    return df, df_

@task
def train_val_split(df: pd.DataFrame):
    premiums = df[target]
    df = df.drop(target, axis=1)
    
    x_train, x_temp, y_train, y_temp = train_test_split(df, premiums, test_size=0.25, random_state=42)
    x_val, x_test, y_val, y_test = train_test_split(x_temp, y_temp, test_size=0.5, random_state=42)
    
    return x_train, x_val, x_test, y_train, y_val, y_test

@task
def prepare_dicts(train:pd.DataFrame, val:pd.DataFrame, test:pd.DataFrame):
    dict_train = train.to_dict(orient='records')
    dict_val = val.to_dict(orient='records')
    dict_test = test.to_dict(orient='records')
    
    return dict_train, dict_val, dict_test

@task(log_prints=True)
def hyperparameter_tuning(
    dict_train,
    dict_val,
    y_train: np.ndarray,
    y_val: np.ndarray,
) -> None:

    def objective(params):
        with mlflow.start_run():
            mlflow.set_tag("model", "xgboost")
            mlflow.log_params(params)

            vectorizer = DictVectorizer(sparse=False)
            X_train = vectorizer.fit_transform(dict_train)
            X_val = vectorizer.transform(dict_val)

            model = xgb.XGBRegressor(**params,
                                    n_estimators = xgboost_rounds,
                                    early_stopping_rounds = early_stopping_condition
            )

            model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
            y_pred = model.predict(X_val)

            nll = gamma_negative_log_likelihood(y_val, y_pred)
            print(params, nll)
            mlflow.log_metric('gamma-nll', nll)

            # Log the vectorizer and model as a pipeline
            pipeline = make_pipeline(vectorizer, model)

            mlflow.sklearn.log_model(pipeline, artifact_path="model")     
    
        return {'loss': nll, 'status': STATUS_OK}

    best_result = fmin(
        fn=objective,
        space=search_space,
        algo=tpe.suggest,
        max_evals=training_rounds,
        trials=Trials()
    )

    return best_result

@task(log_prints=True)
def tag_best_model(mlflow_client, dict_test, y_test):
    run_best_model_uri = get_best_model_uri(mlflow_client)

    #New model performance
    new_model_performance = get_test_result(run_best_model_uri, dict_test, y_test)

    try:
        production_models = mlflow_client.get_latest_versions(model_registry_name, stages=["Production"])
    except mlflow.exceptions.MlflowException:
        production_models = []

    try:
        staging_models = mlflow_client.get_latest_versions(model_registry_name, stages=["Staging"])
    except mlflow.exceptions.MlflowException:
        staging_models = []
    
    if not production_models and not staging_models:
        # Add initial run as in production stage
        print(f"Initial run with test result {new_model_performance}, put into production stage")
        new_version = mlflow.register_model(model_uri=run_best_model_uri, name=model_registry_name)
        mlflow_client.transition_model_version_stage(name=model_registry_name, version=new_version.version, stage="Production")
        
        current_production_model = mlflow_client.get_latest_versions(model_registry_name, stages=["Production"])
        get_final_model_uri(current_production_model)

        return
    
    if production_models and not staging_models:
        current_production_model = production_models[0]
        current_production_uri = current_production_model.source
        #Current in production model performance
        current_production_performance = get_test_result(current_production_uri, dict_test, y_test)
        
        # Performance comparison
        if new_model_performance < current_production_performance:
            # New model better, put production stage, current one into staging.
            print(f"New model better with test result {new_model_performance}, put into production stage")
            new_version = mlflow.register_model(model_uri=run_best_model_uri, name=model_registry_name)
            mlflow_client.transition_model_version_stage(name=model_registry_name, version=current_production_model.version, stage="Staging")
            mlflow_client.transition_model_version_stage(name=model_registry_name, version=new_version.version, stage="Production")

        else:
            # New model worse, put staging stage
            print(f"New model worse with test result {new_model_performance}, put into staging stage")
            new_version = mlflow.register_model(model_uri=run_best_model_uri, name=model_registry_name)
            mlflow_client.transition_model_version_stage(name=model_registry_name, version=new_version.version, stage="Staging")
        
        last_production_model = mlflow_client.get_latest_versions(model_registry_name, stages=["Production"])
        get_final_model_uri(last_production_model)

        return
    
    if production_models and staging_models:
        current_production_model = production_models[0]
        current_production_uri = current_production_model.source
        #Current in production model performance
        current_production_performance = get_test_result(current_production_uri, dict_test, y_test)
        
        current_staging_model = staging_models[0]
        current_staging_uri = current_staging_model.source
        #Current in staging model performance
        current_staging_performance = get_test_result(current_staging_uri, dict_test, y_test)
        
        if new_model_performance < current_production_performance:
            # New model is the best amongst models in production and staging stage
            print(f"New model is the best with test result {new_model_performance}, put into production stage")
            new_version = mlflow.register_model(model_uri=run_best_model_uri, name=model_registry_name)
            mlflow_client.transition_model_version_stage(name=model_registry_name, version=new_version.version, stage="Production")
            mlflow_client.transition_model_version_stage(name=model_registry_name, version=current_production_model.version, stage="Staging")
            mlflow_client.transition_model_version_stage(name=model_registry_name, version=current_staging_model.version, stage="None")
        elif new_model_performance < current_staging_performance:
            # New model is only better than model in staging
            print(f"New model is only better than current staging model with test result {new_model_performance}, put into staging stage")
            new_version = mlflow.register_model(model_uri=run_best_model_uri, name=model_registry_name)
            mlflow_client.transition_model_version_stage(name=model_registry_name, version=new_version.version, stage="Staging")
            mlflow_client.transition_model_version_stage(name=model_registry_name, version=current_staging_model.version, stage="None")
        else:
            # New model is worst, put into none stage
            print(f"New model is the worst with test result {new_model_performance}, to none stage")
            new_version = mlflow.register_model(model_uri=run_best_model_uri, name=model_registry_name)
            mlflow_client.transition_model_version_stage(name=model_registry_name, version=new_version.version, stage="None")

        last_production_model = mlflow_client.get_latest_versions(model_registry_name, stages=["Production"])
        get_final_model_uri(last_production_model)

        return

@task(log_prints=True)
def get_best_model_uri(mlflow_client):
    
    runs = mlflow_client.search_runs(
        experiment_ids='1',
        run_view_type=ViewType.ACTIVE_ONLY
    )

    # Filter runs that started within the retraining frame
    recent_runs = [run for run in runs if run.info.start_time >= retraining_frame]

    # Sort the filtered runs by the desired metric
    sorted_runs = sorted(recent_runs, key=lambda run: run.data.metrics.get('gamma-nll', float('inf')))

    if sorted_runs:
        best_run = sorted_runs[0]
        best_model_uri = best_run.info.artifact_uri
        return best_model_uri
    else:
        return None
    
@task(log_prints=True)
def get_test_result(model_uri, dict_test, y_test):
    
    logged_model = f"""{model_uri}/model"""
    loaded_model = mlflow.pyfunc.load_model(logged_model)

    #test data will be added
    y_pred = loaded_model.predict(dict_test)

    nll = gamma_negative_log_likelihood(y_test, y_pred)

    return nll

@task(log_prints=True)
def get_final_model_uri(production_model):
    production_uri = production_model[0].source
    print(f"URI of the best identified model is saved to json file, {production_uri}")

    # Write the updated JSON data back to the file
    with open('production_uri.json', 'w') as f:
        json.dump({"PRODUCTION_URI": production_uri}, f)

    return

@task(log_prints=True)
def send_json_tos3():
    BUCKET_NAME = os.getenv("BUCKET_NAME")

    if not BUCKET_NAME:
        with open('aws_settings.json', 'r') as f:
            settings = json.load(f)
            os.environ["BUCKET_NAME"] = settings["BUCKET_NAME"]

        BUCKET_NAME = os.getenv("BUCKET_NAME")

    print(f"JSON file uploaded to S3 {BUCKET_NAME} for later use in AWS Lambda")
    S3Manager.upload_files(BUCKET_NAME)

    return

@task
def prepare_elastic_db(df: pd.DataFrame, es):
    df_es = df[es_columns]
    df_es = df_es.groupby(unique_id).last().reset_index()

    index_settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "ID": {"type": "keyword"} 
        }
    }
    }

    # Check if the index exists
    if not es.indices.exists(index=es_index):
        es.indices.create(index=es_index, body=index_settings)

    def dataframe_to_es(df, es_index):
        for df_idx, line in df.iterrows():
            yield {
                "_index": es_index,
                "_id":df_idx,
                "_source" : {
                    "ID": line.iloc[0],
                    "Date_birth": line.iloc[1],
                    "Date_driving_licence": line.iloc[2],
                    "av_senior_year": line.iloc[3],
                    "R_Claims_history": line.iloc[4],
                    "policy_stability": line.iloc[5],
                    "product_stability": line.iloc[6],
                    "claims_per_policy": line.iloc[7],
                    "cost_per_claim": line.iloc[8]
                }
            }
        
    helpers.bulk(es, dataframe_to_es(df_es, es_index), raise_on_error=False)

    return

@flow
def main_flow() -> None:
    #setup aws
    load_aws_settings()
    mlflow_client, es = set_aws()

    #read and preprocess
    read_df = read_data('dataset')
    df, df_ = df_preprocess(read_df)

    #create elasticsearch db
    prepare_elastic_db(df_, es)

    #training operation
    x_train, x_val, x_test, y_train, y_val, y_test = train_val_split(df)
    dict_train, dict_val, dict_test = prepare_dicts(x_train, x_val, x_test)
    hyperparameter_tuning(dict_train, dict_val, y_train, y_val)

    #tagging and productionase best model
    tag_best_model(mlflow_client, dict_test, y_test)

    #send production_uri.json file to s3 bucket for later use in AWS Lambda
    send_json_tos3()

##utility function
def gamma_negative_log_likelihood(y_true, y_pred):
    # y_pred are the predicted rates (inverse scale parameter)
    shape = 2.0  # Assumed shape parameter for simplicity; this can also be learned
    rate = y_pred  # Rate parameter (predicted)
    log_likelihood = shape * np.log(rate) + (shape - 1) * np.log(y_true) - rate * y_true - gammaln(shape)
    return -np.mean(log_likelihood)

if __name__ == "__main__":
    print(f"Prefect flow scheduled to run each intervals of {retraining_cycle} seconds")
    main_flow.serve(name="model-service-deployment", interval=retraining_cycle)