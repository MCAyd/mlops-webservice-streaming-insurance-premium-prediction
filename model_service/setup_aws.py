import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import time

ec2 = boto3.resource('ec2')
rds = boto3.client('rds')
s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')

class KinesisManager:

    @staticmethod
    def create_stream_input(stream_name="kinesis_stream_input"):
        kinesis.create_stream(
            StreamName=stream_name,
            ShardCount=1,
            StreamModeDetails={
                'StreamMode': 'PROVISIONED'
            })
        
        print(f"Kinesis Input Stream is created with name {stream_name}")
        return stream_name

    @staticmethod
    def create_stream_output(stream_name="kinesis_stream_output"):
        kinesis.create_stream(
            StreamName=stream_name,
            ShardCount=1,
            StreamModeDetails={
                'StreamMode': 'PROVISIONED'
            })

        print(f"Kinesis Output Stream is created with name {stream_name}")
        return stream_name

class S3Manager:

    @staticmethod
    def upload_files(bucket_name, file_names=["production_uri.json", "aws_settings.json"]):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        for file_name in file_names:
            try:
                json_file_path = os.path.join(current_dir, file_name)
                s3.upload_file(json_file_path, bucket_name, file_name)
                print(f"{file_name} uploaded successfully.")
            except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
                print(f"Failed to upload {file_name}. Error: {e}")

    @staticmethod
    def download_files(bucket_name, file_names=["config.json", "aws_settings.json"]):
        for file_name in file_names:
            try:
                s3.download_file(bucket_name, file_name, file_name)
                print(f"{file_name} downloaded successfully.")
            except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
                print(f"Failed to download {file_name}. Error: {e}")


class DBManager:
    def __init__(self):
        self.db_identifier = "mlflow-db"
        self.db_name = "mlflow_db"
        self.master_username = "mlflow_user"
        self.password = "12345678"
        self.security_group = ["sg-0e87110c851b8e69c"] ##postqresql security group id

    def create_postgredb(self):
        try:
            response = rds.create_db_instance(
                DBInstanceIdentifier=self.db_identifier,
                AllocatedStorage=20,
                DBName=self.db_name,
                Engine='postgres',
                StorageType='gp2',
                StorageEncrypted=True,
                AutoMinorVersionUpgrade=True,
                MultiAZ=False,
                MasterUsername=self.master_username,
                MasterUserPassword=self.password,
                VpcSecurityGroupIds=self.security_group,
                DBInstanceClass='db.t3.micro'
            )
            print(f"Starting RDS instance with ID {self.db_identifier}")
            return response
        except Exception as e:
            print(f"Error creating DB instance: {e}")
            return None
        
    def get_db_endpoint(self):
        # Wait for the instance to be available
        while True:
            response = rds.describe_db_instances(DBInstanceIdentifier=self.db_identifier)
            status = response['DBInstances'][0]['DBInstanceStatus']
            if status == 'available':
                endpoint = response['DBInstances'][0]['Endpoint']['Address']
                port = response['DBInstances'][0]['Endpoint']['Port']
                print(f"RDS instance is available. Endpoint: {endpoint}, Port: {port}")
                time.sleep(60)
                return f"postgresql://{self.master_username}:{self.password}@{endpoint}:{port}/{self.db_name}"
            else:
                print(f"Waiting for RDS instance to be available. Current status: {status}")
                time.sleep(60)  # Wait for 60 seconds before checking again
        
class EC2Manager:

    def create_instance(backend_store, artifact_root):

        user_data_mlflow = f"""#!/bin/bash
        sudo yum update
        pip3 install mlflow
        pip3 install boto3
        pip3 install psycopg2-binary
        pip3 install 'urllib3<2.0'
        mlflow server -h 0.0.0.0 -p 5000 --backend-store-uri {backend_store} --default-artifact-root s3://{artifact_root}
        """

        user_data_es = f"""#!/bin/bash
        sudo yum update
        sudo yum install -y polkit docker
        sudo yum install -y docker
        sudo service docker start
        sudo docker run \
            --rm \
            --name elasticsearch \
            -p 9200:9200 \
            -p 9300:9300 \
            -e "discovery.type=single-node" \
            -e "xpack.security.enabled=false" \
            docker.elastic.co/elasticsearch/elasticsearch:8.14.2
            """

        instances_mlflow = ec2.create_instances(
            ImageId = 'ami-079bd1a083298389f', # Amzn Lnx 2 AMI – update to latest ID
            MinCount = 1,
            MaxCount = 1,
            InstanceType = 't2.micro',
            KeyName = 'ec2_instance',
            SecurityGroups=['mlflow'], #new security group that allows ssh traffic 5000,   
            UserData=user_data_mlflow # and user-data
            )
        
        instances_es = ec2.create_instances(
            ImageId = 'ami-079bd1a083298389f', # Amzn Lnx 2 AMI – update to latest ID
            MinCount = 1,
            MaxCount = 1,
            InstanceType = 't2.small',
            KeyName = 'ec2_instance',
            SecurityGroups=['mlflow'], #new security group that allows ssh traffic 9200,   
            UserData=user_data_es # and user-data
            )
        
        time.sleep(90)

        instance_mlflow = instances_mlflow[0]
        instance_es = instances_es[0]

        time.sleep(90)

        instance_mlflow.reload()
        instance_es.reload()

        print(f"Instances are created with public dns {instance_mlflow.public_dns_name}, {instance_es.public_dns_name}")

        return instance_mlflow.public_dns_name, instance_es.public_dns_name