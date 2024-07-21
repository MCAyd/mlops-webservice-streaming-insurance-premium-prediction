FROM python:3.11.6-slim

RUN pip install -U pip

# Build time argument
ARG BUCKET_NAME

# Set environment variable
ENV BUCKET_NAME=${BUCKET_NAME}

# Set the working directory to root's home directory
WORKDIR /root

# Copy requirements.txt from the current directory
COPY requirements.txt ./requirements.txt

# Copy dataset, will be removed setting after Snowflake
COPY dataset ./dataset

RUN pip install -r requirements.txt

COPY [ "orchestrate.py", "./" ]

COPY [ "setup_aws.py", "./" ]

CMD ["sh", "-c", "prefect cloud login -k ${PREFECT_CLOUD_API_KEY} && prefect server start & sleep 10 && python orchestrate.py"]
