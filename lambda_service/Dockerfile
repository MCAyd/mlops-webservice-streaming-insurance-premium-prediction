FROM public.ecr.aws/lambda/python:3.11

RUN pip install -U pip

# Build time argument
ARG BUCKET_NAME

# Set environment variable
ENV BUCKET_NAME=${BUCKET_NAME}

# Copy requirements.txt from the current directory
COPY requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

COPY [ "lambda_function.py", "./" ]

CMD [ "lambda_function.lambda_handler" ]
