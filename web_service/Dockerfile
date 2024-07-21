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

RUN pip install -r requirements.txt

COPY static ./static

COPY templates ./templates

COPY [ "entry_forms.py", "./" ]

COPY [ "routes.py", "./" ]

# Default command to run the routes.py script
CMD ["python", "./routes.py"]
