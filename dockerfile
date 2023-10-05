# Use the official Apache Airflow image as the base
FROM apache/airflow:2.6.3

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ARG AIRFLOW_VERSION=2.6.3

# Switch to the 'airflow' user
USER airflow

# Install Apache Airflow and its dependencies as the 'airflow' user
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pandas sqlalchemy psycopg2-binary

# Continue with other Dockerfile commands if needed
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to the 'root' user for other installations
USER root

# Install additional packages as needed
RUN apt-get update -qq && apt-get install -y vim

# Set the shell for the following commands
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Install Google Cloud SDK
ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# Set the working directory for Apache Airflow
WORKDIR $AIRFLOW_HOME

# Copy scripts and make them executable (adjust as needed)
COPY scripts scripts
RUN chmod +x scripts

# Set the user to the 'airflow' user defined in the base image
USER $AIRFLOW_UID
