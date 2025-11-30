# FROM apache/airflow:3.0.1-python3.12
FROM apache/airflow:3.1.0

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Define the command to run when the container starts
# This example starts the HDFS NameNode and DataNode in pseudo-distributed mode.
CMD ["bash", "-c", "hdfs namenode -format -force && start-dfs.sh && tail -f /dev/null"]

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install ODBC Driver 17 for SQL Server
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl -sSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools && \    
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install --no-cache-dir apache-airflow==3.1.0
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dags/ /opt/airflow/dags/
COPY spark/ /opt/airflow/spark/
COPY resources/ /opt/airflow/resources/
COPY notebooks/ /opt/airflow/notebooks/

RUN airflow db migrate