FROM apache/airflow:2.9.2

#copy files
COPY requirements.txt /requirements.txt
ENV PYTHONPATH="/opt/airflow/dags"
COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts
COPY setup.py /opt/airflow/setup.py
COPY secrets /opt/airflow/secrets
COPY data /opt/airflow/data

# set up working directory
WORKDIR /opt/airflow

# change owner of the directory to airflow user
USER root
RUN chown -R airflow /opt/airflow/scripts

# switch back to airflow user
USER airflow

# install dependencies
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install -e /opt/airflow

