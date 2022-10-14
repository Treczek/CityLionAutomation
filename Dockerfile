FROM apache/airflow:2.4.1-python3.9

# Compulsory to switch parameter
ENV PIP_USER=false

#python venv setup
RUN python3 -m venv /opt/airflow/venv_citylion

# Install dependencies:
COPY requirements.txt .

RUN /opt/airflow/venv_citylion/bin/pip install -r requirements.txt
ENV PIP_USER=true