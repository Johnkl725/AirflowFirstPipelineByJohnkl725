FROM apache/airflow:3.0.4
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
RUN apt-get update && apt-get install -y chromium-driver chromium-browser