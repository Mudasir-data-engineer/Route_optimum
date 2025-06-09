FROM apache/airflow:2.7.3-python3.11

# Switch to airflow user immediately
USER airflow

# Copy requirements file to a temp location
COPY --chown=airflow:airflow ./requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt
