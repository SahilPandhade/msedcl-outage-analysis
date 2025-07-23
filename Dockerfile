# Dockerfile

FROM apache/airflow:2.9.1

# Switch to airflow user early
USER airflow

# Copy requirements into image
COPY --chown=airflow:root requirements.txt .

# Install packages in airflow's home
RUN pip install --no-cache-dir -r requirements.txt
