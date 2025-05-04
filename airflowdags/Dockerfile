# Use Astro Runtime base image (Astro CLI expects this)
FROM quay.io/astronomer/astro-runtime:12.8.0

# Switch to root to install system packages if needed
USER root

RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy DAGs, plugins, and include files
COPY dags/ /usr/local/airflow/dags/
COPY plugins/ /usr/local/airflow/plugins/
COPY include/ /usr/local/airflow/include/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

# Switch back to airflow user
USER astro

# Install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Expose Airflow webserver port
EXPOSE 8080

CMD ["/entrypoint.sh"]


