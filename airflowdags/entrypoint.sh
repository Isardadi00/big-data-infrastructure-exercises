#!/bin/bash
set -e

echo "Upgrading DB..."
airflow db upgrade

echo "Syncing Roles..."
airflow sync-perm

sleep 5

echo "Creating admin user..."
airflow users create \
  --role Admin \
  --username admin \
  --firstname Air \
  --lastname Flow \
  --email admin@example.com \
  --password admin

echo "Starting scheduler..."
airflow scheduler &

sleep 5

echo "Starting webserver..."
exec airflow webserver
