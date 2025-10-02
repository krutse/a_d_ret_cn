#!/bin/bash

# path to this script
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# Define the user for whom the cron job should be set
CRON_USER="root"

# script to get data
# Define the cron job to be added
CRON_JOB="0 3 * * 1-6 $SCRIPT_DIR/venv/python $SCRIPT_DIR/sales_data_gen.py >> /var/log/sales_data_gen.log 2>&1"

# Check if a crontab exists for the user
if crontab -u "$CRON_USER" -l 2>/dev/null; then
    # Crontab exists, check if the job is already present
    if ! crontab -u "$CRON_USER" -l | grep -Fq "$CRON_JOB"; then
        echo "Adding cron job for user $CRON_USER: $CRON_JOB"
        (crontab -u "$CRON_USER" -l; echo "$CRON_JOB") | crontab -u "$CRON_USER" -
    else
        echo "Cron job already exists for user $CRON_USER: $CRON_JOB"
    fi
else
    # No crontab exists, create a new one with the job
    echo "No crontab found for user $CRON_USER. Creating a new one with: $CRON_JOB"
    echo "$CRON_JOB" | crontab -u "$CRON_USER" -
fi


# script to load data
# Define the cron job to be added
CRON_JOB="0 4 * * * $SCRIPT_DIR/venv/python $SCRIPT_DIR/sales_data_load.py >> /var/log/sales_data_load.log 2>&1"

if crontab -u "$CRON_USER" -l 2>/dev/null; then
    # Crontab exists, check if the job is already present
    if ! crontab -u "$CRON_USER" -l | grep -Fq "$CRON_JOB"; then
        echo "Adding cron job for user $CRON_USER: $CRON_JOB"
        (crontab -u "$CRON_USER" -l; echo "$CRON_JOB") | crontab -u "$CRON_USER" -
    else
        echo "Cron job already exists for user $CRON_USER: $CRON_JOB"
    fi
else
    # No crontab exists, create a new one with the job
    echo "No crontab found for user $CRON_USER. Creating a new one with: $CRON_JOB"
    echo "$CRON_JOB" | crontab -u "$CRON_USER" -
fi
