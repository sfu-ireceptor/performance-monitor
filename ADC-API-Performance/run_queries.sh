#!/bin/bash

# This script is to be run periodically to track query performance of ADC API serice 
#  using crontab -e (or other cronjob software)
#  This script runs queries for a single service 
#  Service example: https://airr-api2.ireceptor.org

TIME1=`date +%Y-%m-%d_%H-%M-%S`

echo "Current working directory: `pwd`"
echo "Starting run at: " ${TIME1}


# ---------------------------------------------------------------------

echo "Begin Script"
echo " "

cd /home/ubuntu/ireceptor-monitor/ADC_API_Testing/Scripts/

python3 adc_api_performancetest.py '/home/ubuntu/ireceptor-monitor/ADC_API_Testing/Results/' 'https://airr-api2.ireceptor.org' 'rearrangement' "/home/ubuntu/ireceptor-monitor/ADC_API_Testing/JSON/"

echo "End Script" 
