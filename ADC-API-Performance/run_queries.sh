#!/bin/bash

######### PERFORMANCE TESTING BASH SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: July 10
######### LAST MODIFIED ON: Nov 4, 2019
######### This script is to be run periodically to track query performance of ADC API

TIME1=`date +%Y-%m-%d_%H-%M-%S`

echo "Current working directory: `pwd`"
echo "Starting run at: " ${TIME1}


# ---------------------------------------------------------------------

echo "Begin Script"
echo " "

cd /home/ubuntu/ireceptor-monitor/ADC_API_Testing/Scripts/

python3 adc_api_performancetest.py '/home/ubuntu/ireceptor-monitor/ADC_API_Testing/Results/' 'https://airr-api2.ireceptor.org/airr/v1' 'rearrangement' "/home/ubuntu/ireceptor-monitor/ADC_API_Testing/JSON/"

echo "End Script" 
