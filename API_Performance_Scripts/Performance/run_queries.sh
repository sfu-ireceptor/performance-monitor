#!/bin/bash

# This script is to be run periodically to track query performance of production services ipa1,ipa2,ipa3,ipa4,ipa5 
#  using crontab -e (or other cronjob software)
#  This script runs queries for a single service 
#  Service example: https://ipa1.ireceptor.org/ 

TIME1=`date +%Y-%m-%d_%H-%M-%S`

echo "Current working directory: `pwd`"
echo "Starting run at: " ${TIME1}


# ---------------------------------------------------------------------

echo "Begin Script"
echo " "

cd /home/ubuntu/ireceptor-monitor/PerformanceTesting/Scripts/ 

python3 queries.py /home/ubuntu/ireceptor-monitor/PerformanceTesting/SimResults/ "$@"

echo "End Script" 
