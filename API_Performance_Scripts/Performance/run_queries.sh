#!/bin/bash

######### PERFORMANCE TESTING BASH SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: July 10
######### LAST MODIFIED ON: July 10, 2019
######### This script is to be run periodically to track query performance of production servers ipa1,ipa2,ipa3,ipa4,ipa5 

TIME1=`date +%Y-%m-%d_%H-%M-%S`

echo "Current working directory: `pwd`"
echo "Starting run at: " ${TIME1}


# ---------------------------------------------------------------------

echo "Begin Script"
echo " "

cd /home/ubuntu/ireceptor-monitor/PerformanceTesting/Scripts/ 

python3 queries.py /home/ubuntu/ireceptor-monitor/PerformanceTesting/SimResults/ "$@"

echo "End Script" 
