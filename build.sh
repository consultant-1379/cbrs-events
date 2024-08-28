#!/bin/bash
cd ERICDomainProxyEvents_CXP111111/src/main/test
python3 -m unittest

if [ $? -eq 0 ]; then
    cd ../../../..
    chmod 777 ERICDomainProxyEvents_CXP111111/src/main/scripts/cbrs_events
    tar -cvf cbrs_events.tar -C ERICDomainProxyEvents_CXP111111/src/main/scripts cbrs_events  -C ../python resources cbrs_events.py
    mkdir -p target
    mv cbrs_events.tar target
    echo "Build success, find tar in target folder."
else
	  echo "Build fails: There are failing unit tests, (test status $?)."

fi


