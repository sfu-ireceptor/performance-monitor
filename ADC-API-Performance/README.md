# Scripts

## ADC API Performance Testing Script 

### Sample Usage

`python3 adc_api_performancetest.py '/home/ubuntu/ireceptor-monitor/ADC_API_Testing/Results/' 'https://airr-api2.ireceptor.org/airr/v1' 'rearrangement' "/home/ubuntu/ireceptor-monitor/ADC_API_Testing/JSON/"
`

## curlairripa Python Package 

To setup the curlairripa Python package, go to [https://test.pypi.org/project/curlairripa/](https://test.pypi.org/project/curlairripa/)

Open a command line, change directories to where the performance script is, and enter the following command

`pip3 install -i https://test.pypi.org/simple/ curlairripa`

Ensure the curlairripa.py file is on the same directory. 

To use modules in that library:


`from curlairripa import *`
