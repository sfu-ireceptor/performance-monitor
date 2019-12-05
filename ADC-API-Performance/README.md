# Scripts

## ADC API Performance Testing Script 

#### Usage

`python3 adc_api_performancetest.py -h
`

    usage: adc_performance_testing.py [-h] [-v]
                                      base ipa_arr entry_point json_files

    positional arguments:
      base           Indicate the full path to where performance test results will be stored 
      ipa_arr        Array containing URL to API server (e.g. https://airr-api2.ireceptor.org)
      entry_point    Options: string 'rearragement' or string 'repertoire'
      json_files     Enter full path to JSON queries 
      
    optional arguments:
      -h, --help     show this help message and exit
      -v, --verbose  Run the program in verbose mode.

## curlairripa Python Package 

To setup the curlairripa Python package, go to [https://test.pypi.org/project/curlairripa/](https://test.pypi.org/project/curlairripa/)

Open a command line, change directories to where the performance script is, and enter the following command

`pip3 install -i https://test.pypi.org/simple/ curlairripa`

Ensure the curlairripa.py file is on the same directory. 

To use modules in that library:


`from curlairripa import *`
