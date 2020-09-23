# Scripts

## ADC API Performance Testing Script 

This script performs queries (provided by the user) on a given service. 

#### Usage

`python3 adc_performancetest.py -h
`

    usage: adc_performance_testing.py [-h] [-v]
                                  base ipa_arr entry_point json_files

    positional arguments:
      base           Indicate the full path to where performance test results will
                     be stored
      ipa_arr        String containing URL to API server (e.g. https://airr-
                     api2.ireceptor.org)
      entry_point    Options: string 'rearragement' or string 'repertoire'
      json_files     Enter full path to JSON queries

    optional arguments:
      -h, --help     show this help message and exit
      -v, --verbose  Run the program in verbose mode.
      
## ADC API Performance Plotting Script 

This script plots performance test results from the queries obtained using the script above.

#### Usage

    usage: plot_performance.py [-h] [-v] [-s] path s_date e_date

    positional arguments:
      path
      s_date
      e_date

    optional arguments:
      -h, --help     show this help message and exit
      -v, --verbose  Run the program in verbose mode.
      -s, --scale    Scale the graphs so they are all on the same y-axis scale.

## curlairripa Python Package 

Open a command line, change directories to where the performance script is, and enter the following command

`pip3 install -i https://test.pypi.org/simple/ curlairripa`

Ensure the curlairripa.py file is on the same directory where the adc_api_performancetest.py file is located. 

To use modules in that library:

`from curlairripa import *`

To see more information on the curlairripa Python package, go to [https://test.pypi.org/project/curlairripa/](https://test.pypi.org/project/curlairripa/)
