
### Running the script
     DATA PROVENANCE TEST 

    usage: adc_api_datatest.py [-h] [-v]
                               output_dir words base_url entry_point json_files
                               rep_id

    positional arguments:
      output_dir     Indicate the full path to where data provenance test results
                     will be stored
      words          Comma-separated strings containing odd words to search for
      base_url       String containing URL to API server (e.g. https://airr-
                     api2.ireceptor.org)
      entry_point    Options: string 'rearragement' or string 'repertoire'
      json_files     Enter full path to JSON queries
      rep_id         Repertoire id

    optional arguments:
      -h, --help     show this help message and exit
      -v, --verbose  Run the program in verbose mode.


### Example

     python3 adc_api_datatest.py "./ADC-API/output/ipa1/" "Musmus" "http://ipa1.ireceptor.org" "rearrangement" "./ADC-API/JSON-Files/ipa1/PRJEB1289_repertoire_id_10.json 10
     
## curlairripa Python Package 

Open a command line, change directories to where the performance script is, and enter the following command

`pip3 install -i https://test.pypi.org/simple/ curlairripa`

Ensure the curlairripa.py file is on the same directory where the adc_api_performancetest.py file is located. 

To use modules in that library:

`from curlairripa import *`

To see more information on the curlairripa Python package, go to [https://test.pypi.org/project/curlairripa/](https://test.pypi.org/project/curlairripa/)
