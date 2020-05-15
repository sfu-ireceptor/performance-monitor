
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
