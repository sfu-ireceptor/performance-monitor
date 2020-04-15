
### Running the script

     DATA PROVENANCE TEST 

     usage: AIRR-repertoire-checks.py [-h] [-v]
                                      mapping_file base_url entry_point json_files
                                      master_md study_id

     positional arguments:
       mapping_file   Indicate the full path to where the mapping file is found
       base_url       String containing URL to API server (e.g. https://airr-
                      api2.ireceptor.org)
       entry_point    Options: string 'rearragement' or string 'repertoire'
       json_files     Enter full path to JSON queries - repertoire check uses attached no_filters.json
       master_md      Full path to master metadata
       study_id       Study ID

     optional arguments:
       -h, --help     show this help message and exit
       -v, --verbose  Run the program in verbose mode.


### Example

     python3 AIRR-repertoire-checks.py "./AIRR-iReceptorMapping-v1.3-2020-01-16.txt" 'https://ipa4-staging.ireceptor.org/' 'repertoire' "./nofilters.json" "./master_metadata_2019-09-17.xlsx" "PRJNA325416"

### Study_id in each service
##### IPA1 

     'PRJEB1289',
     'PRJEB8745',
     'PRJEB9332',
     'PRJNA188191',
     'PRJNA195543',
     'PRJNA206548',
     'PRJNA229070',
     'PRJNA248411',
     'PRJNA260556',
     'PRJNA275625',
     'PRJNA280743',
     'PRJNA321261',
     'PRJNA368623',
     'SRP001460'
     
##### IPA2 

     'PRJNA330606'

##### IPA3

     'PRJNA312319'

##### IPA4 

     'PRJNA248475',
      'PRJNA315543',
      'PRJNA316033',
      'PRJNA325416',
      'PRJNA356992',
      'PRJNA506151 '
     
##### IPA5

     'PRJNA381394', 
     'PRJNA493983'
