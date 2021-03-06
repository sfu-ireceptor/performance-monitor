
### Running the script

Ensure you run `generate_facet_json.py` first to generate the appropriate repertoire_id JSON input files. 

     usage: generate_facet_json.py [-h] [-v] base_url entry_point path_to_json

     positional arguments:
       base_url       String containing URL to API server (e.g. https://airr-
                      api2.ireceptor.org)
       entry_point    Options: string 'rearragement' or string 'repertoire'
       path_to_json   Enter full path to JSON query containing repertoire ID's for
                      a given study - this must match the value given for study_id

     optional arguments:
       -h, --help     show this help message and exit
       -v, --verbose  Run the program in verbose mode.

Example:

     python 3./generate_facet_json.py "http://covid19-2.ireceptor.org" "repertoire" "./JSON-Files/repertoire/nofilters.json"

Once that has been completed, you can then run a repertoire sanity check with the facet JSON as well as the repertoire ID input JSON files. 
     DATA PROVENANCE TEST 

     usage: AIRR-repertoire-checks.py [-h] [-v]
                                      mapping_file base_url entry_point json_files
                                      master_md study_id facet_count annotation_dir
                                      details_dir Coverage

     positional arguments:
       mapping_file    Indicate the full path to where the mapping file is found
       base_url        String containing URL to API server (e.g. https://airr-
                       api2.ireceptor.org)
       entry_point     Options: string 'rearragement' or string 'repertoire'
       json_files      Enter full path to JSON query containing repertoire ID's for
                       a given study - this must match the value given for study_id
       master_md       Full path to master metadata
       study_id        Study ID (study_id) associated to this study
       facet_count     Enter full path to JSON queries containing facet count
                       request for each repertoire
       annotation_dir  Enter full path to where annotation files associated with
                       study_id
       details_dir     Enter full path where you'd like to store content feedback
                       in CSV format
       Coverage        Sanity check levels: enter CC for content comparison, enter
                       FC for facet count vs ir_curator count test, enter AT for
                       AIRR type test

     optional arguments:
       -h, --help      show this help message and exit
       -v, --verbose   Run the program in verbose mode.


### Example

     python3 AIRR-repertoire-checks.py "./MappingFiles/AIRR-iReceptorMapping-latest.txt" "https://ipa1.ireceptor.org" "repertoire" "./JSON-Files/repertoire/PRJNA493983_ipa5.json" "./MetadataFiles/master_metadata_UTF-latest.csv" "PRJEB9332" "./JSON-Files/facet_queries_for_sanity_tests/ipa1/PRJEB9332/" "./annotation/" "./ExtraFeedback/" "CC-FC-AT"

## curlairripa Python Package 

Open a command line, change directories to where the performance script is, and enter the following command

`pip3 install -i https://test.pypi.org/simple/ curlairripa`

Ensure the curlairripa.py file is on the same directory where the adc_api_performancetest.py file is located. 

To use modules in that library:

`from curlairripa import *`

To see more information on the curlairripa Python package, go to [https://test.pypi.org/project/curlairripa/](https://test.pypi.org/project/curlairripa/)

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

