from curlairripa import *       # https://test.pypi.org/project/curlairripa/ 
import time                     # time stamps
import pandas as pd
import argparse
import os


def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )
    
    # Array with URL
    parser.add_argument(
        "base_url",
        help="String containing URL to API server  (e.g. https://airr-api2.ireceptor.org)"
    )
    # Entry point
    parser.add_argument(
        "entry_point",
        help="Options: string 'rearragement' or string 'repertoire'"
    )
    
    
    parser.add_argument(
            "path_to_json",
        help="Enter full path to JSON query containing repertoire ID's for a given study - this must match the value given for study_id"
    )
    
    # Verbosity flag
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Run the program in verbose mode.")

    # Parse the command line arguements.
    options = parser.parse_args()
    return options


if __name__ == "__main__":

    options = getArguments()
    base_url = options.base_url
    entry_pt = options.entry_point
    query_files = options.json_files
    path_to_json = options.path_to_json
    
    query_url = base_url + "/airr/v1/" + entry_pt
    
    
    # Leave static for now
    expect_pass = True
    verbose = True
    force = True

    # Ensure our HTTP set up has been done.
    initHTTP()
    # Get the HTTP header information (in the form of a dictionary)
    header_dict = getHeaderDict()
    
    # Process json file into JSON structure readable by Python
    query_dict = process_json_files(force,verbose,query_files)
    
    
    # Perform the query. Time it
    start_time = time.time()
    query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
    total_time = time.time() - start_time
    
    
    st_id = pd.json_normalize(json.loads(query_json),record_path="Repertoire")['study.study_id'].unique()
    
    for item in st_id:
        
        os.chdir(path_to_json)
        
        path =  item + "/"
        
        if os.path.exists(path):
            
            continue
        
        else:
            
            os.makedirs(path)
        
        
        rep_ids = pd.json_normalize(json.loads(query_json),record_path="Repertoire")['repertoire_id'].to_list()
        
        for repid in rep_ids:
            
            with open(str(path_to_json) + str(path) + "facet_repertoire_id_" +repid + ".json","w" ) as f:
                f.write('{"filters": {"op": "=", "content": {"field": "repertoire_id", "value": "' + str(repid)  + '"}}, "facets": "repertoire_id"}')
            f.close()