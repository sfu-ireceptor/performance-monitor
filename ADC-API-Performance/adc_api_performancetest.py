######### PERFORMANCE TESTING PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: October 31, 2019
######### LAST MODIFIED ON: November 4, 2019

from curlairripa import *       # https://test.pypi.org/project/curlairripa/ 
import time                     # time stamps
import pandas as pd
from pandas.io.json import json_normalize # parse JSON response into pandas df 
import argparse                 # Input parameters from command line 

def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Output Directory - where Performance test results will be stored 
    parser.add_argument(
        "base",
        help="Indicate the full path to where performance test results will be stored."
    )
    # Array with URL
    parser.add_argument(
        "ipa_arr",
        help="Array containing URL to API server."
    )
    # Entry point
    parser.add_argument(
        "entry_point",
        help="Options: 'rearragement' or 'repertoire'."
    )
    
    parser.add_argument(
            "json_files",
        help="Enter the names of JSON files with query field name and value."
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
    # Input reading
    options = getArguments()
    output_dir = options.base
    base_url = options.ipa_arr
    entry_pt = options.entry_point
    query_files = options.json_files
    query_url = base_url + "/" + entry_pt
    
    # Leave static for now
    expect_pass = True
    verbose = True
    force = True

    # Ensure our HTTP set up has been done.
    initHTTP()
    # Get the HTTP header information (in the form of a dictionary)
    header_dict = getHeaderDict()
    
    # Get timestampt
    dates = pd.to_datetime('today')
    dates_str = str(dates)
    date_f = dates_str.split(" ")[0]
    time_f = dates_str.split(" ")[1].split(".")[0]
    downloaded_at = date_f + "_"+time_f
    
    # Load file
    query_dict = process_json_files(force,verbose,query_files)
    
    # Tunr response into pandas dataframe 
    json_response_df = json_normalize(query_dict)
    
    
    # Perform the query. Time it
    start_time = time.time()
    query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
    total_time = time.time() - start_time
    # Turn query response into pandas dataframe
    norm_facets = json_normalize(query_json['Facet'])
    # Append results 
    json_response_df["TimeTaken(s)"] = total_time
    json_response_df["NumberRepertoire"] = len(norm_facets)
    json_response_df["RepertoireCount"] = sum(norm_facets["count"])
    json_response_df["Date/Time"] = dates
    json_response_df['Date/TimeConverted'] = json_response_df['Date/Time'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')

    # Save into CSV for later visualizing 
    json_response_df.to_csv(output_dir + "_PerformanceTesting_" + str(date_f) + "_" + str(time_f) + "_Query_Times_" + str(base_url.split("//")[1].split(".")[0]) + "_" + str(base_url.split("/")[-1]) + ".csv",sep=",")
    
    
