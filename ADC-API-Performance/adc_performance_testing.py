######### PERFORMANCE TESTING PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: September 20, 2019
######### LAST MODIFIED ON: November 4, 2019

import urllib.request, urllib.parse
import argparse
import json
import os, ssl
import time
import pandas as pd
from pandas.io.json import json_normalize
#from curlairripa import *

def processQuery(query_url, header_dict, expect_pass, query_dict={}, verbose=False, force=False):
    # Build the required JSON data for the post request. The user
    # of the function provides both the header and the query data

    # Convert the query dictionary to JSON
    query_json = json.dumps(query_dict)

    # Encode the JSON for the HTTP requqest
    query_json_encoded = query_json.encode('utf-8')

    # Try to connect the URL and get a response. On error return an
    # empty JSON array.
    try:
        # Build the request
        request = urllib.request.Request(query_url, query_json_encoded, header_dict)
        # Make the request and get a handle for the response.
        response = urllib.request.urlopen(request)
        # Read the response
        url_response = response.read()
        # If we have a charset for the response, decode using it, otherwise assume utf-8
        if not response.headers.get_content_charset() is None:
            url_response = url_response.decode(response.headers.get_content_charset())
        else:
            url_response = url_response.decode("utf-8")

    except urllib.error.HTTPError as e:
        if not expect_pass:
            if e.code == 400:
                # correct failure
                return json.loads('[400]')
        print('ERROR: Server could not fullfil the request to ' + query_url)
        print('ERROR: Error code = ' + str(e.code) + ', Message = ', e.read())
        return json.loads('[]')
    except urllib.error.URLError as e:
        print('ERROR: Failed to reach the server')
        print('ERROR: Reason =', e.reason)
        return json.loads('[]')
    except Exception as e:
        print('ERROR: Unable to process response')
        print('ERROR: Reason =' + str(e))
        return json.loads('[]')

    # Convert the response to JSON so we can process it easily.
    try:
        json_data = json.loads(url_response)
    except json.decoder.JSONDecodeError as error:
        if force:
            print("WARNING: Unable to process JSON response: " + str(error))
            return json_data
        else:
            print("ERROR: Unable to process JSON response: " + str(error))
            if verbose:
                print("ERROR: JSON = " + url_response)
            return json.loads('[]')
    except Exception as error:
        print("ERROR: Unable to process JSON response: " + str(error))
        if verbose:
            print("ERROR: JSON = " + url_response)
        return json.loads('[]')

    # Return the JSON data
    return json_data


def getHeaderDict():
    # Set up the header for the post request.
    header_dict = {'accept': 'application/json',
                   'Content-Type': 'application/json'}
    return header_dict

def initHTTP():
    # Deafult OS do not have create cient certificate bundles. It is
    # easiest for us to ignore HTTPS certificate errors in this case.
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
        getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context


if __name__ == "__main__":

    
    # Ensure our HTTP set up has been done.
    initHTTP()
    # Get the HTTP header information (in the form of a dictionary)
    header_dict = getHeaderDict()
    # Build the full URL combining the URL and the entry point.
    base_url = 'https://airr-api2.ireceptor.org/airr/v1'
    entry_point = 'rearrangement'
    query_url = base_url + '/' + entry_point
    # Boolean parameters
    expect_pass = True
    verbose = True
    force = True
    # Query files
    query_files = ["performance_junction_aa.json","performance_junction_length_9.json","performance_junction_length_15.json"]
    # Initialize query time result df
    all_df = []
    # Initialize json output df
    all_json = []
    # Iterate over the query files
    for query_file in query_files:
        
        # Open the JSON query file and read it as a python dict.
        try:
            with open(query_file, 'r') as f:
                dates = pd.to_datetime('today')
                dates_str = str(dates)
                date_f = dates_str.split(" ")[0]
                time_f = dates_str.split(" ")[1].split(".")[0]
                downloaded_at = date_f + " "+time_f
                # Load file
                query_dict = json.load(f)
                # Tunr response into pandas dataframe 
                json_response_df = json_normalize(query_dict)
                json_response_df["Date/Time"] = downloaded_at
                # Perform the query. Time it
                start_time = time.time()
                query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
                total_time = time.time() - start_time
                json_response_df["TimeTaken(s)"] = total_time
                # Turn query response into pandas dataframe
                norm_facets = json_normalize(query_json['Facet'])
                
                all_json.append(norm_facets)
                
                json_response_df["NumberRepertoire"] = len(norm_facets)
                json_response_df["RepertoireCount"] = sum(norm_facets["count"])
                all_df.append(json_response_df)
                
        except IOError as error:
            print("ERROR: Unable to open JSON file " + query_file + ": " + str(error))
        except json.JSONDecodeError as error:
            if force:
                print("WARNING: JSON Decode error detected in " + query_file + ": " + str(error))
                with open(query_file, 'r') as f:
                    query_dict = f.read().replace('\n', '')
            else:
                print("ERROR: JSON Decode error detected in " + query_file + ": " + str(error))
        except Exception as error:
            print("ERROR: Unable to open JSON file " + query_file + ": " + str(error))

        if verbose:
            print('INFO: Performing query: ' + str(query_dict))

        
#         print("--- Query processed in %s seconds ---" % (time.time() - start_time))
#         if verbose:
#             print('INFO: Query response: ' + str(query_json))

        
