######### PERFORMANCE TESTING PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: October 31, 2019
######### LAST MODIFIED ON: April 21 2020

from curlairripa import *       # https://test.pypi.org/project/curlairripa/ 
import time                     # time stamps
import pandas as pd
import argparse                 # Input parameters from command line 
import os
import airr

#############################################################################
########################## Old Performance Testing ##########################
#############################################################################

def getSequenceSummary(sequence_url, header_dict, query_dict={}):
    # Build the required JSON data for the post request. The user
    # of the function provides both the header and the query data
    url_dict = dict()
    #url_dict.update(header_dict)
    url_dict.update(query_dict)
    url_data = urllib.parse.urlencode(url_dict).encode()
    

    # Try to make the connection and get a response.
    try:
        request = urllib.request.Request(sequence_url, url_data, header_dict)
        #response = urllib.request.urlopen(sequence_url, data=url_data)
        response = urllib.request.urlopen(request)
        url_response = response.read().decode(response.headers.get_content_charset())
    except urllib.error.HTTPError as e:
        print('Error: Server could not fullfil the request')
        print('Error: Error code =', e.code)
        print(e.read())
        return json.loads('[]')

    except urllib.error.URLError as e:
        print('Error: Failed to reach the server')
        print('Error: Reason =', e.reason)
        return json.loads('[]')

    
    # Convert the response to JSON so we can process it easily.
    json_data = json.loads(url_response)
    
    # Print out the summary stats for the repository.
    sample_summary = json_data['summary']

    # Return the JSON of the results.
    return sample_summary

def getSamples(sample_url, header_dict, query_dict={}):
    # Build the required JSON data for the post request. The user
    # of the function provides both the header and the query data
    url_dict = dict()
    #url_dict.update(header_dict)
    url_dict.update(query_dict)
    url_data = urllib.parse.urlencode(url_dict).encode()

    # Try to connect the URL and get a response. On error return an
    # empty JSON array.
    try:
        request = urllib.request.Request(sample_url, url_data, header_dict)
        #response = urllib.request.urlopen(sample_url, data=url_data, headers=header_dict)
        response = urllib.request.urlopen(request)
        url_response = response.read().decode(response.headers.get_content_charset())
    except urllib.error.HTTPError as e:
        print('Error: Server could not fullfil the request')
        print('Error: Error code =', e.code)
        print(e.read())
        return json.loads('[]')
        
    except urllib.error.URLError as e:
        print('Error: Failed to reach the server')
        print('Error: Reason =', e.reason)
        return json.loads('[]')

    # Convert the response to JSON so we can process it easily.
    # print(url_response)
    json_data = json.loads(url_response)
    # Return the JSON data
    return json_data

def getHeaderDict_old_performance_test():
    # Set up the header for the post request.
    header_dict = {'accept': 'application/json',
                   'Content-Type': 'application/x-www-form-urlencoded'}
    return header_dict

def performQueryAnalysis(base_url, query_key, query_values):
    # Ensure our HTTP set up has been done.
    initHTTP()
    # Get the HTTP header information (in the form of a dictionary)
    header_dict = getHeaderDict_old_performance_test()

    # Select the API entry points to use, based on the base URL provided
    sample_url = base_url+'/v2/samples'
    sequence_url = base_url+'/v2/sequences_summary'

    # Get the sample metadata for the query. We want to keep track of each sample
    sample_json = getSamples(sample_url, header_dict)
    # Create a dictionary with keys as the ID for the sample.
    sample_dict = dict()
    # For each sample, create an empty dictionary (to be filled in later)
    for sample in sample_json:
        sample_dict[str(sample['_id'])] = dict()
    
    times  = []
    # Iterate over the query values of interest. One query per value gives us results
    # for all samples so this is about as efficient as it gets.
    for value in query_values:
        #time.sleep(-time.time()%2)
        # Create and do the query for this value.
        query_dict = dict()
        query_dict.update({query_key: value})
        print(query_dict)
        #print('Performing query: ' + str(query_key) + ' = ' + str(value))
        start_time = time.time()
        sequence_summary_json = getSequenceSummary(sequence_url, header_dict, query_dict)
        total_time = time.time() - start_time
        times.append(total_time)
        #print(sequence_summary_json)
        # The query gives us a count for each sample. We iterate over the samples to do some
        # bookkeeping for that sample.
        for sample in sequence_summary_json:
            # Get the dictionaries of values for this sample
            #print(sample['sample_id'])
            value_dict = sample_dict[str(sample['_id'])]
            # Update the count for the value we are considering
            study_id = sample['study_id']
            submi_by = sample['submitted_by']
            filtered_sequence_count = sample['ir_filtered_sequence_count']
            pure_sequence_count = sample["ir_sequence_count"]
            # NEED TO FIND OUT HOW TO GET FULL DICT
            value_dict.update({value:[study_id,submi_by,filtered_sequence_count,pure_sequence_count]})
            sample_dict[str(sample['_id'])] = value_dict
            #print(query_key + ' ' + str(value) + ' = ' + str(filtered_sequence_count))
            #print(value_dict) 

    # Return the data.
    return [sample_dict,times]

## This function takes as input a dictionary containing query information
##  for either a v,d,j call and the value being queries
##  and it returns a list with the number of samples that contained such query
##  as well as the total number of sequences for those samples containing the query

##  Sample_dict_call is a dictionary of the form: 
##   {'4': {'IGHV1': ['PRJEB9332', 'Chang, Y.H., Kuan, H.C.', 8028]}}
##   where the first key corresponds to the sample _id, and the value is a secont
##   dictionary holding the query, along with the study id the authors and the
##  ir_filtered_sequence_count value
##  the query value that will serve as input for our function is the query "IGHV1"
def get_nr_empty_full_entries_calls(sample_dic_call,query):
    
    # Initialize counters #
    # Samples where query was found
    count_full = 0
    # Total number of sequences for samples such that the query was found
    sum_vc = 0
    
    
    # Iterate over all samples, each key is determined by the sample _id value
    for key in sample_dic_call:
        
        # If the query was not found under a sample, skip
        if(sample_dic_call[key]=={}):
            continue
        # If on the other hand, the sample did contain the query, the dictionary 
        #  will not be empty, increment number of samples where query was found
        else:
            count_full +=1
        
        # Now it's time to track the total number of sequences associated to that
        #   query. For each _id, our dictionary will track information including
        #   ir_filtered_sequence_count, such information is the last entry in the
        #   array associated to our key, we add it to the total sum of sequences 
        if query in sample_dic_call[key].keys():
            
            sum_vc += sample_dic_call[key][query][2]

    # Return total number of samples where query was found and
    #  total number of sequences associated to samples where query was found
    return [count_full,sum_vc]

# Get total number of sequences (rearrangements) in call 
def get_total_number_sequences(sample_dic_call):
    sum_all_seq = 0
    for key in sample_dic_call:
        if '4' not in sample_dic_call[key].keys():
            continue
        else:
            sum_all_seq += sample_dic_call[key]['4'][3]
    return sum_all_seq

# Check total number of sequences found in queries containing junction_length = 9 and 15 
# If junction_length values change, this MUST be reflected in the if statements accordingly
# Notice that an additional case catches whether none of the specified values appear in the service
def get_nr_empty_full_entries_junction(sample_dic_call):
    
    # Initialize counters - number of samples (repertoires) containing desired junction_length
    count_full_15  = 0
    count_full_9  = 0
    # Initialize counters - number of sequences (rearragements) containing desired junction_length
    sum_junction_len_9 = 0
    sum_junction_len_15 = 0 

    # Iterate over keys in query
    for key in sample_dic_call:
        
        # Check if junction_length ==9, count number of sequences (rearrangements)
        if 9 in sample_dic_call[key].keys():
            
            sum_junction_len_9 += sample_dic_call[key][9][2]
            
        # Check if junction_length == 15, count number of sequences (rearrangements)
        if 15 in sample_dic_call[key].keys():
            
            sum_junction_len_15 += sample_dic_call[key][15][2]
        
        # Count number of samples (repertoires) - note that when we count samples, it may happen that the sample (repertoire) contains 
        #  sequences (rearrangements) that either have all junction_length  = 9, all sequences (rearrangements) have junction_length  = 15, 
        #  some sequences (rearrangements) with junction_length 9, some sequences (rearrangemnts) with junction_length 15 or neither
        if len(sample_dic_call[key].keys()) ==1:
            
            if 15 in sample_dic_call[key].keys():
                
                count_full_15 +=1
                
            elif 9 in sample_dic_call[key].keys():
                
                count_full_9 +=1
            else:
                
                print(sample_dict1_j[key])
                
        elif len(sample_dic_call[key].keys())==2:
            count_full_15 +=1
            count_full_9 +=1
        else:
            continue
    # Return full counts 
    return [count_full_9,count_full_15,sum_junction_len_9,sum_junction_len_15]

def perform_queries(base_url,call_dic):
        # Get date and time
    dates = pd.to_datetime('today')
    dates_str = str(dates)
    date_f = dates_str.split(" ")[0]
    time_f = dates_str.split(" ")[1].split(".")[0]
    downloaded_at = date_f + " "+time_f
    
    ### NEW QUERY
    # Get total number of sequences and samples
    [all_sampl_seq_dict,all_sampl_seq_time]= performQueryAnalysis(base_url, "_id", ["4"])    
    number_seq_tot = get_total_number_sequences(all_sampl_seq_dict)
    number_samp_tot = len(all_sampl_seq_dict)
   
    ### NEW QUERY
    # Get junction length queries 
    [sample_dict1_j,times1_j]= performQueryAnalysis(base_url, "junction_aa_length", [9,15])
    # Count Number samples and sequences where query was found
    entries_1_j = get_nr_empty_full_entries_junction(sample_dict1_j)
    
    ### NEW QUERY
    # Get junctionaa  queries 
    [sample_dict1_junctionaa,times1_junctionaa]= performQueryAnalysis(base_url, "junction_aa", ["LLTL"])
    # Count Number samples and sequences where query was found
    entries_1_junctionaa = get_nr_empty_full_entries_calls(sample_dict1_junctionaa,"LLTL")

    ### NEW QUERY
    # Get v,d,j call queries
    query_arr = []

    for item in call_dic.keys():

        for j in range(len(call_dic[item])):


            [ig_sample_dict1_vc,ig_times1_vc]= performQueryAnalysis(base_url, item, [call_dic[item][j]])

            # Count number samples a d sequences where query was found
            entries_1_vs = get_nr_empty_full_entries_calls(ig_sample_dict1_vc,call_dic[item][j])
            # Add time taken
            entries_1_vs.append(ig_times1_vc[0])
            # Add v,d,j call field name
            entries_1_vs.append(call_dic[item][j])
            # Add number samples and sequences to array
            query_arr.append([entries_1_vs])
    
    # Add date and time 
    query_arr.append(downloaded_at)    
    
    return [number_seq_tot,number_samp_tot,entries_1_j,times1_j,entries_1_junctionaa,times1_junctionaa,query_arr]

def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Output Directory - where Performance test results will be stored 
    parser.add_argument(
        "base",
        help="Indicate the full path to where performance test results will be stored"
    )
    # Array with URL
    parser.add_argument(
        "ipa_arr",
        help="String containing URL to API server  (e.g. https://airr-api2.ireceptor.org)"
    )
    # Entry point
    parser.add_argument(
        "entry_point",
        help="Options: string 'rearragement' or string 'repertoire'"
    )
    
    parser.add_argument(
            "json_files",
        help="Enter full path to JSON queries"
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
    query_url = base_url + "/airr/v1/" + entry_pt
    
    # Leave static for now
    expect_pass = True
    verbose = True
    force = True

    # Ensure our HTTP set up has been done.
    initHTTP()
    # Get the HTTP header information (in the form of a dictionary)
    header_dict = getHeaderDict()
    
    # Get all JSON files
    files = []
    for r, d, f in os.walk(query_files):
        for file in f:
            if '.json' in file:
                files.append(os.path.join(r, file))
    
    all_facets_count = []
    json_response_all_dfs = []
    # Perform query for each JSON file
    print("\nADC-API AIRR Performance Testing\n")
    for item in files:
        if "checkpoint" in item:
            continue
        else:
            # Get timestampt
            dates = pd.to_datetime('today')
            dates_str = str(dates)
            date_f = dates_str.split(" ")[0]
            time_f = dates_str.split(" ")[1].split(".")[0]
            downloaded_at = date_f + "_"+time_f

            # Load file
            query_dict = process_json_files(force,verbose,item)

            # Tunr response into pandas dataframe 
            json_response_df = pd.json_normalize(query_dict)


            # Perform the query. Time it
            start_time = time.time()
            query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
            total_time = time.time() - start_time
            # Append results 
            json_response_df["TimeTaken(s)"] = total_time
            
            if len(query_json) != 0:
                query_json = json.loads(query_json)

                # Turn query response into pandas dataframe
                norm_facets = pd.json_normalize(query_json['Facet'])
                all_facets_count.append(norm_facets)
                
                if 'count' in norm_facets.keys():
                    json_response_df["NumberRepertoire"] = len(norm_facets[norm_facets['count']!=0])
                    json_response_df["NumberRearrangement"] = sum(norm_facets["count"])
                else:
                    json_response_df["NumberRepertoire"] = 0
                    json_response_df["NumberRearrangement"] =0

                
                json_response_all_dfs.append(json_response_df)
                
            else:
                json_response_df["NumberRepertoire"] = -100
                json_response_df["NumberRearrangement"] = -100

                
                json_response_all_dfs.append(json_response_df)
                
            
            # Store in array for storage of all query results into single CSV
    df_list = [json_response_all_dfs[i] for i in range(len(json_response_all_dfs))]

            # Keep this option if you want one CSV per query - might be cumbersome but who knows, also handy - Save into CSV for later visualizing 
#             json_response_df.to_csv(output_dir + "_PerformanceTesting_" + str(date_f) + "_" + str(time_f) + "_Query_Times_" + str(base_url.split("//")[1].split(".")[0]) + "_" + str(base_url.split("/")[-1]) + "_"+ str(item.split(".")[0].split("/")[-1]) +  ".csv",sep=",")
    
    # Stack all results into single datafrane
    stacked = pd.concat(df_list)
    # Store content 
    stacked.to_csv(output_dir + "_PerformanceTesting_" + str(date_f) + "_" + str(time_f) + "_Query_Times_" + str(base_url.split("//")[1].split(".")[0]) + "_" + str(base_url.split("/")[-1]) +  ".csv",sep=",")
