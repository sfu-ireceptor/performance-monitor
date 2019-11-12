# ADC API PERFORMANCE TESTING PYTHON SCRIPT
# AUTHORS: Brian Corrie, Laura Gutierrez Funderburk
# SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
# CREATED ON: MAY 20, 2019
# LAST MODIFIED ON: November 11, 2019

import urllib.request, urllib.parse
import json
import os, ssl
import time 

#############################################################################
#################### ADC-API (File) Performance Testing #####################
#############################################################################

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

def process_json_files(force,verbose,query_file):
        
        # Open the JSON query file and read it as a python dict.
        with open(query_file, 'r') as f:
            try: 
                # Load file
                query_dict = json.load(f)

                if verbose:
                    print('INFO: Performing query: ' + str(query_dict))    
                return query_dict
                
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
    header_dict = getHeaderDict()

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

    for item in ["v_call","d_call","j_call"]:

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
