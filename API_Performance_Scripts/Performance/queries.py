######### PERFORMANCE TESTING PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: MAY 20, 2019
######### LAST MODIFIED ON: July 11, 2019

import urllib.request, urllib.parse
import argparse
import json
import os, ssl
import pandas as pd
import sys
import time
import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict

##################################
#### FUNCTION DEFINITION AREA ####
##################################

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

def getHeaderDict():
    # Set up the header for the post request.
    header_dict = {'accept': 'application/json',
                   'Content-Type': 'application/x-www-form-urlencoded'}
    return header_dict

def initHTTP():
    # Deafult OS do not have create cient certificate bundles. It is
    # easiest for us to ignore HTTPS certificate errors in this case.
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
        getattr(ssl, '_create_unverified_context', None)): 
        ssl._create_default_https_context = ssl._create_unverified_context

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
    return[count_full,sum_vc]


def get_total_number_sequences(sample_dic_call):
    sum_all_seq = 0
    for key in sample_dic_call:
        if '4' not in sample_dic_call[key].keys():
            continue
        else:
            sum_all_seq += sample_dic_call[key]['4'][3]
    return sum_all_seq

def get_nr_empty_full_entries_junction(sample_dic_call):
    
    count_full_15  = 0
    count_full_9  = 0
    sum_junction_len_9 = 0
    sum_junction_len_15 = 0 

    for key in sample_dic_call:
        
        if 9 in sample_dic_call[key].keys():
            
            sum_junction_len_9 += sample_dic_call[key][9][2]
            
        if 15 in sample_dic_call[key].keys():
            
            sum_junction_len_15 += sample_dic_call[key][15][2]
        
        
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

        for j in range(6):

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
    
def construct_df_from_queries(base_url,call_dic,ipa_name):
    
    try:
    
        # Perform queries
        [number_seq_tot,number_samp_tot,entries_1_j,\
         times1_j,entries_1_junctionaa,times1_junctionaa,\
         query_arr] = perform_queries(base_url,call_dic) 

        # Build data frame using # samples, # sequences and time taken 
        df_ipa = pd.DataFrame({"Date/Time":query_arr[-1],"IPA#":[str(ipa_name)],#,"ipa2","ipa3","ipa4","ipa5",],

                       # Total number sequences and samples
                            "NUMBERSAMPLES(TOTAL)":number_samp_tot,
                            "NUMBERSEQUENCES(TOTAL)":number_seq_tot,   

                       # Junction aa LLTL    
                           "NUMBERSAMPLES(junction_aa=LLTL)": entries_1_junctionaa[0],
                           "NUMBERSEQUENCES(junction_aa=LLTL)": entries_1_junctionaa[1],
                            "TIME(junction_aa=LLTL)":times1_junctionaa[0],

                       # Junction length 9
                           "NUMBERSAMPLES(junction_aa_length=9)": entries_1_j[0],
                           "NUMBERSEQUENCES(junction_aa_length=9)": entries_1_j[2],
                            "TIME(junction_aa_length=9)":times1_j[0],

                           # Junction length 15
                           "NUMBERSAMPLES(aa_len=15)": entries_1_j[1],
                           "NUMBERSEQUENCES(aa_len=15)": entries_1_j[3],
                           "TIME(aa_len=15)":times1_j[1],

                        # B Cell 
                           # V call family 
                         "NUMBERSAMPLES(v_call = " + str(query_arr[0][0][3]) +" f)": query_arr[0][0][0],
                           "NUMBERSEQUENCES(v_call = " + str(query_arr[0][0][3]) +" f)": query_arr[0][0][1],
                            "TIME(v_call = " + str(query_arr[0][0][3]) +" f)":query_arr[0][0][2],

                           # V call gene
                         "NUMBERSAMPLES(v_call = " + str(query_arr[1][0][3]) +" g)": query_arr[1][0][0],
                           "NUMBERSEQUENCES(v_call = " + str(query_arr[1][0][3]) +" g)": query_arr[1][0][1],
                            "TIME(v_call = " + str(query_arr[1][0][3]) +" g)":query_arr[1][0][2],

                           # V call allele
                         "NUMBERSAMPLES(v_call = " + str(query_arr[2][0][3]) +" a)": query_arr[2][0][0],
                           "NUMBERSEQUENCES(v_call = " + str(query_arr[2][0][3]) +" a)": query_arr[2][0][1],
                            "TIME(v_call = " + str(query_arr[2][0][3]) +" a)":query_arr[2][0][2],
                        # T Cell 
                             # V call family 
                         "NUMBERSAMPLES(v_call = " + str(query_arr[3][0][3]) +" f)": query_arr[3][0][0],
                           "NUMBERSEQUENCES(v_call = " + str(query_arr[3][0][3]) +" f)": query_arr[3][0][1],
                            "TIME(v_call = " + str(query_arr[3][0][3]) +" f)":query_arr[3][0][2],

                           # V call gene
                         "NUMBERSAMPLES(v_call = " + str(query_arr[4][0][3]) +" g)": query_arr[4][0][0],
                           "NUMBERSEQUENCES(v_call = " + str(query_arr[4][0][3]) +" g)": query_arr[4][0][1],
                            "TIME(v_call = " + str(query_arr[4][0][3]) +" g)":query_arr[4][0][2],

                           # V call allele
                         "NUMBERSAMPLES(v_call = " + str(query_arr[5][0][3]) +" a)": query_arr[5][0][0],
                           "NUMBERSEQUENCES(v_call = " + str(query_arr[5][0][3]) +" a)": query_arr[5][0][1],
                            "TIME(v_call = " + str(query_arr[5][0][3]) +" a)":query_arr[5][0][2],
                        # B Cell 
                         # D call family 
                         "NUMBERSAMPLES(d_call = " + str(query_arr[6][0][3]) +" f)": query_arr[6][0][0],
                           "NUMBERSEQUENCES(d_call = " + str(query_arr[6][0][3]) +" f)": query_arr[6][0][1],
                            "TIME(d_call = " + str(query_arr[6][0][3]) +" f)":query_arr[6][0][2],

                           # D call gene
                         "NUMBERSAMPLES(d_call = " + str(query_arr[7][0][3]) +" g)": query_arr[7][0][0],
                           "NUMBERSEQUENCES(d_call = " + str(query_arr[7][0][3]) +" g)": query_arr[7][0][1],
                            "TIME(d_call = " + str(query_arr[7][0][3]) +" g)":query_arr[7][0][2],

                           # D call allele
                         "NUMBERSAMPLES(d_call = " + str(query_arr[8][0][3]) +" a)": query_arr[8][0][0],
                           "NUMBERSEQUENCES(d_call = " + str(query_arr[8][0][3]) +" a)": query_arr[8][0][1],
                            "TIME(d_call = " + str(query_arr[8][0][3]) +" a)":query_arr[8][0][2],

                        # T Cell       
                         # D call family 
                         "NUMBERSAMPLES(d_call = " + str(query_arr[9][0][3]) +" f)": query_arr[9][0][0],
                           "NUMBERSEQUENCES(d_call = " + str(query_arr[9][0][3]) +" f)": query_arr[9][0][1],
                            "TIME(d_call = " + str(query_arr[9][0][3]) +" f)":query_arr[9][0][2],

                           # D call gene
                         "NUMBERSAMPLES(d_call = " + str(query_arr[10][0][3]) +" g)": query_arr[10][0][0],
                           "NUMBERSEQUENCES(d_call = " + str(query_arr[10][0][3]) +" g)": query_arr[10][0][1],
                            "TIME(d_call = " + str(query_arr[10][0][3]) +" g)":query_arr[10][0][2],

                           # D call allele
                         "NUMBERSAMPLES(d_call = " + str(query_arr[11][0][3]) +" a)": query_arr[11][0][0],
                           "NUMBERSEQUENCES(d_call = " + str(query_arr[11][0][3]) +" a)": query_arr[11][0][1],
                            "TIME(d_call = " + str(query_arr[11][0][3]) +" a)":query_arr[11][0][2],

                           # B Cell 
                             # J call family 
                         "NUMBERSAMPLES(j_call = " + str(query_arr[12][0][3]) +" f)": query_arr[12][0][0],
                           "NUMBERSEQUENCES(j_call = " + str(query_arr[12][0][3]) +" f)": query_arr[12][0][1],
                            "TIME(j_call = " + str(query_arr[12][0][3]) +" f)":query_arr[12][0][2],

                           # J call gene
                         "NUMBERSAMPLES(j_call = " + str(query_arr[13][0][3]) +" g)": query_arr[13][0][0],
                           "NUMBERSEQUENCES(j_call = " + str(query_arr[13][0][3]) +" g)": query_arr[13][0][1],
                            "TIME(j_call = " + str(query_arr[13][0][3]) +" g)":query_arr[13][0][2],

                           # J call allele
                         "NUMBERSAMPLES(j_call = " + str(query_arr[14][0][3]) +" a)": query_arr[14][0][0],
                           "NUMBERSEQUENCES(j_call = " + str(query_arr[14][0][3]) +" a)": query_arr[14][0][1],
                            "TIME(j_call = " + str(query_arr[14][0][3]) +" a)":query_arr[14][0][2],
                        # T Cell 
                         # J call family 
                         "NUMBERSAMPLES(j_call = " + str(query_arr[15][0][3]) +" f)": query_arr[15][0][0],
                           "NUMBERSEQUENCES(j_call = " + str(query_arr[15][0][3]) +" f)": query_arr[15][0][1],
                            "TIME(j_call = " + str(query_arr[15][0][3]) +" f)":query_arr[15][0][2],

                           # J call gene
                         "NUMBERSAMPLES(j_call = " + str(query_arr[16][0][3]) +" g)": query_arr[16][0][0],
                           "NUMBERSEQUENCES(j_call = " + str(query_arr[16][0][3]) +" g)": query_arr[16][0][1],
                            "TIME(j_call = " + str(query_arr[16][0][3]) +" g)":query_arr[16][0][2],

                           # J call allele
                         "NUMBERSAMPLES(j_call = " + str(query_arr[17][0][3]) +" a)": query_arr[17][0][0],
                           "NUMBERSEQUENCES(j_call = " + str(query_arr[17][0][3]) +" a)": query_arr[17][0][1],
                            "TIME(j_call = " + str(query_arr[17][0][3]) +" a)":query_arr[17][0][2]})

        return df_ipa
    except: 
        start_time = time.time()
        orig_stdout = sys.stdout
        f = open(str(start_time) + '_out.txt', 'w')
        sys.stdout = f

        print("ERROR: CANNOT GENERATE DATA FRAME. SOMETHING WENT WRONG")
        

        sys.stdout = orig_stdout
        f.close()
        sys.exit(1)

def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Output Directory - where Performance test results will be stored 
    parser.add_argument(
        "output_dir",
        help="Indicate the full path to where performance test results will be stored."
    )
    
    parser.add_argument(
        "ipa_arr",
        help="Array containing URL to API server"
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
    output_dir = options.output_dir
    ipa = options.ipa_arr
    
    # Max and mins queries in each ipa
    # ipa1 v d j 
    max_fam_ipa1 = ['IGHV1','IGHD1','IGHJ4']

    max_gen_ipa1 = ['IGHV1-69','IGHD1-1','IGHJ4']

    max_all_ipa1 = ['IGHV1-18*01','IGHD1-1*01','IGHJ4*02']


    # ipa2 v d j 
    max_fam_ipa2 = ['TRBV20','TRBD1','TRBJ2']

    max_gen_ipa2 = ['TRBV20-1','TRBD1','TRBJ2-7']

    max_all_ipa2 = ['TRBV20-1*01 ','TRBD1*01','TRBJ2-7*01']

    # Set up dictionary where keys are v,d,j call fields, and values are values under each field to be queried
    call_dic = {"v_call":[max_fam_ipa1[0],max_gen_ipa1[0],max_all_ipa1[0],max_fam_ipa2[0],max_gen_ipa2[0],max_all_ipa2[0]],
           "d_call":[max_fam_ipa1[1],max_gen_ipa1[1],max_all_ipa1[1],max_fam_ipa2[1],max_gen_ipa2[1],max_all_ipa2[1]],
           "j_call":[max_fam_ipa1[2],max_gen_ipa1[2],max_all_ipa1[2],max_fam_ipa2[2],max_gen_ipa2[2],max_all_ipa2[2]]}

    # Perform queries on the following API URL
    #base_url =["https://ipa1.ireceptor.org/","https://ipa2.ireceptor.org/",\
    #           "https://ipa3.ireceptor.org/","https://ipa4.ireceptor.org/",\
    #           "http://ipa5.ireceptor.org/"]
    
    #base_url=["https://ipa1.ireceptor.org/"] #,"https://ipa4-staging.ireceptor.org/"]
    
    # Store results into df
    
    base_url = ipa.split(",")
    all_dfs = []
    # For each API
    for item in base_url:
        print(item)
        
        name = item.split(".")[0].split("//")[1]
        # Perform queries and store into df
        df = construct_df_from_queries(item,call_dic,name)
        
        # Store df into array
        all_dfs.append(df) 
    
    
    # Concatenate all df into a single df
    all_ipas = pd.concat([all_dfs[i] for i in range(len(base_url))])
    
    # Save results
    
    dates = pd.to_datetime('today')
    dates_str = str(dates)
    date_f = dates_str.split(" ")[0]
    time_f = dates_str.split(" ")[1].split(".")[0]
    timestamp = (date_f +"_" +time_f).replace(":","_")

    all_ipas.to_csv(output_dir + "_PerformanceTesting_" +  timestamp +"_Query_Times" + str(name) + ".csv")
    
    sys.exit(0)