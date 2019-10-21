##Script Author: Laura Gutierrez Funderburk
##Supervised by: Dr. Felix Breden, Dr. Jamie Scott, Dr. Brian Corrie
##Created on: May 2 2019
##Last modified on: July 22 2019

"""Description:

This script determines whether there are odd words in v_call, d_call, j_call fields within a TSV sequence file

"""

import pandas as pd
import numpy as np
import argparse
import requests
import urllib3
import time
import sys
import os
import airr
import urllib.request, urllib.parse
import os, ssl
import json
import zipfile
import math

#########################################
######## FUNCTION DEFINITION AREA #######
#########################################



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

def getSequenceTSV(sequence_url, header_dict, query_dict={}):
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
    


    # Return the JSON of the results.
    return url_response

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
    

    # Iterate over the query values of interest. One query per value gives us results
    # for all samples so this is about as efficient as it gets.
    for value in query_values:
        # Create and do the query for this value.
        query_dict = dict()
        query_dict.update({query_key: value})
        #print(query_dict)
        #print('Performing query: ' + str(query_key) + ' = ' + str(value))
        sequence_summary_json = getSequenceSummary(sequence_url, header_dict, query_dict)
        #print(sequence_summary_json)
        # The query gives us a count for each sample. We iterate over the samples to do some
        # bookkeeping for that sample.
        for sample in sequence_summary_json:
            # Get the dictionaries of values for this sample
            # print(sample['sample_id'])
            value_dict = sample_dict[str(sample['_id'])]
            # Update the count for the value we are considering
            study_id = sample['study_id']
            submi_by = sample['submitted_by']
            filtered_sequence_count = sample['ir_filtered_sequence_count']
            # NEED TO FIND OUT HOW TO GET FULL DICT
            value_dict.update({value:[study_id,submi_by,filtered_sequence_count]})
            sample_dict[str(sample['_id'])] = value_dict
            #print(query_key + ' ' + str(value) + ' = ' + str(filtered_sequence_count))
            print(value_dict)

    # Return the data.
    return sample_dict

def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Sample id 
    parser.add_argument("sample_id")
    # Junction length
    #parser.add_argument("junction_length")
    # Word search
    parser.add_argument("words")
    # Sequence file path
    parser.add_argument("TSV_dir")
    # ipa address
    parser.add_argument("base_url")
    # Verbosity flag
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Run the program in verbose mode.")

    # Parse the command line arguements.
    options = parser.parse_args()
    return options

def check_no_odd_words(dataframe,column_name,ig_tr_class,all_any_option):
    
    try:
        # Get dimensions of dataframe
        all_entries_in_column = dataframe[column_name].tolist()
        iterate_over = len(all_entries_in_column)
        
        # Initialize array - the items in this array will be tested and correspond to each entry in each of the alleles
        # listed under v,d,j call fields
        new_arr = []
        # Iterate over all entries in pandas
        for i in range(iterate_over):
            #print(all_entries_in_column[i])
            # Skip empty entries - python pandas parses NaN as float type
            if type(all_entries_in_column[i])==float:
                continue
            else:
                # Iterate over all alleles in each row
                for j in range(len(all_entries_in_column[i])):
                        # Append all entries in new_array
                    new_arr.append(all_entries_in_column[i][j])
    
        # Determine whether the desired word is found in each entry in the array
        if all_any_option=="all":
            return all(ig_tr_class in item for item in new_arr)
        # Determine whether the desired word exists in the entire array
        elif all_any_option=="any":
            return any(ig_tr_class in item for item in new_arr)
    
    except:
        
        print("WARNING: Input is a pandas dataframe, a column name within the dataframe containing entries for\
        v call, d call or j call, and a string from the following list: IG, TR")

        
#########################################
############## MAIN PROGRAM #############
#########################################
        
if __name__ == "__main__":
    
#     # Get the command line arguments.
    options = getArguments()
    sample_url = options.base_url+'/v2/samples'
    sequence_url = options.base_url+'/v2/sequences_summary'
    sequence_data = options.base_url+"/v2/sequences_data"

    sample_id = options.sample_id
    TSV_Path = options.TSV_dir
    
    sample_dict = performQueryAnalysis(options.base_url, "ir_project_sample_id_list[]", [options.sample_id])
    #Disable warning
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Info necessary to complete requests 
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
    'ir_project_sample_id_list[]': str(options.sample_id),
     #'junction_aa_length': str(options.junction_length),
    'ir_data_format': 'airr'
    }

    #Time sequence download
    start_time = time.time()
    response = requests.post(sequence_data, headers=headers, data=data, verify=False)
    
    total_time = time.time() - start_time
    
    print("ELAPSED DOWNLOAD TIME (in seconds): %s" % (total_time))
    print("ELAPSED DOWNLOAD TIME (in minutes): %s" % (total_time/60))
    print("ELAPSED DOWNLOAD TIME (in hours): %s" % (total_time/3600))

    # Save response into TSV file
    filename  =  options.TSV_dir + "sequences_data_sampleid_" + str(options.sample_id) + "_.tsv"
    file = open(filename, "w")
    file.write(response.text)
    file.close()
    
    # Get filesize
    file_size = os.path.getsize(str(filename))
    print("FILE SIZE (in MB): " + str(file_size/1000000))
        
     # Parse TSV as pandas dataframe
    sample_file = pd.read_csv(str(filename),sep="\t",low_memory=False)
    
    # Take care of float entries
    sample_file[sample_file.v_call.notnull()] = sample_file.astype(str)
    sample_file[sample_file.d_call.notnull()] = sample_file.astype(str)
    sample_file[sample_file.j_call.notnull()] = sample_file.astype(str)
    
    # Remove ", or " from list of entries in v_call, d_call, j_call and separate entries with ","
    split_vcall = np.array(sample_file.v_call.str.split(", or "))
    split_dcall = np.array(sample_file.d_call.str.split(", or "))
    split_jcall = np.array(sample_file.j_call.str.split(", or "))

    # Add new columns and new values
    sample_file["split_vcall"] = split_vcall
    sample_file["split_dcall"] = split_dcall
    sample_file["split_jcall"] = split_jcall
    

    print("NUMBER OF SEQUENCES: " + str(sample_file.shape[0]))
    if sample_file.shape[0]==0:
        print("(AVERAGE) ELAPSED TIME PER SEQUENCE (in seconds per sequence): " + str(sample_file.shape[0]) + "\n")
    else:
        print("(AVERAGE) ELAPSED TIME PER SEQUENCE (in seconds per sequence): " + str(total_time/sample_file.shape[0]) + "\n")
    
    # Validate file
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("AIRR STANDARD TEST")
    if airr.validate_rearrangement(filename, False)==True:
        print("File passes AIRR standards\n")
    else:
        print("WARNING: file does not pass AIRR standards\n")
    
    # Run test
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("Checking for odd words\n")
    print("Case  \t Any  \t All  \t Meaning ")
    print("----------------------------------------------------------------------------------------")
    print("1 \t True \t True \t A exists and A is in all entries ")
    print("2 \t True \t False \t A exists but there is at least one entry where A is not found  ")
    print("3 \t False \t True \t Empty dataframe  ")
    print("4 \t False \t False \t A does not exist in any of the entries  \n")
    print("Field\tWord\tAny \tAll")
    
    values = options.words.split(',')
    
    
    for item in values:
    
        V_IG_any = check_no_odd_words(sample_file,"split_vcall",str(item),"any")
        V_IG_all = check_no_odd_words(sample_file,"split_vcall",str(item),"all")
        D_IG_any = check_no_odd_words(sample_file,"split_dcall",str(item),"any")
        D_IG_all = check_no_odd_words(sample_file,"split_dcall",str(item),"all")
        J_IG_any = check_no_odd_words(sample_file,"split_jcall",str(item),"any")
        J_IG_all = check_no_odd_words(sample_file,"split_jcall",str(item),"all")
    
        print("V Call \t"+str(item)+"\t" + str(V_IG_any)+'\t'+str(V_IG_all))
        print("D Call \t"+str(item)+"\t" +str(D_IG_any)+'\t'+str(D_IG_all))
        print("J Call \t"+str(item)+"\t" +str(J_IG_any)+'\t'+str(J_IG_all))
        print("")

        
    # TEST FOR LOCUS EQUALITY :3 v,d,j call fields
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("\n")

    # Get items from each field: v,d,j call
    v_cal_list = sample_file["v_call"].tolist()
    d_cal_list = sample_file["d_call"].tolist()
    j_cal_list = sample_file["j_call"].tolist()

    # Turn list into set
    v_cal_loc = set([v_cal_list[i][:3] for i in range(len(v_cal_list)) if type(v_cal_list[i])!=float])
    d_cal_loc = set([d_cal_list[i][:3] for i in range(len(d_cal_list)) if type(d_cal_list[i])!=float])
    j_cal_loc = set([j_cal_list[i][:3] for i in range(len(j_cal_list)) if type(j_cal_list[i])!=float])


    # Test: get union of three sets generated above - 
        # Case 1: union only has one element - test passes
        # Case 2: union has 2 elements - a string and a NaN value - test passes
        # Case 3: union has 2 or more elements, at least two of them must be non-empty values - test fails
    test = v_cal_loc.union(d_cal_loc).union(j_cal_loc)
    # Case 1
    if len(test)==1:
        result = True
    # Case 2
    elif len(test)==2 and float('NaN') in test:
        result = True
    # Case 2
    elif len(test)==2 and str(float('NaN')) in test:
        result = True
    # Case 3
    else:
        result = False

    if result == True:
        print("Locus equality for v,d,j call, first three characters (provided they exist): All entries pass\n")
    else:
        print("Locus equality for v,d,j call, first three characters (provided they exist): WARNING >>>>>> locus equality does not hold for at least one sample\n")
    
    # TEST FOR LENGTH OF junction_aa AGAINST junction_aa_length
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("\n")
    print("Compare the length of junction entries against junction_length \n")
        
    # Get subfields with data 
    junction = sample_file["junction"].tolist()
    junction_len = sample_file["junction_length"].tolist()
    seq_id = sample_file["sequence_id"].tolist()
    sam_id = sample_file["sample_id"].tolist()
    produc = sample_file["productive"].tolist()
    # Zip them together for later use
    identify_samples = zip(junction,junction_len,seq_id,sam_id,produc)
    
    # Initialize arrays
    nan_to_str = []
    non_matching_lengths = []
    # Iterate over all sequences
    for item in identify_samples:
        #print(item[0],item[1])
        # Check if nan is listed under junction_aa and a numnber other than 0 is reported in junction_aa_length
        if type(item[0])==float:
            
            if math.isnan(item[0]):
            
                if '0'!=str(item[1]):
                    
                    nan_to_str.append(item)
                    

                else:
                    continue   
        else:
            if item[0]=='nan' or item[0]=='NaN':
                
                if '0'!=str(item[1]):
                    nan_to_str.append(item)
                    

                else:
                    continue   
                    
            elif str(len(item[0])) != str(item[1]):
                non_matching_lengths.append(item)           

    if len(nan_to_str)!= 0:
        print("WARNING: length of junction and junction_length need to be revised -" + str(len(nan_to_str))+ " possible NaN value(s) turned into string(s)")
        print("ACTION-------------------------------------------------------> Check " + "store_info_nan_to_str__id_" + options.sample_id + ".csv" + " for details")
        with open(str(TSV_Path) + "store_info_nan_to_str__id_" + options.sample_id + ".csv","w") as f:
            f.write("junction,junction_len,sequence_id,sample_id,productive,Base_URL\n")
            for item in nan_to_str:
                for it in item:
                    f.write(it + ",")
                f.write(str(options.base_url) + "\n")
            f.close
    else:
        print("No NaN turned into string found")

    print("\n")
    if len(non_matching_lengths)!= 0:
        print("WARNING: length of junction and junction_length need to be revised - " + str(len(non_matching_lengths)) + " non-matching non-null values found")
        print("ACTION-------------------------------------------------------> Check " + "store_info_non_matching_non_null__id_" + options.sample_id + ".csv" + " for details.")
        with open(str(TSV_Path) + "store_info_non_matching_non_null__id_" + options.sample_id + ".csv","w") as f:
            f.write("junction_aa,junction_aa_len,sequence_id,sample_id,productive,Base_URL\n")
            for item in non_matching_lengths:
                for it in item:
                    f.write(it + ",")
                f.write("\n")
            f.close
    else:
        print("All non-empty junction entries associated to sample pass this test")

# Store data into zipped file

    zip_file = zipfile.ZipFile(str(TSV_Path) + 'zipped_TSV_SampleID_' + str(sample_id) +  '.zip', 'w')
    zip_file.write(str(filename), compress_type=zipfile.ZIP_DEFLATED)
    zip_file.close()
    
#    os.remove(filename)
#     # Return success

    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("End of tests\n")
    sys.exit(0)
