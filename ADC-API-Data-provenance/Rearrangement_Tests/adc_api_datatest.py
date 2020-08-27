######### PERFORMANCE TESTING PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: December 5 2019
######### LAST MODIFIED ON: August 25 2020

"""
This script downloads TSV from airr-api and stores into file - it subsequently performs Data Provenance tests 
"""

from curlairripa import *       # https://test.pypi.org/project/curlairripa/ 
import time                     # time stamps
import pandas as pd
import argparse                 # Input parameters from command line 
import os, sys
import airr
import numpy as np
import zipfile
import math
import string


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

def alphabet_to_check(nucleotide_base_list):
    '''
    This function takes a list of characters from the alphabet, and returns a list with characters from the alphabet which exclude the ones provided
    
    Args:
    
        nucleotide_base_list (list): contains letters in the alphabet corresponding to nucleotide bases
        
    Returns:
    
        DNA_nucleo_list (list): list containing all letters in the alphabet except those provided by the user 
    '''
    
    try:

        alphabet_string = string.ascii_lowercase
        alphabet_list = [str(i) for i in alphabet_string]
        alphabet_set = set(alphabet_list)
        nucleotide_base_list = [x.lower() for x in nucleotide_base_list]
        DNA_nucleo_set = alphabet_set.difference(set(nucleotide_base_list))
        DNA_nucleo_list = list(DNA_nucleo_set)

        return DNA_nucleo_list
    except:
        print("WARNING, expected a list containing nucleotide bases")
        print("nucleotide_base_list arg returns:", nucleotide_base_list)


def check_field_is_non_empty(dataframe,field,output_dir,rep_id):
    '''
    Function that checks whether a field contains empty entries
    
    Args:
        
        dataframe (pandas dataframe object): contains an AIRR compliant dataframe for rearrangement data
        field (string): name of field to be tested
        output_dir (string): path where output file should be stored
        rep_id (string): repertoire_id being tested 
        
    Returns: 
    
        test_result (bool): True if no NaN entries were found, False otherwise 
    '''

    if dataframe[field].isna().sum()==0:
        
        test_result = True
        
        print("PASS",str(field),"has no empty entries.")
    else:
        
        test_result=False
        
        print("WARNING",str(field),"has empty entries.")
        print("Generating sequence_id and repertoire_id list with problems")
        dt = dataframe[dataframe[field].isna()==True][[field,'sequence_id','repertoire_id']]
        
        dt.to_csv("NAN_entries_in_" + str(field) +"_" + str(rep_id) + ".csv")
        
    return test_result 


        
def sequence_check_against_DNA_nb(dataframe,field,nucleotide_base_list,output_dir,rep_id):
    '''
    This function checks whether odd characters (characters not found in the DNA nucleotide bases)
    for a given field, typically sequences and generates a CSV with problematic entries and id information
    
    Args:
    
        dataframe (pandas dataframe object): contains an AIRR compliant dataframe for rearrangement data
        field (string): name of field to be tested
        nucleotide_base_list (list): list containing characters (typically 'a','c','t','g')
        output_dir (string): path where output file should be stored
        rep_id (string): repertoire_id being tested 
    
    Returns:
        None
    
    '''
    try:
    
        # Create non-nb characters list    
        DNA_nucleo_list = alphabet_to_check(nucleotide_base_list)

            # Turn entries into lower case
        sample_file_seq = dataframe[field].str.lower()

            # Build df with problematic entries
        all_dfs = []

            # Iterate over non-nb character list
        for i in range(len(DNA_nucleo_list)):
                # Check if non-nb characters are present
            df = dataframe[sample_file_seq.str.contains(DNA_nucleo_list[i])][[field,'sequence_id','repertoire_id']]
            all_dfs.append(df)

        final_df = pd.concat(all_dfs)

        if final_df.empty == True:

            print("DNA Nucleotide Base Check Pass")
        else:
            print("WARNING: DNA Nucleotide Base Check Fails")
            print("Check","NAN_entries_in_" + str(field) +"_" + str(rep_id) + ".csv","for details")

            dt.to_csv(str(output_dir) + "DNA_nucleotide_base_errors_in_" + str(field) +"_" + str(rep_id) + ".csv")

        return None
    except:
        print("WARNING: provide a dataframe object containing AIRR compliant content, a field in AIRR Rearrangement, a list with NB, an output directory,\
        and the corresponding repertoire id")
        print("Received df object containing:", type(dataframe))
        print("Received field name:",field)
        print("Received NB list:",nucleotide_base_list)
        print("Received output directory:",output_dir)
        print("Received repertoire id:",rep_id)

def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Output Directory - where Performance test results will be stored 
    parser.add_argument(
        "output_dir",
        help="Indicate the full path to where data provenance test results will be stored"
    )
    
    # Word search
    parser.add_argument("words",type=str,
                    help='Comma-separated strings containing odd words to search for')
    
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
            "json_files",
        help="Enter full path to JSON queries"
    )
    
    parser.add_argument(
            "rep_id",
        help="Repertoire id"
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
    
    print("DATA PROVENANCE TEST \n")
    # Input reading
    options = getArguments()
    output_dir = options.output_dir
    odd_words = options.words
    base_url = options.base_url
    entry_pt = options.entry_point
    query_files = options.json_files
    rep_id = options.rep_id
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
    
    # Turn response into pandas dataframe 
    json_response_df = pd.json_normalize(query_dict)
    
    # Perform the query. Time it
    start_time = time.time()
    query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
    total_time = time.time() - start_time
    
     # Time
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("ELAPSED DOWNLOAD TIME (in seconds): %s" % (total_time))
    print("ELAPSED DOWNLOAD TIME (in minutes): %s" % (total_time/60))
    print("ELAPSED DOWNLOAD TIME (in hours): %s" % (total_time/3600))
    
    # Write TSV to file
    filename = output_dir + "TSV_" + str(query_files.split("/")[-1].split(".")[0])
    parse_query(query_json, filename)
    
    # Get filesize
    file_size = os.path.getsize(str(filename)+  ".tsv")
    print("FILE SIZE (in MB): " + str(file_size/1000000))
   
        
    # Parse TSV as pandas dataframe
    sample_file = pd.read_csv(str(filename) + ".tsv",sep="\t",low_memory=False)
    
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
    
    # Track how long it took to download sequences (seq per sequence)
    print("NUMBER OF SEQUENCES: " + str(sample_file.shape[0]))
    if sample_file.shape[0]==0:
        print("(AVERAGE) ELAPSED TIME PER SEQUENCE (in seconds per sequence): " + str(sample_file.shape[0]) + "\n")
    else:
        print("(AVERAGE) ELAPSED TIME PER SEQUENCE (in seconds per sequence): " + str(total_time/sample_file.shape[0]) + "\n")
    
    
    # Validate TSV file
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("TSV AIRR STANDARD TEST")
    if airr.validate_rearrangement(filename + ".tsv", False)==True:
        print("File passes AIRR standards\n")
    else:
        print("WARNING: file does not pass AIRR standards\n")
    
    # Run odd words test - print test result interpretation
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("Checking for odd words\n")
    print("Case  \t Any  \t All  \t Meaning ")
    print("----------------------------------------------------------------------------------------")
    print("1 \t True \t True \t A exists and A is in all entries ")
    print("2 \t True \t False \t A exists but there is at least one entry where A is not found  ")
    print("3 \t False \t True \t Empty dataframe  ")
    print("4 \t False \t False \t A does not exist in any of the entries  \n")
    print("Field\tWord\tAny \tAll")
    
    # Get all values from input and iterate over them
    values = odd_words.split(',')
    for item in values:
        # Apply odd words test 
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
    sam_id = sample_file["repertoire_id"].tolist()
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

    # Junction length test results
    # Case 1) At least one NaN value found
    if len(nan_to_str)!= 0:
        print("WARNING: length of junction and junction_length need to be revised -" + str(len(nan_to_str))+ " possible NaN value(s) turned into string(s)")
        print("ACTION-------------------------------------------------------> Check " + "store_info_nan_to_str__id_" + rep_id + ".csv" + " for details")
        with open(str(output_dir) + "store_info_nan_to_str__id_" + rep_id + ".csv","w") as f:
            f.write("junction,junction_len,sequence_id,sample_id,productive,Base_URL\n")
            for item in nan_to_str:
                for it in item:
                    f.write(str(it) + ",")
                f.write(str(options.base_url) + "\n")
            f.close
    else:
        print("No NaN turned into string found")

    print("\n")
    # Case 2) Non nans, but integer values do not match 
    if len(non_matching_lengths)!= 0:
        print("WARNING: length of junction and junction_length need to be revised - " + str(len(non_matching_lengths)) + " non-matching non-null values found")
        print("ACTION-------------------------------------------------------> Check " + "store_info_non_matching_non_null__id_" +rep_id + ".csv" + " for details.")
        with open(str(output_dir) + "store_info_non_matching_non_null__id_" + rep_id + ".csv","w") as f:
            f.write("junction,junction_len,sequence_id,sample_id,productive,Base_URL\n")
            for item in non_matching_lengths:
                for it in item:
                    f.write(it + ",")
                f.write("\n")
            f.close
    else:
        print("All non-empty junction entries associated to sample pass this test")
        
        
    # Productive Test - check productive field is not NaN 

    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("End of tests\n")
    
    # TEST empty values under sequences
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("\n")
    print("Checking for empty entries under sequences field \n")
    check_field_is_non_empty(sample_file,"sequence",output_dir,rep_id)
    
    # TEST odd characters in sequences field
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("\n")
    print("Checking for empty entries under sequences field \n")
    sequence_check_against_DNA_nb(sample_file,"sequence",['a','c','t','g','n'],output_dir,rep_id)
    
    sys.exit(0)
    
