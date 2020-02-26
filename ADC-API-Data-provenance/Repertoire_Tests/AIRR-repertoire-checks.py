######### PERFORMANCE TESTING PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: December 5 2019
######### LAST MODIFIED ON: February 17 2020

"""
This script gets airr-api ts response and stores into file - it subsequently performs Data Provenance at the repertoire level
"""

from curlairripa import *       # https://test.pypi.org/project/curlairripa/ 
import time                     # time stamps
import pandas as pd
from pandas.io.json import json_normalize # parse JSON response into pandas df 
import argparse                 # Input parameters from command line 
import os, sys
import airr
import numpy as np
import zipfile
import math

pd.set_option('display.max_columns', 500)


#### Section 1. Verify, read and parse files
# Test I can open file
def test_book(filename):
    
    """This function verifies whether it is possible to open a metadata EXCEL file. 
    
    It returns True if yes, False otherwise"""
    try:
        open_workbook(filename)
    except XLRDError:
        return False
    else:
        return True
    
# Report whether file can be opened or not
def verify_non_corrupt_file(master_metadata_file):
    
    """This function verifies whether test_book returns True or False and prints a message to screen in either case"""
    
    try:
        if test_book(master_metadata_file)==False:
            print("CORRUPT FILE: Please verify master metadata file\n")
            sys.exit()
        
        else:
            print("HEALTHY FILE: Proceed with tests\n")
    except:
        
        print("INVALID INPUT\nInput is an EXCEL metadata file.")
    
        
# Get appropriate metadata sheet        
def get_metadata_sheet(master_metadata_file):
    
    """This function extracts the 'metadata' sheet from an EXCEL metadata file """

    # Tabulate Excel file
    table = pd.ExcelFile(master_metadata_file)#,encoding="utf8")
    # Identify sheet names in the file and store in array
    sheets = table.sheet_names
    # How many sheets does it have
    number_sheets = len(sheets)

    ### Select metadata spreadsheet
    metadata_sheet = ""
    for i in range(number_sheets):
        # Check which one contains the word metadata in the title and hold on to it
        if "Metadata"== sheets[i] or "metadata"==sheets[i]:
            metadata_sheet = metadata_sheet + sheets[i]
            break 
        # Need to design test that catches when there is no metadata spreadsheet ; what if there are multiple metadata sheets?        
        
    # This is the sheet we want
    metadata = table.parse(metadata_sheet)
    
    return metadata

# Parse metadata sheet as pandas dataframe
def get_dataframes_from_metadata(master_MD_sheet):
    
    """This function parses the metadata EXCEL sheet into a pandas dataframe
    
    EXCEL metadata sheets normally have 2 headers: internal-use headers and AIRR header
    
    This function creates a pandas dataframe using only the AIRR headers. This is the dataframe
    
    that the sanity checks will be performed on"""
    
    try:
        # Get the appropriate sheet from EXCEL metadata file
        data_dafr = get_metadata_sheet(master_MD_sheet) 
        
        #grab the first row for the header
        new_header = data_dafr.iloc[0] 
        #take the data less the header row
        data_dafr = data_dafr[1:] 
        #set the header row as the df header
        data_dafr.columns = new_header 
    
        return data_dafr
    except:
        print("INVALID INPUT\nInput is a single variable containing path and name to metadata spreadsheet.")

# Section 2. Sanity Checking        
# Uniqueness and existence of field uniquely identifying each sample in metadata        
def check_uniqueness_ir_rearrangement_nr(master_MD_dataframe,unique_field_id):  
    
    """This function verifies that the unique field used to identify each sample exists and is unique in metadata"""
    
    try:
        print("Existence and uniquenes of " + str(unique_field_id) + " in metadata")

        # Check it exists
        if unique_field_id not in master_MD_dataframe.columns:
            print("WARNING: FIELD NAME DOES NOT EXIST TO UNIQUELY IDENTIFY SAMPLES IN THIS STUDY\n")
            print("Verify the column name exists and contains the correct information in your spreadsheet\n")
            sys.exit(0)

        else:
            # Check it is unique
            if pd.Series(master_MD_dataframe[unique_field_id]).is_unique==False:
                print("FALSE: There are duplicate entries under "+ str(unique_field_id) + " in master metadata\n")

            else:
                print("TRUE: All entries under  "+ str(unique_field_id) + "  in master metadata are unique\n")
    except:
        
        print("INVALID INPUT\nInput is a dataframe containing metadata and a field from metadata which uniquely identifies each sample.")

def get_unique_identifier(JSON_DATA_FILE,unique_field_id,ir_rear_number):
    
    """This function obtains the index corresponding to a sample found in API response
    
    This function uses the unique identifies that the user provided, the unique number associated to it
    
    As well as the JSON file name containing API response"""
    
    try:
        # Get total numnber of entries in JSON file containing API response
        no_iterations = len(JSON_DATA_FILE)
        
        # Set up array to store index
        JSON_index = []
        # Iterate over all entries
        for i in range(no_iterations):
            # Check unique identifier is found in the entry 
            if unique_field_id in JSON_DATA_FILE[i].keys():
                # Check value under unique identifier matches the unique identifier for that sample in metadata
                if JSON_DATA_FILE[i][unique_field_id]==ir_rear_number:
                   # if both conditions are met, append the index, otherwise the array will be empty
                    JSON_index.append(i) 

        return JSON_index
    except:
        print("INVALID DATA FORMAT\nEnter a JSON file from API response, a field name which uniquely identifies each sample and an entry uniquely identifying the sample.")

def rename_cols(flattened_sub_df,field_name):
    
    flattened_cols = flattened_sub_df.columns
    new_col_names = {item: str(field_name) + ".0." + str(item) for item in flattened_cols}
    flattened_sub_df = flattened_sub_df.rename(columns=new_col_names)
    
    return flattened_sub_df        
        

def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Output Directory - where Performance test results will be stored 
    parser.add_argument(
        "mapping_file",
        help="Indicate the full path to where the mapping file is found"
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
            "json_files",
        help="Enter full path to JSON queries"
    )
    
    parser.add_argument(
            "master_md",
        help="Full path to master metadata"
    )
    
    parser.add_argument(
            "study_id",
        help="Study ID"
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
    mapping_file = options.mapping_file
    base_url = options.base_url
    entry_pt = options.entry_point
    query_files = options.json_files
    master_md = options.master_md
    study_id = options.study_id
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
    json_response_df = json_normalize(query_dict)
    
    # Perform the query. Time it
    start_time = time.time()
    query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
    total_time = time.time() - start_time
    
     # Time
    print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    print("ELAPSED DOWNLOAD TIME (in seconds): %s" % (total_time))
    print("ELAPSED DOWNLOAD TIME (in minutes): %s" % (total_time/60))
    print("ELAPSED DOWNLOAD TIME (in hours): %s" % (total_time/3600))
    
    
    
    filename =  str(query_files.split("/")[-1].split(".")[0]) + "_OUT.json"
    json_data = parse_query(query_json,"./" + str(query_files.split("/")[-1].split(".")[0]))

    if entry_pt=="repertoire":
        
        print("In repertoire entry point",entry_pt)
    
        try:
            airr.load_repertoire("./" + filename,validate = True)
            print("Successful repertoire loading\n")
        except airr.ValidationError as err:  
            print("ERROR: AIRR repertoire validation failed for file %s - %s" %
                      (filename, err))
            print("\n")
        print("---------------------------------------------------------------------------------------------------------------------------------------------------")
    
    # Begin sanity checking 
    print("########################################################################################################")
    print("---------------------------------------VERIFY FILES ARE HEALTHY-----------------------------------------\n")
    print("---------------------------------------------Metadata file----------------------------------------------\n")
    # GET METADATA    
    if "xlsx" in master_md:
        verify_non_corrupt_file(master_md)
        master = get_dataframes_from_metadata(master_md)
    elif "csv" in master_md:
        master = pd.read_csv(master_md ,encoding='utf8')
        master = master.loc[:, ~master.columns.str.contains('^Unnamed')]

    # Get metadata and specific study
    master = master.replace('\n',' ', regex=True)
    master["study_id"] = master["study_id"].str.strip()

    data_df = master.loc[master['study_id'] == study_id]

    input_unique_field_id = "ir_rearrangement_number"
    # Check entries under unique identifier are  unique
    check_uniqueness_ir_rearrangement_nr(data_df,input_unique_field_id)

    if data_df.empty:
        print("EMPTY DATA FRAME: Cannot find specified study ID\n")
        print(data_df)
        sys.exit(0)

    no_rows = data_df.shape[0]
    
    
    # Mapping file
    map_csv = pd.read_csv(mapping_file,sep="\t",encoding="utf8")
    ir_adc_fields = map_csv["ir_adc_api_response"].tolist()
    ir_cur_fields = map_csv["ir_curator"].tolist()
    rep_metadata_f = ir_cur_fields[0:89]
    rep_mappings_f = ir_adc_fields[0:89]
    
    # API response
    DATA = airr.load_repertoire(filename)
    
    print("================================================")
    data_pro = json_normalize(data=DATA['Repertoire'], record_path='data_processing')
    data_pro = rename_cols(data_pro,"data_processing")
    #display(data_pro)
    print("================================================")
    sample = json_normalize(data=DATA['Repertoire'], record_path='sample')
    sample = rename_cols(sample,"sample")
    
    sample_0_cell_subset_value = [item['value'] for item in sample['sample.0.cell_subset'].to_list()]
    cell_subset_dic = pd.DataFrame({'sample.0.cell_subset.value':sample_0_cell_subset_value})
    
    pcr_target = json_normalize(DATA["Repertoire"],record_path=['sample','pcr_target'])
    pcr_target = rename_cols(pcr_target,"sample.0.pcr_target")
    
    subject = json_normalize(data=DATA['Repertoire'], record_path=["subject","diagnosis"])
    subject = rename_cols(subject,"subject.diagnosis")
    
    
    #print("================================================")
    repertoire = json_normalize(data=DATA['Repertoire'])
    #print("================================================")
    
    # Optional 
    concat_version = pd.concat([repertoire,data_pro,sample,cell_subset_dic,pcr_target,subject],1).drop(["data_processing","sample",'sample.0.cell_subset',
                                                                                                        'sample.0.pcr_target','subject.diagnosis'],1)
#     print("================================================")
#     print("Verify ADC API Response\n")
#     display(concat_version.head(1))
#     print("================================================")
#     print("================================================")
#     print("================================================")
    
    print("Cross comparison - field names\n")
    field_names_in_mapping_not_in_API = []
    field_names_in_mapping_not_in_MD = []
    in_both = []
    for f1,f2 in zip(rep_mappings_f,rep_metadata_f):
        if f1 not in concat_version.columns:
            field_names_in_mapping_not_in_API.append(f1)
        if f2 not in master.columns:
            field_names_in_mapping_not_in_MD.append(f2)

        if f1 in concat_version.columns and f2 in master.columns:
            in_both.append([f1,f2])
        
    print("---------------------------------------------------------------------------------------------------------------")
    print("Field names in mapping, ir_adc_api_response, not in API response\n")
    
    for item in field_names_in_mapping_not_in_API:
        print(item)
        
    print("---------------------------------------------------------------------------------------------------------------")
    print("Field names in mapping, ir_curator, not in metadata fields\n")
    for item in field_names_in_mapping_not_in_MD:
        print(item)
       
#     print("---------------------------------------------------------------------------------------------------------------")
#     print("Field names in both\n")
#     for item in in_both:
#         print(item)
        
    print("---------------------------------------------------------------------------------------------------------------")
    print("Content cross comparison\n")
    
    unique_items = data_df["ir_rearrangement_number"].to_list()
    
    for i in range(len(unique_items)):
        
        print("ir_rearrangement_numner",unique_items[i])
        
        rowAPI = concat_version[concat_version['data_processing.0.data_processing_id']==str(unique_items[i])]
        
        rowMD = data_df[data_df["ir_rearrangement_number"]==unique_items[i]]
        
        #print(rowAPI)

        for item in in_both:

            md_entry = rowMD[item[1]].to_list()#[0]
            API_entry = rowAPI[item[0]].to_list()#[0]

            if md_entry==API_entry:
                continue
            else:
                
                if len(API_entry)==0 or len(md_entry)==0:
                    
                    print(item)
                    print("metadata entrt: ", md_entry,"API response content", API_entry)
                    
                else:
                    if API_entry[0]==None and type(md_entry[0])==float:
                        continue
                    else:
                        print(item)
                        print("metadata entrt: ", md_entry,"API response content", API_entry)
            print("-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*\n")
        print("================================================")

            
        # concat as above
        # where for data_processing flaten, remove data_processing field name, and to each prepend data_processing.0. 
        # do the same for the sample
        # do the same to subject.diagnosis 
        
        # alternatively .... (might be bigger change)
        # do not flatten and iterate over each item in the DATA JSON structure 
        
        
        
