import pandas as pd
import argparse
import os
import sys
import math
import glob

def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Sequence file path
    parser.add_argument("TSV_dir",
                    help='Full path where TSV files are stored')
    # _id
    parser.add_argument("_id", 
                         help='Corresponds to value found in _id API iReceptor Service value')
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
    
    # Get the command line arguments.
    options = getArguments()
    TSV_Path = options.TSV_dir
    _id = options._id
    
    filename  =  options.TSV_dir + "sequences_data_sampleid_" + str(_id ) + "_.tsv"

    
    df = pd.read_csv(filename,sep="\t")

        
    productive_values  = df["productive"].tolist()
    seq_id = df["sequence_id"].tolist()
    sam_id = df["sample_id"].tolist()
    rear_id = df["rearrangement_id"].tolist()
    tsv_id = [_id for i in range(len(rear_id))]
        
    identify_samples = zip(productive_values,seq_id,sam_id,rear_id,tsv_id)
        
    productive_problem = []
        
    for item in identify_samples:
        if math.isnan(item[0]):
            productive_problem.append(item)
        else:
            continue
            
            
    with open(str(TSV_Path) + "productive_problem__id_" + str(_id) + ".csv","w") as f:
        f.write("productive,sequence_id,sample_id,rearrangement_id,_id\n")
        for item in productive_problem:
            for it in item:
                f.write(str(it) + ",")
            f.write("\n")
        f.close
                


        
    
