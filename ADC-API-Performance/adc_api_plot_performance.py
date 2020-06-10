######### PERFORMANCE TESTING PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: MAY 20, 2019
######### LAST MODIFIED ON: April 29, 2020
"""

Description: this script takes as input a set of .csv files containing performance query results and it outputs a series of plots comparing hourly time taken to finish query
for queries junction_aa, junction_aa_length, v_call (a family, a gene and an allele), d_call (a family, a gene and an allele) and j_call (a family, a gene and an allele) 

"""

import pandas as pd
import matplotlib.pyplot as plt
import os
import sys
import argparse
import numpy as np
import plotly.io as pio

#load the "cufflinks" library under the short name "cf"
import cufflinks as cf

import plotly.express as px

#command to display graphics correctly in a Jupyter notebook
cf.go_offline()

global all_ipa1,all_ipa2,all_ipa3,all_ipa4,all_ipa5,gold_ipa,all_the_data

def get_mix_col(df):
    fcf = df["filters.content.field"].to_list()
    fcv = df["filters.content.value"].to_list()
    df["filters.content.field: filters.content.value"] = [str(item[0]) + ": " + str(item[1])\
                                                         for item in zip(fcf,fcv)]
    
    return df

def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )
    # Output results directory
    parser.add_argument("path")
    # Start Date
    parser.add_argument("s_date")
    # End date
    parser.add_argument("e_date")
    # Verbosity flag
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Run the program in verbose mode.")
    # Unified scale flag
    parser.add_argument(
        "-s",
        "--scale",
        action="store_true",
        help="Scale the graphs so they are all on the same y-axis scale.")

    # Parse the command line arguements.
    options = parser.parse_args()
    return options       
        
    
if __name__ == "__main__":
    
    options = getArguments()
    
    
    path = options.path
    s_date = options.s_date
    e_date = options.e_date

    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(r, file))

    all_dfs = []
    for i in range(len(files)):
        #print(files[i])
        df = pd.read_csv(files[i],sep=',')
        df["IPA#"] = files[i].split("/")[2].split(".ireceptor.org.csv")[0].split("_")[-1]
        all_dfs.append(df)
        
    all_the_data = pd.concat([all_dfs[i] for i in range(len(files))],sort=False)
    
    all_the_data = all_the_data.drop(['Unnamed: 0'], 1)
    
    # Conver times from str to time stamps (compare when time is off by 1 second)
    all_data_right_times = pd.to_datetime(all_the_data['Date/Time']).dt.floor('T')
    all_the_data["Date/Time"]= all_data_right_times
    
    all_the_data = all_the_data.sort_values(by=['Date/Time'])
    
    all_the_data = all_the_data[(all_the_data["Date/Time"] > pd.Timestamp(str(s_date))) & (all_the_data["Date/Time"] < pd.Timestamp(str(e_date)))]
    
    allQueries = [item for item in all_the_data.columns if "TIME" in item]
    allQueries2 = [item for item in all_the_data.columns if "SEQUENCES" in item]
    allQueries3 = [item for item in all_the_data.columns if "SAMPLES" in item]
    
    family_calls = [item for item in allQueries if "f" in item]
    gene_calls = [item for item in allQueries if "g" in item]
    allele_calls = [item for item in allQueries if "a" in item]
    
    all_ipa1 = all_the_data[(all_the_data["IPA#"]=="ipa1-staging") | (all_the_data["IPA#"]=="ipa1")]
    all_ipa2 = all_the_data[(all_the_data["IPA#"]=="ipa2-staging") | (all_the_data["IPA#"]=="ipa2")]
    all_ipa3 = all_the_data[(all_the_data["IPA#"]=="ipa3-staging") | (all_the_data["IPA#"]=="ipa3")]
    all_ipa4 = all_the_data[(all_the_data["IPA#"]=="ipa4-staging") | (all_the_data["IPA#"]=="ipa4")]
    all_ipa5 = all_the_data[(all_the_data["IPA#"]=="airr-ipa5") | (all_the_data["IPA#"]=="ipa5")]
    gold_ipa = all_the_data[all_the_data["IPA#"]=="ipa5_airr"]
    
    t1 = all_ipa1[["filters.content.field","filters.content.value","TimeTaken(s)","Date/TimeConverted"]]
    t2 = all_ipa2[["filters.content.field","filters.content.value","TimeTaken(s)","Date/TimeConverted"]]
    t3 = all_ipa3[["filters.content.field","filters.content.value","TimeTaken(s)","Date/TimeConverted"]]
    t4 = all_ipa4[["filters.content.field","filters.content.value","TimeTaken(s)","Date/TimeConverted"]]
    t5 = all_ipa5[["filters.content.field","filters.content.value","TimeTaken(s)","Date/TimeConverted"]]
    
    t1 = get_mix_col(t1)
    t1["Service"] = "IPA1"

    t2 = get_mix_col(t2)
    t2["Service"] = "IPA2"

    t3 = get_mix_col(t3)
    t3["Service"] = "IPA3"

    t4 = get_mix_col(t4)
    t4["Service"] = "IPA4"

    t5 = get_mix_col(t5)
    t5["Service"] = "IPA5"
    
    test = pd.concat([t1,t2,t3,t4,t5],sort=False)
    
    full_fig  = px.line(test,
           y="TimeTaken(s)",
           x="Date/TimeConverted",
           color='filters.content.field: filters.content.value',range_y=[0,500],
       facet_col="Service")
    pio.write_html(full_fig,"ADC_API_performance_" + str(s_date) + "_" + str(e_date) + ".html",
              auto_open=True)

    hmap = px.density_heatmap(test,
           y="TimeTaken(s)",
           x="filters.content.field: filters.content.value",
                  marginal_x="rug", marginal_y="histogram",template="plotly_dark",facet_col="Service")
    pio.write_html(hmap,"HMAP_ADC_API_performance_" + str(s_date) + "_" + str(e_date) + ".html",
              auto_open=True)
