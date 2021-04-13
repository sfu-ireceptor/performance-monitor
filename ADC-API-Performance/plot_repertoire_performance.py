import pandas as pd
import matplotlib.pyplot as plt
import os
import sys
import argparse
import numpy as np
import plotly.io as pio
import plotly.express as px
import sys
import csv
import math

csv.field_size_limit(sys.maxsize)

def clean_df(df):
     #Remove cols 
    if "Repertoire" in df.columns:
        df.drop("Repertoire",1,inplace=True)
    
    if "Unnamed: 0" in df.columns: 
        df.drop(['Unnamed: 0'], 1,inplace=True)
    
    # Add variable num
    if "Query#" not in df.columns:
        df['Query#'] = [i for i in range(df.shape[0])]
    
    return df

def plot_performance(df,name,output_dir):

    df  = px.line(df,
                   y="TimeTaken(s)",
                   x="Date/TimeConverted",
                color="QueryName",
               facet_col="IPA#",title="Time taken per query",hover_name="IPA#",labels={"Date/TimeConverted":"Date","TimeTaken(s)": "Time (s)"})

    pio.write_html(df,output_dir+str(name) + "_TimeSeries_.html",
                      auto_open=True)

def parse_df_content(df,s_date,e_date):
    
    # Concatenate array with df
    new_df = pd.concat(df)
    # Sort values by date and time
    new_df.sort_values(by=['Date/TimeConverted'],inplace=True)
    # Get only desired dates
    new_df = new_df[(new_df['Date/TimeConverted']>=s_date) & (new_df['Date/TimeConverted']<=e_date)]
    # Remove NA values
    new_df.dropna(inplace=True)
    # Query processing 
    new_df['QueryName'] = new_df['QueryName'].str.replace("/home/ubuntu/ireceptor-monitor/ADC_API_Testing/JSON/repertoire_selected_queries/","").to_list()
    return new_df


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
    #Output dir
    parser.add_argument("output_dir")
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
    
    # Read arguments
    options = getArguments()
    
    path = options.path
    s_date = options.s_date
    e_date = options.e_date
    output_dir = options.output_dir

    files = []
        # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(r, file))
                
    airr_seq_df = []
    ireceptor_df = []
    vdjserver_df = []
    covid_df = []
    irec_df = []
    external_df = []
    for i in range(len(files)):

        #print(files[i])
        df = pd.read_csv(files[i],sep=',', engine='python' ,encoding='utf-8', error_bad_lines=False)
        if "ireceptor" in files[i] and "covid" not in files[i]:
            df["IPA#"] = files[i].split("/")[-1].split(".ireceptor.org.csv")[0].split("_")[-1]
            # Remove odd col, remove Repertoire col, add query col
            clean_df(df)
            # Save new file
            df.to_csv(files[i])
            # Add to list of files
            ireceptor_df.append(df)
        if "vdjserver" in files[i]:
            df["IPA#"] = files[i].split("/")[-1].split("vdjserver.org.csv")[0].split("_")[-2]
            # Remove odd col, remove Repertoire col, add query col
            clean_df(df)
            # Save new file
            df.to_csv(files[i])
            vdjserver_df.append(df)
        if "airr-seq" in files[i]:
            df["IPA#"] = files[i].split("/")[-1].split("airr-seq.vdjbase.org.csv")[0].split("_")[-2]
            # Remove odd col, remove Repertoire col, add query col
            clean_df(df)
            # Save new file
            df.to_csv(files[i])
            airr_seq_df.append(df)
        if "covid" in files[i]:
            df['IPA#'] = files[i].split("/")[-1].split("airr-seq.vdjbase.org.csv")[0].split("_")[-2]
            # Remove odd col, remove Repertoire col, add query col
            clean_df(df)
            # Save new file
            df.to_csv(files[i])
            covid_df.append(df)
            
        if "irec_irec" in files[i]:
            df["IPA#"] = files[i].split("/")[-1].split("irec.")[0].split("_")[-2]
            # Remove odd col, remove Repertoire col, add query col
            clean_df(df)
            # Save new file
            df.to_csv(files[i])
            irec_df.append(df)
            
    print(len(airr_seq_df))
        
    # Process each dataframe
    airr_full_df = parse_df_content(airr_seq_df,s_date,e_date)
    ireceptor_full_df = parse_df_content(ireceptor_df,s_date,e_date)
    vdjserver_full_df = parse_df_content(vdjserver_df,s_date,e_date)
    covid_full_df = parse_df_content(covid_df,s_date,e_date)
    irec_full_df = parse_df_content(irec_df,s_date,e_date)
    external_full_df = pd.concat([vdjserver_full_df,airr_full_df,irec_full_df])
    
    # Generate plots
    plot_performance(ireceptor_full_df,"iReceptor",output_dir)
    plot_performance(covid_full_df,"COVID19",output_dir)
    plot_performance(external_full_df,"ExternalServices",output_dir)