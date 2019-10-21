######### PERFORMANCE TESTING PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: MAY 20, 2019
######### LAST MODIFIED ON: September 5, 2019

"""

Description: this script takes as input a set of .csv files containing performance query results and it outputs a series of plots comparing hourly time taken to finish query
for queries junction_aa, junction_aa_length, v_call (a family, a gene and an allele), d_call (a family, a gene and an allele) and j_call (a family, a gene and an allele) 

"""

import pandas as pd
import matplotlib.pyplot as plt
import os
import sys
import argparse
import matplotlib.cm as cm
import numpy as np
from matplotlib.cm import get_cmap
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.pyplot import figure
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

global all_ipa1,all_ipa2,all_ipa3,all_ipa4,all_ipa5,all_the_data

def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )
    # Output results directory
    parser.add_argument("path")
    # Verbosity flag
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Run the program in verbose mode.")

    # Parse the command line arguements.
    options = parser.parse_args()
    return options       
        


def plot_all_query_time(sub_queries,plot_name):

    # Set seaborn style
    plt.style.use('seaborn-darkgrid')
    # Get colour map
    name = "Accent"
    cmap = get_cmap(name)  
    # Create a colour palette with the size of the array containing queries done
    colors = cm.tab10(np.linspace(0, 1, len(sub_queries)))
    # Set up different line styles - not enough colours in the palette used above
    linestyles = ['-', '--', '-.', ':']
    
    # Initialize figure
    fig = plt.figure(figsize=(25,10))
    # Create subplots layout size - this will create 5 plots along the x axis
    la_int = 150
    
    # Iterate over subdataframes containing query time data for IPA1-IPA5
    for df,xla in zip([all_ipa1,all_ipa2,all_ipa3,all_ipa4,all_ipa5],[1,2,3,4,5]):
    # Indicate which subplot to focus on
        gh1 = fig.add_subplot(la_int + xla)
    
        i = 0

        # Colour performance as a time series, iterate over each query and add a colour per query
        for item,c in zip(sub_queries,colors):
        # If i is even, add a straight line pattern to the time series
            if i%2==0:
                plt.plot(df["Date/Time"],df[item],label=item,linestyle=linestyles[0], color=c)
        # If i is odd, add a dash line pattern to the time series
            else:
                plt.plot(df["Date/Time"],df[item],label=item,linestyle=linestyles[1], color=c)
        # Increment i by 1
            i +=1
        # Make the plot more readable - add title, legend, x,y labels 
            plt.title("Time taken per query, \n" + str(plot_name) + " (IPA#" +str(xla) + ", \nhourly basis AUG 29 -  SEP 05(UTC))",fontsize=15)
            plt.legend(bbox_to_anchor=(1, 1),bbox_transform=plt.gcf().transFigure)
            plt.ylabel("Time Query Took (seconds)",fontsize=15)
            plt.xlabel("Time",fontsize=15)
            plt.xticks(rotation=90)
            start, end = plt.xlim()
            stepsize=1
            plt.xticks(np.arange(start, end, stepsize))

    # Tight layout to ensure all plots fit in the png file        
    plt.tight_layout()
    # Save figures in file
    plt.savefig('QueryTimes(AllQueries_' + str(plot_name) + ')__Aug29_Sep05.png',dpi='figure')
    # Display figures in the screen 
    plt.show()
    

def plot_stats_ipa_query(ipa_df,arr,option,ipa_name):
    """This function takes as input a dataframe with query times (either all_the_data, or all_ipa1,...,all_ipa5), 
    an array containing query names, and an integer 0 or 1, 0 to label accodring to queries, 1 to label according to ipa's"""

    # Initialize figure    
    fig = plt.figure(figsize=(25,15))
    # Create subplots layout size - this will create 8 plots along the x axis
    la_int = 180

        # One plot per iteration
    for i in range(1,8):

        # Indicate subplot to focus on
        gh1 = fig.add_subplot(la_int + i)
        # Pivot full dataframe containing all query times for IPA1-IPA5 - note this function works with all_ipa1, all_ipa2, all_ipa3, all_ipa4, all_ipa5 , values will be 
        # queries (it can either contain time taken to complete query, number of sequences where the query was found, or number of samples where the query was found)
        pd3 = ipa_df.pivot(index='Date/Time', columns='IPA#', values=arr[3*i-3:3*i])
        # Remove empty entries - this occurs when parsing a batch that has not been finalized - usually at the last line of the dataframe
        pd4 = pd3.dropna()
        # Set 5 colours - one for each IPA (if you do a single ipa dataset, it will use the first 3)
        my_colors = ['b', 'g', 'r', 'c', 'm']*3#'y', 'k', 'w']*3 
        # Get levels for a more appropriate legend name
        queries= pd4.columns.levels[0]
        # Query names
        name = queries[0] + ",\n "+  queries[1] + ",\n " + queries[2]
        # Use plot method to generate figure 
        pd4.plot(figsize=(30,8),color=my_colors,ax=gh1,title='Query time (seconds) - ' + str(ipa_name)+ '\n Selected Queries:\n ' + name).legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
        # Add legend - this can be changed manually 
        gh1.legend(pd4.columns.levels[option]);

    fig.savefig('QueryTimes(SelectedQueries_' +str(ipa_name) + 'OnePlot)__Aug29_Sep05.png') 
    plt.show() 
    
    
if __name__ == "__main__":
    
    options = getArguments()
    
    
    path = options.path

    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(r, file))


#     new_header  =['','Date/Time','IPA#','NUMBERSAMPLES(TOTAL)','NUMBERSEQUENCES(TOTAL)','NUMBERSAMPLES(junction_aa=LLTL)','NUMBERSEQUENCES(junction_aa=LLTL)','TIME(junction_aa=LLTL)','NUMBERSAMPLES(junction_aa_length=9)','NUMBERSEQUENCES(junction_aa_length=9)','TIME(junction_aa_length=9)','NUMBERSAMPLES(junction_aa_length=15)','NUMBERSEQUENCES(junction_aa_length=15)','TIME(junction_aa_length=15)','NUMBERSAMPLES(v_call = IGHV1 f)','NUMBERSEQUENCES(v_call = IGHV1 f)','TIME(v_call = IGHV1 f)','NUMBERSAMPLES(v_call = IGHV1-69 g)','NUMBERSEQUENCES(v_call = IGHV1-69 g)','TIME(v_call = IGHV1-69 g)','NUMBERSAMPLES(v_call = IGHV1-18*01 a)','NUMBERSEQUENCES(v_call = IGHV1-18*01 a)','TIME(v_call = IGHV1-18*01 a)','NUMBERSAMPLES(v_call = TRBV20 f)','NUMBERSEQUENCES(v_call = TRBV20 f)','TIME(v_call = TRBV20 f)','NUMBERSAMPLES(v_call = TRBV20-1 g)','NUMBERSEQUENCES(v_call = TRBV20-1 g)','TIME(v_call = TRBV20-1 g)','NUMBERSAMPLES(v_call = TRBV20-1*01  a)','NUMBERSEQUENCES(v_call = TRBV20-1*01  a)','TIME(v_call = TRBV20-1*01  a)','NUMBERSAMPLES(d_call = IGHD1 f)','NUMBERSEQUENCES(d_call = IGHD1 f)','TIME(d_call = IGHD1 f)','NUMBERSAMPLES(d_call = IGHD1-1 g)','NUMBERSEQUENCES(d_call = IGHD1-1 g)','TIME(d_call = IGHD1-1 g)','NUMBERSAMPLES(d_call = IGHD1-1*01 a)','NUMBERSEQUENCES(d_call = IGHD1-1*01 a)','TIME(d_call = IGHD1-1*01 a)','NUMBERSAMPLES(d_call = TRBD1 f)','NUMBERSEQUENCES(d_call = TRBD1 f)','TIME(d_call = TRBD1 f)','NUMBERSAMPLES(d_call = TRBD1 g)','NUMBERSEQUENCES(d_call = TRBD1 g)','TIME(d_call = TRBD1 g)','NUMBERSAMPLES(d_call = TRBD1*01 a)','NUMBERSEQUENCES(d_call = TRBD1*01 a)','TIME(d_call = TRBD1*01 a)','NUMBERSAMPLES(j_call = IGHJ4 f)','NUMBERSEQUENCES(j_call = IGHJ4 f)','TIME(j_call = IGHJ4 f)','NUMBERSAMPLES(j_call = IGHJ4 g)','NUMBERSEQUENCES(j_call = IGHJ4 g)','TIME(j_call = IGHJ4 g)','NUMBERSAMPLES(j_call = IGHJ4*02 a)','NUMBERSEQUENCES(j_call = IGHJ4*02 a)','TIME(j_call = IGHJ4*02 a)','NUMBERSAMPLES(j_call = TRBJ2 f)','NUMBERSEQUENCES(j_call = TRBJ2 f)','TIME(j_call = TRBJ2 f)','NUMBERSAMPLES(j_call = TRBJ2-7 g)','NUMBERSEQUENCES(j_call = TRBJ2-7 g)','TIME(j_call = TRBJ2-7 g)','NUMBERSAMPLES(j_call = TRBJ2-7*01 a)','NUMBERSEQUENCES(j_call = TRBJ2-7*01 a)',"TIME(j_call = TRBJ2-7*01 a)"]

#     all_dfs = []
#     for i in range(len(files)):
#         df = pd.read_csv(files[i],sep=',')
#         #df.to_csv(files[i], header=new_header)


#     files = []
#     # r=root, d=directories, f = files
#     for r, d, f in os.walk(path):
#         for file in f:
#             if '.csv' in file:
#                 files.append(os.path.join(r, file))

    all_dfs = []
    for i in range(len(files)):
        #print(files[i])
        df = pd.read_csv(files[i],sep=',')

        all_dfs.append(df)
        
    all_the_data = pd.concat([all_dfs[i] for i in range(len(files))],sort=False)
    
    all_the_data = all_the_data.drop(['Unnamed: 0'], 1)
    
    # Conver times from str to time stamps (compare when time is off by 1 second)
    all_data_right_times = pd.to_datetime(all_the_data['Date/Time']).dt.floor('T')
    all_the_data["Date/Time"]= all_data_right_times
    
    all_the_data = all_the_data.sort_values(by=['Date/Time'])
    
    allQueries = [item for item in all_the_data.columns if "TIME" in item]
    allQueries2 = [item for item in all_the_data.columns if "SEQUENCES" in item]
    allQueries3 = [item for item in all_the_data.columns if "SAMPLES" in item]
    
    family_calls = [item for item in allQueries if "f" in item]
    gene_calls = [item for item in allQueries if "g" in item]
    allele_calls = [item for item in allQueries if "a" in item]
    
    all_ipa1 = all_the_data[all_the_data["IPA#"]=="ipa1"]
    all_ipa2 = all_the_data[all_the_data["IPA#"]=="ipa2"]
    all_ipa3 = all_the_data[all_the_data["IPA#"]=="ipa3"]
    all_ipa4 = all_the_data[all_the_data["IPA#"]=="ipa4"]
    all_ipa5 = all_the_data[all_the_data["IPA#"]=="ipa5"]

    
    v_calls = ['TIME(v_call = IGHV1 f)',
     'TIME(v_call = IGHV1-69 g)',
     'TIME(v_call = IGHV1-18*01 a)',
     'TIME(v_call = TRBV20 f)',
     'TIME(v_call = TRBV20-1 g)',
     'TIME(v_call = TRBV20-1*01  a)']
    d_calls = ['TIME(d_call = IGHD1 f)',
     'TIME(d_call = IGHD1-1 g)',
     'TIME(d_call = IGHD1-1*01 a)',
     'TIME(d_call = TRBD1 f)',
     'TIME(d_call = TRBD1 g)',
     'TIME(d_call = TRBD1*01 a)']
    j_calls = ['TIME(j_call = IGHJ4 f)',
     'TIME(j_call = IGHJ4 g)',
     'TIME(j_call = IGHJ4*02 a)',
     'TIME(j_call = TRBJ2 f)',
     'TIME(j_call = TRBJ2-7 g)',
     'TIME(j_call = TRBJ2-7*01 a)']
    
    # IPA STATS
    
    num_seq_tot_ipa1 = max(all_ipa1["NUMBERSEQUENCES(TOTAL)"].unique())
    num_seq_tot_ipa2 = max(all_ipa2["NUMBERSEQUENCES(TOTAL)"].unique())
    num_seq_tot_ipa3 = max(all_ipa3["NUMBERSEQUENCES(TOTAL)"].unique())
    num_seq_tot_ipa4 = max(all_ipa4["NUMBERSEQUENCES(TOTAL)"].unique())
    num_seq_tot_ipa5 = max(all_ipa5["NUMBERSEQUENCES(TOTAL)"].unique())

    num_sam_tot_ipa1 = max(all_ipa1["NUMBERSAMPLES(TOTAL)"].unique())
    num_sam_tot_ipa2 = max(all_ipa2["NUMBERSAMPLES(TOTAL)"].unique())
    num_sam_tot_ipa3 = max(all_ipa3["NUMBERSAMPLES(TOTAL)"].unique())
    num_sam_tot_ipa4 = max(all_ipa4["NUMBERSAMPLES(TOTAL)"].unique())
    num_sam_tot_ipa5 = max(all_ipa5["NUMBERSAMPLES(TOTAL)"].unique())


    labels = 'IPA1', 'IPA2', 'IPA3', 'IPA4', 'IPA5'
    sequences = [num_seq_tot_ipa1,num_seq_tot_ipa2,num_seq_tot_ipa3,num_seq_tot_ipa4,num_seq_tot_ipa5]
    samples = [num_sam_tot_ipa1,num_sam_tot_ipa2,num_sam_tot_ipa3,num_sam_tot_ipa4,num_sam_tot_ipa5]
    explode_seq = (0, 0, 0, 0,0.1)  
    explode_sam = (0.1, 0, 0, 0,0)  

    #fig1, ax1 = plt.subplots(figsize=(10,10))
    fig, (ax1, ax2) = plt.subplots(1, 2,figsize=(10,6))
    fig.suptitle('Distribution of total number of sequences (left) \nand samples (right) per IPA',fontsize=25)
    #ax1. title("Distribution of total number of sequences per IPA",fontsize=25)
    ax1.pie(sequences, explode=explode_seq, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    ax2.pie(samples, explode=explode_sam, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax2.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    plt.savefig('TotalSequenceSampleDistributionALLIPAS.png')
    plt.show()
    
    x = ["IPA" + str(i) for i in range(1,6)]
    fig1, (ax3,ax4) = plt.subplots(1,2,figsize=(10,6))
    fig1.suptitle("Total number of sequences (left)\n and samples (right) per IPA\n",fontsize=20)
    ax3.set_xlabel("IPA Number")
    ax3.set_ylabel("Number of sequences")
    ax3.bar(x, sequences)

    ax4.set_xlabel("IPA Number")
    ax4.set_ylabel("Number of samples")
    ax4.bar(x, samples)

    plt.savefig('TotalSequenceSamplesALLIPAS.png')
    plt.show()

    
    #BOXPLOT
    fig = plt.figure(figsize=(45,45))

    for i in range(len(allQueries)):
        plt.subplot(7, 3, i+1)
        data = [all_ipa1[allQueries[i]],all_ipa2[allQueries[i]],all_ipa3[allQueries[i]],all_ipa4[allQueries[i]],all_ipa5[allQueries[i]]]
        plt.boxplot(data);
        plt.xlabel("IPA#")
        plt.ylabel("Query time (seconds)")
        plt.title("BoxPlot Query time (seconds) per IPA. \nQuery: " + str(allQueries[i]))
    plt.savefig('BoxplotQueryTime(seconds)perIPA(ALLQUERIES)).png')
    plt.show()
    
    # TIME SERIES (all queries, one plot per IPA)
    
    plot_all_query_time(allQueries,"all queries")
          
        
    # TIME SERIES (all ipas, one plot per query)
    plot_stats_ipa_query(all_the_data,allQueries,1,"ALL_IPAS")
            
#     for i in range(len(allQueries)):

#     # Overwrite the label by calling the method.

#         fig = plt.figure(figsize=(25,4))
#         la_int = 150
#         for xla in [1,2,3,4,5]:
#             gh1 = fig.add_subplot(la_int + xla)
#             for col,item in zip(['b', 'g', 'r', 'c','y'],[all_ipa1,all_ipa2,all_ipa3,all_ipa4,all_ipa5]):

#                 pd.pivot_table(item, index='Date/Time', values= allQueries[i], aggfunc= [np.mean])\
#                 .plot(color=col,ax=gh1,title="Average Daily Query Time All IPAs",label="IPA#" + str(xla)).legend(bbox_to_anchor=(0.8, 1.5))
#                 labels = gh1.set_xticklabels(all_the_data["Date/Time"].values, rotation=90, fontsize=10)

#         plt.show()
