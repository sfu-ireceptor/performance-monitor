#!/bin/bash
#SBATCH --time=04:00:00
#SBATCH --mem=56000M
#SBATCH --array=1-99%2
#SBATCH --account=rpp-breden-ab
#SBATCH --output=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Sequence_Checks/output3_Sep2019/TSV_download_ipa3_seq%J.out

##Script Author: Laura Gutierrez Funderburk
##Supervised by: Dr. Felix Breden, Dr. Jamie Scott, Dr. Brian Corrie
##Created on: May 2 2019
##Last modified on: May 2 2019

echo "Current working directory: `pwd`"
echo "Starting run at: `date`"
# ---------------------------------------------------------------------
echo ""
echo "Job Array ID / Job ID: $SLURM_ARRAY_JOB_ID / $SLURM_JOB_ID"
echo "This is job $SLURM_ARRAY_TASK_ID out of $SLURM_ARRAY_TASK_COUNT jobs."
echo ""

echo "Begin Script"
echo " "

#######################
##### Directories #####
#######################

###############################
## OUTPUT DIRS AND ARRAY DATA##
###############################

# Call array data file
array_data=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Sequence_Checks/array_data/

# Scripts director
script=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Sequence_Checks/scripts/

# Sequenced directory

seq=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Sequence_Checks/SeqFiles3_Sep2019/

#######################
# Array File Entries ##
#######################

cd ${array_data}

_id=`awk -F_ '{print $1}' array_data_ipa3 | head -$SLURM_ARRAY_TASK_ID | tail -1` 


#######################
## Verify Input Para ##
#######################

echo "Sample ID: " ${_id}

#######################
### Sanity Checking ###
#######################

cd ${script}

echo "Begin Python Sequence Check "
echo " "
module load python/3.7.0

#virtualenv ~/ENV

source ~/ENV/bin/activate

module load python/3.7.0

python sequences.py ${_id} "nan,IG,TR,Homosap,F,ORF,less,nucleotides,are,than,aligned,(F),Musmus" ${seq} 'https://ipa3.ireceptor.org'

python productive_test.py ${seq} ${_id}

deactivate

echo "End Python Sanity Check"

# rm ${APIFile_name}
