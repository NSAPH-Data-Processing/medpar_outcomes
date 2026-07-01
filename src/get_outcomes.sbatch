#!/bin/bash
#
#SBATCH -p med # partition (queue)
#SBATCH -c 4 # number of cores per job in the array
#SBATCH --mem 48GB # total memory in the array
#SBATCH -t 0-02:00 # time (D-HH:MM)
#SBATCH --array=0-21

YEARS=($(seq 2000 2021))
YEAR=${YEARS[$SLURM_ARRAY_TASK_ID]}

#activate <env_name>

# Run your script with the specific year
python src/get_outcomes.py --year $YEAR