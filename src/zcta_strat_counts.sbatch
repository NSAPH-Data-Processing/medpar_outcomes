#!/bin/bash
#
#SBATCH -p large # partition (queue)
#SBATCH -c 4 # number of cores per job in the array
#SBATCH --mem 32GB # total memory in the array
#SBATCH -t 0-01:00 # time (D-HH:MM)
#SBATCH --array=0-65

YEARS=($(seq 2000 2021))
FREQS=(yearly weekly  daily)

YEAR_IDX=$(( SLURM_ARRAY_TASK_ID / 3 ))
FREQ_IDX=$(( SLURM_ARRAY_TASK_ID % 3 ))

YEAR=${YEARS[$YEAR_IDX]}
FREQ=${FREQS[$FREQ_IDX]}

echo "Running for year=$YEAR, freq=$FREQ"

#activate <env_name>

# Run your script with the specific year
python src/zcta_strat_counts.py --year $YEAR --temporal_freq $FREQ