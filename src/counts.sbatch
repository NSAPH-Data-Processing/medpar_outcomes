#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 4 # number of cores
#SBATCH --mem 32GB # memory per job
#SBATCH -t 0-00:30 # time (D-HH:MM)
#SBATCH --array=2000-2016

# ===================
#module load python
#conda activate medpar_outcomes_env

python src/zip_year_counts.py --year $SLURM_ARRAY_TASK_ID