#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 2 # number of cores
#SBATCH --mem 4GB # memory per job
#SBATCH -t 0-00:30 # time (D-HH:MM)
#SBATCH --array=2015-2018

# ===================
#module load python
#conda activate medpar_outcomes_env

python src/strat_counts.py --year $SLURM_ARRAY_TASK_ID