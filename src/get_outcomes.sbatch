#!/bin/bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH -c 4 # number of cores
#SBATCH --mem 24GB # memory per job
#SBATCH -t 0-00:30 # time (D-HH:MM)
#SBATCH --array=2000-2018

# == cluster specific lines ==
#module load python
#conda activate medpar_outcomes_env
# ===================
python src/get_outcomes.py --year $SLURM_ARRAY_TASK_ID