#!/bin/bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH -c 4 # number of cores
#SBATCH --mem 32GB # memory per job
#SBATCH -t 0-00:30 # time (D-HH:MM)

# == cluster specific lines ==
#module load python
#conda activate medpar_outcomes_env
# ===================
python src/get_outcomes_index.py $SLURM_ARRAY_TASK_ID