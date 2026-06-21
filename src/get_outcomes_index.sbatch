#!/bin/bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH -c 12 # number of cores
#SBATCH --mem 96GB # memory per job
#SBATCH -t 0-08:00 # time (D-HH:MM)

# == cluster specific lines ==
#module load python
#conda activate medpar_outcomes_env
# ===================
python src/get_outcomes_index.py $SLURM_ARRAY_TASK_ID