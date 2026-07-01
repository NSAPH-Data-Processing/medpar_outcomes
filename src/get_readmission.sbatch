#!/bin/bash
#
#SBATCH -p large # partition (queue)
#SBATCH -c 8 # number of cores per job in the array
#SBATCH --mem 96GB # total memory in the array
#SBATCH -t 0-02:00 # time (D-HH:MM)

#activate <env_name>

# Run your script with the specific year
python src/get_readmission.py 