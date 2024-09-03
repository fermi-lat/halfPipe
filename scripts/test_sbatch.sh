#!/bin/bash

#SBATCH --partition=milano
#
#SBATCH --job-name=TEST
#SBATCH --output=output.log
#SBATCH --error=error.log
#
#SBATCH --ntasks=1
#SBATCH --time=10

echo "HOSTNAME=${HOSTNAME}"
echo "LSCRATCH=${LSCRATCH}"
srun --prolog ./prolog.sh ./run_rhel6.sh 
