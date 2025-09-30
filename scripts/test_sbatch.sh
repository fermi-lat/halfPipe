#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=fermi:users
#SBATCH --ntasks=1
#SBATCH --time=0-10:00:00
#
#SBATCH --job-name=TEST
#SBATCH --output=output_test_prolog.out
#SBATCH --error=error_test_prolog.out
lock_file="cacca.lock"
touch $lock_file

cat <<< '#!/bin/bash
#SBATCH --partition=milano
#SBATCH --account=fermi:users
#SBATCH --ntasks=1
#SBATCH --time=0-10:00:00
#
#SBATCH --job-name=TEST
#SBATCH --output=output_test_prolog2.out
#SBATCH --error=error_test_prolog2.out
lock_file="lock.sh"
pre_exec="--partition=milano --account=fermi:users --parsable --mem=1GB --cpus-per-task=1 prolog.sh $lock_file"
JOBID1=$(sbatch $pre_exec)
echo "JOBID1=${JOBID1}"
sbatch --partition=milano --account=fermi:users --time=00:30:00 --mem=1GB --cpus-per-task=1 --dependency=afterok:${JOBID1} ./run_rhel6.sh
' > lock.sh
sbatch lock.sh
