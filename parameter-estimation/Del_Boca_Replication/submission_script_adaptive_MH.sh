#!/bin/bash
#SBATCH --partition=compute        # Partition (job queue)
#SBATCH --requeue                  # Return job to the queue if preempted
#SBATCH --job-name=adaptive-metropolis        # Assign a short name to your job
#SBATCH --nodes=1                  # Number of nodes you require
#SBATCH --cpus-per-task=40          # Cores per task (>1 if multithread tasks)
#SBATCH --mem-per-cpu=32gb         # Real memory per cpu
#SBATCH --time=02-00:00:00         # Total run time limit (DD-HH:MM:SS)
#SBATCH --output=slurm.adaptive-MH.%N.%j.out   # STDOUT file for SLURM output
#SBATCH --mail-type=ALL            # Email if anything happens (job start, end, failure, requeue, etc.)
#SBATCH --mail-user=btaheri1@swarthmore.edu


## Modifying the executable nature of the files, so that I don't have to change them every time
chmod +x "adaptive_metropolis.py"
chmod +x "Del_Boca_Implementation.py"
chmod +x "solve_system.py"

conda activate oconnelllab

## Set the value of BIGSCRATCH to 'true' if your job is
## anticipated to require more than a few hundred gigabytes
## of scratch space, which includes all input, temporary,
## and output data. If BIGSCRATCH is true, then the job
## will use scratch space on the parallel storage node.
## This is slower than local scratch space on the compute
## nodes but has significantly more space.

BIGSCRATCH=false

SCRATCHDIR='scratch'
if [ "$BIGSCRATCH" = true ]
then
        SCRATCHDIR='data/scratch'
fi

## Move files to the scratch location so we can execute the
## job. Make certain to replace the placeholder filenames
## below (data_file_1, my_script) with all files required
## for your job (wildcards such as * are allowed, and by
## default any directories listed will be copied recursively).
mkdir -p /$SCRATCHDIR/$USER/$SLURM_JOB_NAME-$SLURM_JOB_ID
cp -r \
   adaptive_metropolis.py  \
   Del_Boca_Implementation.py  \
   solve_system.py \
/$SCRATCHDIR/$USER/$SLURM_JOB_NAME-$SLURM_JOB_ID

## Change to the scratch directory.
cd /$SCRATCHDIR/$USER/$SLURM_JOB_NAME-$SLURM_JOB_ID

## ADD YOUR COMMANDS HERE

srun adaptive_metropolis.py

## Move any outputs and files back to the original submit
## directory and clean-up.
cp -pru /$SCRATCHDIR/$USER/$SLURM_JOB_NAME-$SLURM_JOB_ID/* $SLURM_SUBMIT_DIR
cd
rm -rf /$SCRATCHDIR/$USER/$SLURM_JOB_NAME-$SLURM_JOB_ID

## Hang out for 10 seconds just to ensure all actions complete.
sleep 10
