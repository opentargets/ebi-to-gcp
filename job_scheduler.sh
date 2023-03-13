#!/bin/bash
# Job requirements
#BSUB -J ot_gwas_sumstats_scheduler
#BSUB -W 2:00
#BSUB -n 1
#BSUB -M 1024M
#BUSB -R rusage[mem=1024M]
#BUSB -N
#BUSB -B

# This script is used to schedule the GWAS catalog processing jobs periodically.

# Environment variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Defaults
path_baseline_summary_statistics='/nfs/ftp/public/databases/gwas/summary_statistics'
path_file_harmonised_listing=${path_baseline_summary_statistics}/harmonised_list.txt

# Helper functions
function print_environment {
    echo "---> Environment variables:"
    echo "  SCRIPT_DIR=${SCRIPT_DIR}"
    echo "  path_baseline_summary_statistics=${path_baseline_summary_statistics}"
    echo "  path_file_harmonised_listing=${path_file_harmonised_listing}"
}

# Main
print_environment
for study in $(cat ${path_file_harmonised_listing} | head -n 1); do
    export path_study=$(readlink -f "${path_baseline_summary_statistics}/${study}")
    echo "---> Launch processing job for study: '${path_study}'"
    #sbatch ${SCRIPT_DIR}/process_gwas_study.sh ${path_study}
    ${SCRIPT_DIR}/process_gwas_study.sh ${path_study}
done