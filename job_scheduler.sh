#!/bin/bash

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
for study in $(cat ${path_file_harmonised_listing}); do
    echo "---> Launch processing job for study: '${study}'"
    #sbatch ${SCRIPT_DIR}/process_study.sh ${study}
done