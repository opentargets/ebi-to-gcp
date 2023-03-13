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
# Operational defaults
path_ops_baseline="$HOME/ot-ops"
path_ops_credentials="${path_ops_baseline}/credentials"
path_ops_gcp_service_account="${path_ops_credentials}/gcp-service-account-gwas-summary-stats.json"

# Defaults
path_baseline_summary_statistics='/nfs/ftp/public/databases/gwas/summary_statistics'
path_file_harmonised_listing=${path_baseline_summary_statistics}/harmonised_list.txt

# Logging helpers
function log {
    echo "[$(date)] $@"
}

# Helper functions
function print_environment {
    echo "---> Environment variables:"
    echo "  SCRIPT_DIR=${SCRIPT_DIR}"
    echo "  path_baseline_summary_statistics=${path_baseline_summary_statistics}"
    echo "  path_file_harmonised_listing=${path_file_harmonised_listing}"
}

# Activate GCP service account
function activate_gcp_service_account {
    log "Activating GCP service account"
    singularity exec docker://google/cloud-sdk:latest gcloud auth activate-service-account --key-file=${path_ops_gcp_service_account}
}


# Main
print_environment
activate_gcp_service_account
for study in $(cat ${path_file_harmonised_listing} | head -n 1); do
    export path_study=$(readlink -f "${path_baseline_summary_statistics}/${study}")
    echo "---> Launch processing job for study: '${path_study}'"
    #sbatch ${SCRIPT_DIR}/process_gwas_study.sh ${path_study}
    ${SCRIPT_DIR}/process_gwas_study.sh ${path_study}
done