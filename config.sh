#!/bin/bash
# -*- coding: utf-8 -*-

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Operational environment for the scripts in this repository
# Operational defaults
path_ops_baseline="/nfs/production/opentargets/ot-ops"
path_ops_credentials="${path_ops_baseline}/credentials"
path_ops_gcp_service_account="${path_ops_credentials}/gcp-service-account-gwas-summary-stats.json"
path_env_python="${SCRIPT_DIR}/.venv"
path_payload_processing="${SCRIPT_DIR}/gwas_sumstats_ingset.py"

# Defaults
path_baseline_summary_statistics='/nfs/ftp/public/databases/gwas/summary_statistics'
path_file_harmonised_listing=${path_baseline_summary_statistics}/harmonised_list.txt
# Path baseline for study processing depositions
gcp_path_baseline='gs://open-targets-gwas-summary-stats'
# Path to deposit the processed studies
gcp_path_studies=${gcp_path_baseline}/studies
# Path for study tracking related files, e.g. to find out whether a study has already been processed, needs to be reprocessed or is new
gcp_path_study_tracking=${gcp_path_baseline}/study_tracking
# Path for study processing status files, this is used for pointing to possible errors when processing a study, so it can be reprocessed and / or fixed
gcp_path_study_status=${gcp_path_baseline}/study_status
# Runtime - Docker container
runtime_pyspark_image='jupyter/pyspark-notebook:latest'


# Helpers
function log {
    echo "[$(date)] $@"
}

function print_common_environment {
    log "---> Environment --- Configuration ---:"
    log "  path_ops_baseline=${path_ops_baseline}"
    log "  path_ops_credentials=${path_ops_credentials}"
    log "  path_ops_gcp_service_account=${path_ops_gcp_service_account}"
    log "  path_env_python=${path_env_python}"
    log "  path_payload_processing=${path_payload_processing}"
    log "  path_baseline_summary_statistics=${path_baseline_summary_statistics}"
    log "  path_file_harmonised_listing=${path_file_harmonised_listing}"
    log "  gcp_path_baseline=${gcp_path_baseline}"
    log "  gcp_path_studies=${gcp_path_studies}"
    log "  gcp_path_study_tracking=${gcp_path_study_tracking}"
    log "  gcp_path_study_status=${gcp_path_study_status}"
    log "  runtime_pyspark_image=${runtime_pyspark_image}"
}

# Main
print_common_environment