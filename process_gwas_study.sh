#!/bin/bash

# This script will process a single GWAS study.

# Environment variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Operational defaults
path_ops_baseline="$HOME/ot-ops"
path_ops_credentials="${path_ops_baseline}/credentials"
path_ops_gcp_service_account="${path_ops_credentials}/gcp-service-account.json"
# Path baseline for summary statistics
path_baseline_summary_statistics='/nfs/ftp/public/databases/gwas/summary_statistics'
# Path baseline for study processing depositions
gcp_path_baseline='gs://open-targets-gwas-summary-stats'
# Path to deposit the processed studies
gcp_path_studies=${gcp_path_baseline}/studies
# Path for study tracking related files, e.g. to find out whether a study has already been processed, needs to be reprocessed or is new
gcp_path_study_tracking=${gcp_path_baseline}/study_tracking
# Path for study processing status files, this is used for pointing to possible errors when processing a study, so it can be reprocessed and / or fixed
gcp_path_study_status=${gcp_path_baseline}/study_status

# Command line arguments
[[ $# -eq 1 ]] || { echo "ERROR: Invalid number of arguments. Usage: $0 <path_study>"; exit 1; }
path_study=$1

# Logging helpers
function log {
    echo "[$(date)] $@"
}

# Get the GWAS study ID
study_id=$(basename ${path_study} | egrep -o 'GCST[0-9]+')
study_dir=$(dirname ${path_study})
study_filename=$(basename ${path_study})
study_checksum_file=${study_dir}/md5sum.txt
study_checksum_value=$(cat ${study_checksum_file} | grep ${study_filename} | awk '{print $1}')
study_gcp_path_dst=${gcp_path_studies}/${study_id}
study_gcp_path_checksum=${gcp_path_study_tracking}/${study_id}.md5
study_gcp_path_status_error=${gcp_path_study_status}/${study_id}.error

# Helper functions
# Print environment summary
function print_environment {
    echo "---> Environment SUMMARY:"
    echo "  SCRIPT_DIR=${SCRIPT_DIR}"
    echo "  path_ops_baseline=${path_ops_baseline}"
    echo "  path_ops_credentials=${path_ops_credentials}"
    echo "  path_ops_gcp_service_account=${path_ops_gcp_service_account}"
    echo "  path_baseline_summary_statistics=${path_baseline_summary_statistics}"
    echo "  gcp_path_baseline=${gcp_path_baseline}"
    echo "  gcp_path_studies=${gcp_path_studies}"
    echo "  gcp_path_study_tracking=${gcp_path_study_tracking}"
    echo "  gcp_path_study_status=${gcp_path_study_status}"
    echo "  path_study=${path_study}"
    echo "  study_id=${study_id}"
    echo "  study_dir=${study_dir}"
    echo "  study_filename=${study_filename}"
    echo "  study_checksum_file=${study_checksum_file}"
    echo "  study_checksum_value=${study_checksum_value}"
    echo "  study_gcp_path_dst=${study_gcp_path_dst}"
    echo "  study_gcp_path_checksum=${study_gcp_path_checksum}"
    echo "  study_gcp_path_status_error=${study_gcp_path_status_error}"
}

# Activate GCP service account
function activate_gcp_service_account {
    log "Activating GCP service account"
    gcloud auth activate-service-account --key-file=${path_ops_gcp_service_account}
}

# Set error status for a study
function set_error_status {
    log "Setting error status for study '${study_id}'"
    echo "${study_checksum_value} ${study_filename}" | gsutil cp - ${study_gcp_path_status_error}
}

# Clear error status for a study
function clear_error_status {
    log "Clearing error status for study '${study_id}'"
    gsutil rm ${study_gcp_path_status_error}
}

# Set study processed
function set_study_processed {
    log "Setting study '${study_id}' processed flag"
    echo "${study_checksum_value} ${study_filename}" | gsutil cp - ${study_gcp_path_checksum}
}

# Clear study processed
function clear_study_processed {
    log "Clearing study '${study_id}' processed flag"
    gsutil rm ${study_gcp_path_checksum}
}

# Check error status for a study
function check_error_status {
    log "Checking error status for study ID '${study_id}'"
    gsutil ls ${study_gcp_path_status_error} > /dev/null 2>&1
    if [[ $? -eq 0 ]]; then
        log "Study '${study_id}' has an error status"
        return 0
    else
        log "Study '${study_id}' has no error status"
        return 1
    fi
}

# Check if study has been processed
function check_study_processed {
    log "Checking if study has been processed"
    gsutil ls ${study_gcp_path_dst} > /dev/null 2>&1
    if [[ $? -eq 0 ]]; then
        log "Study '${study_id}' has been seen/processed before"
        return 0
    else
        log "Study '${study_id}' has NOT been seen/processed before"
        return 1
    fi
}




# --- Main ---
print_environment
exit 0


# By default, we assume that we don't need to process the study
flag_process_study=1

# Flag for processing if the study has any error flag from previous processing attempts
if check_error_status; then
    # Clear error status
    clear_error_status
    # Set flag to process study
    flag_process_study=0
fi

# Flag for processing if the study has been updated
if check_study_processed; then
    # Check if the study has been updated
    study_gcp_checksum_value=$(gsutil cat ${study_gcp_path_checksum} | awk '{print $1}')
    if [ "${study_gcp_checksum_value}" != "${study_checksum_value}" ]; then
        log "Study '${study_id}' has been updated since last processing"
        # Set flag to process study
        flag_process_study=0
        # Remove the study from the GCP bucket
        gsutil rm -r ${study_gcp_path_dst}
        # Clear the study processed
        clear_study_processed
    fi
else
    # Flag for processing if the study is new
    log "Study '${study_id}' is NEW"
    # Set flag to process study
    flag_process_study=0
fi

# Process the study
if [[ ${flag_process_study} -eq 0 ]]; then
    log "Processing study '${study_id}'"
    # TODO - Process the study
    log "XXX ------------------------- STUDY PROCESSING PAYLOAD ---------------------------- XXX"
    if [[ $? -eq 0 ]]; then
        log "Study '${study_id}' processing was SUCCESSFUL"
        # Set the study status to OK, i.e. no errors and md5sum upload
        set_study_processed
        # TODO - Upload the study to GCP
    else
        log "ERROR: Study '${study_id}' processing FAILED"
        set_error_status
    fi
else
    log "--- SKIP --- Study '${study_id}' does not need to be processed"
fi

