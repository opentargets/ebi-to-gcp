#!/bin/bash
# Job requirements
#BSUB -J ot_gwas_sumstats_worker
#BSUB -W 3:00
#BSUB -n 1
#BSUB -M 1024M
#BUSB -R rusage[mem=1024M]
#BUSB -N
#BUSB -B

# This script will process a single GWAS study.

# Bootstrapping
#export TMPDIR="$TMPDIR/%J-tmp_dir"
#mkdir -p $TMPDIR
export WORKDIR="$TMPDIR/work_dir"
mkdir -p $WORKDIR
#trap "rm -rf $TMPDIR" EXIT

# Environment variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Operational defaults
path_ops_baseline="$HOME/ot-ops"
path_ops_credentials="${path_ops_baseline}/credentials"
path_ops_gcp_service_account="${path_ops_credentials}/gcp-service-account-gwas-summary-stats.json"
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
    log "---> Environment SUMMARY:"
    log "  SCRIPT_DIR=${SCRIPT_DIR}"
    log "  path_ops_baseline=${path_ops_baseline}"
    log "  path_ops_credentials=${path_ops_credentials}"
    log "  path_ops_gcp_service_account=${path_ops_gcp_service_account}"
    log "  path_baseline_summary_statistics=${path_baseline_summary_statistics}"
    log "  gcp_path_baseline=${gcp_path_baseline}"
    log "  gcp_path_studies=${gcp_path_studies}"
    log "  gcp_path_study_tracking=${gcp_path_study_tracking}"
    log "  gcp_path_study_status=${gcp_path_study_status}"
    log "  path_study=${path_study}"
    log "  study_id=${study_id}"
    log "  study_dir=${study_dir}"
    log "  study_filename=${study_filename}"
    log "  study_checksum_file=${study_checksum_file}"
    log "  study_checksum_value=${study_checksum_value}"
    log "  study_gcp_path_dst=${study_gcp_path_dst}"
    log "  study_gcp_path_checksum=${study_gcp_path_checksum}"
    log "  study_gcp_path_status_error=${study_gcp_path_status_error}"
}

# Set error status for a study
function set_error_status {
    log "Setting error status for study '${study_id}'"
    echo "${study_checksum_value} ${study_filename}" | singularity exec docker://google/cloud-sdk:latest gsutil cp - ${study_gcp_path_status_error}
}

# Clear error status for a study
function clear_error_status {
    log "Clearing error status for study '${study_id}'"
    singularity exec docker://google/cloud-sdk:latest gsutil rm ${study_gcp_path_status_error}
}

# Set study processed
function set_study_processed {
    log "Setting study '${study_id}' processed flag to '${study_checksum_value} ${study_filename}' at '${study_gcp_path_checksum}'"
    echo "${study_checksum_value} ${study_filename}" | singularity exec docker://google/cloud-sdk:latest gsutil cp - ${study_gcp_path_checksum}
}

# Clear study processed
function clear_study_processed {
    log "Clearing study '${study_id}' processed flag"
    singularity exec docker://google/cloud-sdk:latest gsutil rm ${study_gcp_path_checksum}
}

# Check error status for a study
function check_error_status {
    log "Checking error status for study ID '${study_id}'"
    singularity exec docker://google/cloud-sdk:latest gsutil ls ${study_gcp_path_status_error} > /dev/null 2>&1
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
    singularity exec docker://google/cloud-sdk:latest gsutil ls ${study_gcp_path_dst} > /dev/null 2>&1
    if [[ $? -eq 0 ]]; then
        log "Study '${study_id}' has been seen/processed before"
        return 0
    else
        log "Study '${study_id}' has NOT been seen/processed before"
        return 1
    fi
}

# Upload processed study to GCP
function upload_study_to_gcp {
    log "Uploading study '${study_id}' to GCP location '${study_gcp_path_dst}'"
    singularity exec docker://google/cloud-sdk:latest gsutil -m cp -r ${WORKDIR} ${study_gcp_path_dst}
    if [[ $? -ne 0 ]]; then
        log "ERROR: Failed to upload study '${study_id}' to GCP"
        set_error_status
        return 1
    fi
    return 0
}

# Remove study from GCP
function remove_study_from_gcp {
    log "Removing study '${study_id}' from GCP"
    singularity exec docker://google/cloud-sdk:latest gsutil -m rm -r ${study_gcp_path_dst}
    if [[ $? -ne 0 ]]; then
        log "ERROR: Failed to remove study '${study_id}' from GCP"
        set_error_status
        return 1
    fi
    return 0
}




# --- Main ---
print_environment

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
    study_gcp_checksum_value=$(singularity exec docker://google/cloud-sdk:latest gsutil cat ${study_gcp_path_checksum} | awk '{print $1}')
    if [ "${study_gcp_checksum_value}" != "${study_checksum_value}" ]; then
        log "Study '${study_id}' has been updated since last processing"
        # Set flag to process study
        flag_process_study=0
        # Remove the study from the GCP bucket
        remove_study_from_gcp
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
    log "XXX ------------------------- [START] STUDY PROCESSING PAYLOAD [START] ---------------------------- XXX"
    # Touch 10 empty files in WORKDIR
    for i in {1..10}; do
        touch ${WORKDIR}/file_${i}.parquet
    done
    log "XXX ------------------------- [END]   STUDY PROCESSING PAYLOAD   [END] ---------------------------- XXX"
    if [[ $? -eq 0 ]]; then
        log "Study '${study_id}' processing was SUCCESSFUL"
        # Upload the study to GCP
        if upload_study_to_gcp; then
            log "Study '${study_id}' was uploaded to GCP"
            # Set the study status to OK, i.e. no errors and md5sum upload
            set_study_processed
        else
            log "ERROR: Study '${study_id}' was NOT uploaded to GCP"
            set_error_status
            exit 1
        fi
    else
        log "ERROR: Study '${study_id}' processing FAILED"
        set_error_status
    fi
else
    log "--- SKIP --- Study '${study_id}' does not need to be processed"
fi

