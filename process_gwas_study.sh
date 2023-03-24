#!/bin/bash
# Job requirements
#BSUB -J ot_gwas_sumstats_worker
#BSUB -W 1:00
#BSUB -n 16
#BSUB -M 32768M
#BSUB -R rusage[mem=32768M]
#BSUB -R span[hosts=1]
#BSUB -N
#BSUB -B
#BSUB -e /nfs/production/opentargets/lsf/logs/ot_gwas_sumstats_worker-%J.err
#BSUB -o /nfs/production/opentargets/lsf/logs/ot_gwas_sumstats_worker-%J.out

# This script will process a single GWAS study.

# Environment variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})

# Operational defaults
source ${SCRIPT_DIR}/config.sh

# Bootstrapping
export TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Command line arguments
[[ $# -eq 1 ]] || { echo "ERROR: Invalid number of arguments. Usage: $0 <path_study>"; exit 1; }
path_study=$1

# Get the GWAS study ID
study_id=$(basename ${path_study} | egrep -o 'GCST[0-9]+')
study_dir=$(dirname ${path_study})
study_filename=$(basename ${path_study})
study_checksum_file=${study_dir}/md5sum.txt
study_checksum_value=$(cat ${study_checksum_file} | grep ${study_filename} | awk '{print $1}')
# The following path is used to store the study data after it has been processed
path_study_data="$TMPDIR/${study_id}"
# The following paths are related to GCP
study_gcp_path_dst=${gcp_path_studies}/${study_id}
study_gcp_path_checksum=${gcp_path_study_tracking}/${study_id}.md5
study_gcp_path_status_error=${gcp_path_study_status}/${study_id}.error
# LSF environment related paths
lsf_path_output_error_file=$LSB_ERRORFILE
lsf_path_output_file=$LSB_OUTPUTFILE
# Analysis related paths
path_output_gwas_analysis_error=$(dirname ${lsf_path_output_error_file})/${study_id}.gwas_sumstats.err

# Helper functions
# Print environment summary
function print_environment {
    log "---> Local Environment for '${SCRIPT_NAME}':"
    log "  SCRIPT_DIR=${SCRIPT_DIR}"
    log "  TMPDIR=${TMPDIR}"
    log "  path_study=${path_study}"
    log "  study_id=${study_id}"
    log "  study_dir=${study_dir}"
    log "  study_filename=${study_filename}"
    log "  study_checksum_file=${study_checksum_file}"
    log "  study_checksum_value=${study_checksum_value}"
    log "  path_study_data=${path_study_data}"
    log "  study_gcp_path_dst=${study_gcp_path_dst}"
    log "  study_gcp_path_checksum=${study_gcp_path_checksum}"
    log "  study_gcp_path_status_error=${study_gcp_path_status_error}"
    log "  lsf_path_output_error_file=${lsf_path_output_error_file}"
    log "  lsf_path_output_file=${lsf_path_output_file}"
    log "  path_output_gwas_analysis_error=${path_output_gwas_analysis_error}"
}

# Set error status for a study
function set_error_status {
    tmp_path_error_log=$TMPDIR/${study_id}.error.log
    log "Collecting summary stats processing error information for study '${study_id}' at temporary path '${tmp_path_error_log}'"
    echo -e "--- ERROR processing study '${study_id}' ---\n" >> ${tmp_path_error_log}
    echo -e "${study_checksum_value} ${study_filename}\n" >> ${tmp_path_error_log}
    echo -e "---> LSF PATHS:\n" >> ${tmp_path_error_log}
    echo -e "\tLSF stdout file: ${lsf_path_output_file}\n" >> ${tmp_path_error_log}
    echo -e "\tLSF stderr file: ${lsf_path_output_error_file}\n" >> ${tmp_path_error_log}
    echo -e "---> GWAS ANALYSIS ERROR LOG:\n" >> ${tmp_path_error_log}
    cat ${path_output_gwas_analysis_error} >> ${tmp_path_error_log}
    #echo "${study_checksum_value} ${study_filename}" | singularity exec docker://google/cloud-sdk:latest gsutil cp - ${study_gcp_path_status_error}
    log "Setting error status for study '${study_id}' at GCP path '${study_gcp_path_status_error}'"
    cat ${tmp_path_error_log} | singularity exec docker://google/cloud-sdk:latest gsutil cp - ${study_gcp_path_status_error}
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
    singularity exec docker://google/cloud-sdk:latest gsutil -m rsync -d -e -r ${path_study_data}/ ${study_gcp_path_dst}/
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

# Prepare study data directory
function prepare_study_data_folder {
    log "Preparing study data for '${study_id}' at '${path_study_data}'"
    #mkdir -p ${path_study_data}
}



# --- Main ---
print_environment
prepare_study_data_folder

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
    # Process the study
    log "[ ------------------------- [START] STUDY - '${study_id}' - PROCESSING PAYLOAD [START] ---------------------------- ]"
    #log "Copy file to current folder, '${path_study}'"
    singularity exec --bind /nfs/ftp:/nfs/ftp docker://${runtime_pyspark_image} python ${path_payload_processing} --input_file ${path_study} --output_file ${path_study_data} 2>&1 | tee ${path_output_gwas_analysis_error}
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
log "[ ------------------------- [END]   STUDY - '${study_id}' - PROCESSING PAYLOAD   [END] ---------------------------- ]"

