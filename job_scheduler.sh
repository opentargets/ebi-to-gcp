#!/bin/bash
# Job requirements
#BSUB -J ot_gwas_sumstats_scheduler
#BSUB -W 1:00
#BSUB -n 1
#BSUB -M 1024M
#BSUB -R rusage[mem=1024M]
#BSUB -N
#BSUB -B
#BSUB -e /nfs/production/opentargets/lsf/logs/ot_gwas_sumstats_scheduler-%J.err
#BSUB -o /nfs/production/opentargets/lsf/logs/ot_gwas_sumstats_scheduler-%J.out


# This script is used to schedule the GWAS catalog processing jobs periodically.

# Environment variables
if [[ -n "$LS_EXECCWD" ]]; then
    SCRIPT_DIR=$LS_EXECCWD
    cd $LS_EXECCWD
else
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
fi
SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})

# DEBUG
env
pwd
echo "SCRIPT_DIR=${SCRIPT_DIR}"
echo "SCRIPT_NAME=${SCRIPT_NAME}"

# Operational defaults
source ${SCRIPT_DIR}/config.sh

# Helper functions
function print_environment {
    log "---> Local Environment for '${SCRIPT_NAME}':"
    log "  SCRIPT_DIR=${SCRIPT_DIR}"
}

# Activate GCP service account
function activate_gcp_service_account {
    log "Activating GCP service account `basename ${path_ops_gcp_service_account}`"
    singularity exec docker://google/cloud-sdk:latest gcloud auth activate-service-account --key-file=${path_ops_gcp_service_account}
}

function setup_python_environment {
    log "Setting up Python environment at '${path_env_python}'"
    #singularity exec docker://${runtime_pyspark_image} python -m venv ${path_env_python}
    singularity exec docker://${runtime_pyspark_image} pip install -r ${SCRIPT_DIR}/requirements.txt
}


# Main
print_environment
activate_gcp_service_account
setup_python_environment
for study in $(cat ${path_file_harmonised_listing} | head -n 5000); do
    export path_study=$(readlink -f "${path_baseline_summary_statistics}/${study}")
    log "---> Launch processing job for study: '${path_study}'"
    #bsub -q datamover -J ot_gwas_sumstats_worker -W 1:00 -n 16 -M 32768M -R "rusage[mem=32768M]" -R "span[hosts=1]" ${SCRIPT_DIR}/process_gwas_study.sh ${path_study}
    export PARAM_PATH_STUDY=${path_study}
    bsub -q datamover < ${SCRIPT_DIR}/process_gwas_study.sh
    #${SCRIPT_DIR}/process_gwas_study.sh ${path_study}
done