#!/bin/bash
# Job requirements
# Submit this script with: sbatch thefilename
# For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=8:00:00   # walltime
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=16   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH -p datamover   # partition(s)
#SBATCH --mem=32G   # memory per node
#SBATCH --mail-type=all
#SBATCH -J "gcp-uploader"   # job name
#SBATCH -o "/nfs/production/opentargets/lsf/logs/ot_gwascat_gcp_rsync-%j.out"  # job output file
#SBATCH -e "/nfs/production/opentargets/lsf/logs/ot_gwascat_gcp_rsync-%j.err"  # job error file

# Bucket name for the outputs
gcs_bucket="gwas_catalog_inputs"

############################################ SYNC SUMMARY STATISTICS AND METADATA TO GCP ############################################

# Sync paths
target_path="gs://${gcs_bucket}/raw_summary_statistics/"
base_path=/nfs/ftp/public/databases/gwas/summary_statistics/
base_metadata_list_path="metadata_files_list.$(date -I).txt"
target_metadata_list_path="gs://${gcs_bucket}/sync_dump/${base_metadata_list_path}"

# Software paths
gsutil_path=${HOME}/google-cloud-sdk/bin

# GCS credentials
path_ops_baseline="/nfs/production/opentargets/ot-ops"
path_ops_credentials="${path_ops_baseline}/credentials"
path_ops_gcp_service_account="${path_ops_credentials}/gcp-service-account-gwas-summary-stats.json"

# Setting up credentials:
${gsutil_path}/gcloud auth activate-service-account --key-file=${path_ops_gcp_service_account}

# Sync all h.tsv.gz and h.tsv.gz.meta.yaml files
${gsutil_path}/gsutil -u open-targets-genetics-dev -m rsync -r -d -x '^(?!.*\.h\.tsv\.gz(.meta.yaml)?$)' ${base_path} ${target_path}


############################################ READ METADATA AND COLLECT TO PARQUET FILE ############################################

# Sync paths
datetime_now="$(date -I)"
target_local_filename="sync_dump_${datetime_now}.parquet"
target_local_yaml_dump_path="${HOME}/${target_local_filename}"
target_remote_yaml_dump_path="gs://${gcs_bucket}/sync_dump/${datetime_now}/${target_local_filename}"
target_remote_yaml_latest_dump_path="gs://${gcs_bucket}/sync_dump/latest/sync_dump_latest.parquet"

# Software paths
ebi_to_gcp_path=${HOME}/ebi-to-gcp/ebi-to-gcp

# Read and collect all yaml files
${ebi_to_gcp_path} $base_path $target_local_yaml_dump_path --n-threads 150

# Sync the yaml dump file
${gsutil_path}/gsutil -u open-targets-genetics-dev cp ${target_local_yaml_dump_path} ${target_remote_yaml_dump_path}
${gsutil_path}/gsutil -u open-targets-genetics-dev cp ${target_local_yaml_dump_path} ${target_remote_yaml_latest_dump_path}
