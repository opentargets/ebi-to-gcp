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
gcs_bucket="ot_orchestration"

# Sync paths
target_path="gs://${gcs_bucket}/raw_summary_statistics/"
base_path=/nfs/ftp/public/databases/gwas/summary_statistics/
target_local_yaml_dump_path="sync_dump_$(date -I).parquet"
target_remove_yaml_dump_path="gs://${gcs_bucket}/sync_dump/${target_local_yaml_dump_path}"

# Software paths
gsutil_path=${HOME}/google-cloud-sdk/bin
ebi_to_gcp_path=./ebi-to-gcp

# GCS credentials
path_ops_baseline="/nfs/production/opentargets/ot-ops"
path_ops_credentials="${path_ops_baseline}/credentials"
path_ops_gcp_service_account="${path_ops_credentials}/gcp-service-account-gwas-summary-stats.json"

# Merge the yaml files into a dump
${ebi_to_gcp_path} $base_path $target_local_yaml_dump_path

# Setting up credentials:
${gsutil_path}/gcloud auth activate-service-account --key-file=${path_ops_gcp_service_account}

# Sync all h.tsv.gz and h.tsv.gz.meta.yaml files
${gsutil_path}/gsutil -m rsync -r -d -x '^(?!.*\.h\.tsv\.gz(.meta.yaml)?$)' ${base_path} ${target_path}

# Sync the yaml dump file
${gsuitl_path}/gsutil cp ${target_local_yaml_dump_path} ${target_remove_yaml_dump_path}