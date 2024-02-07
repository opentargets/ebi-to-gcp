#!/bin/bash
# Job requirements
#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

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
target_path='gs://gwas_catalog_data/raw_summary_statistics/'
base_path=/nfs/ftp/public/databases/gwas/summary_statistics/
gsutil_path=${HOME}/google-cloud-sdk/bin
path_ops_baseline="/nfs/production/opentargets/ot-ops"
path_ops_credentials="${path_ops_baseline}/credentials"
path_ops_gcp_service_account="${path_ops_credentials}/gcp-service-account-gwas-summary-stats.json"

# Setting up credentials:
${gsutil_path}/gcloud auth activate-service-account --key-file=${path_ops_gcp_service_account}

${gsutil_path}/gsutil -m rsync -r -d -x '^(?!.*\.h\.tsv\.gz$)' ${base_path} ${target_path}