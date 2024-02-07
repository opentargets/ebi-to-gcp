#!/bin/bash
# Job requirements
#Submit this script with: sbatch thefilename
#For more details about each parameter, please check SLURM sbatch documentation https://slurm.schedmd.com/sbatch.html

#SBATCH --time=1-00:00:00
#SBATCH --ntasks=1   # number of tasks
#SBATCH --cpus-per-task=16   # number of CPUs Per Task i.e if your code is multi-threaded
#SBATCH --nodes=1   # number of nodes
#SBATCH -p datamover   # partition(s)
#SBATCH --mem=10G   # memory per node
#SBATCH --mail-type=all
#SBATCH -J "eqtl-catalogue-gcp-uploader"   # job name
#SBATCH -o "/nfs/production/opentargets/lsf/logs/ot_eqtl_gcp_rsync-%j.out"  # job output file
#SBATCH -e "/nfs/production/opentargets/lsf/logs/ot_eqtl_gcp_rsync-%j.err"  # job error file
target_path='gs://eqtl_catalog_data/ebi_ftp/susie'
base_path=/nfs/ftp/public/databases/spot/eQTL/susie/
gsutil_path=${HOME}/google-cloud-sdk/bin
path_ops_baseline="/nfs/production/opentargets/ot-ops"
path_ops_credentials="${path_ops_baseline}/credentials"
path_ops_gcp_service_account="${path_ops_credentials}/gcp-service-account-gwas-summary-stats.json"

# Setting up credentials:
${gsutil_path}/gcloud auth activate-service-account --key-file=${path_ops_gcp_service_account}

${gsutil_path}/gsutil -m rsync -r -d ${base_path} ${target_path}