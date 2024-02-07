# ebi-to-gcp

This repository contains the code to transfer data from EBI LSF to GCP.

## Prerequisites

- Access to EBI Slurm cluster (`codon-slurm-login`)
- Credentials to access GCP bucket

## Installation

1. Clone the repository in the EBI cluster
2. Download the `gcloud` SDK in the EBI cluster. You can find the instructions [here](https://cloud.google.com/sdk/docs/install#linux)

## Usage

Jobs can be manually triggered by running

```bash
sbatch gwas_catalog_data_mover.sh
```

Job progress can be monitored in the respective log files. For example:

```bash
watch "tail /nfs/production/opentargets/lsf/logs/ot_gwascat_gcp_rsync-47150371.err"
```

## Setting up cron jobs in slurm

Documentation on how to setup a CRON job within the Slurm cluster can be found [here](https://embl.service-now.com/esc?id=kb_article&table=kb_knowledge&sysparm_article=KB0010982#mcetoc_1grgc1g622). 

For example, to setup a CRON job to run the `gwas_catalog_data_mover.sh` script every Monday at 7:30am, you can create a file `gwas_catalog_rsync_cron` with the following content:

```bash
#SCRON -t 1
#SCRON --mem=1
#SCRON -J gwas_catalog_rsync_cron
# min hour day-of-month month day-of-week command
30 7 * * 1 sbatch /homes/ochoa/gwas-summary-stats/gwas_catalog_data_mover.sh
```
