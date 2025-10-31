# ebi-to-gcp

This repository contains the code to transfer data from EBI LSF to GCP.

## Prerequisites

- Access to EBI Slurm cluster (`codon-slurm-login`)
- Credentials to access GCP bucket

## Installation

1. Clone the repository in the EBI cluster
2. Download the `gcloud` SDK in the EBI cluster. You can find the instructions at [gcloud docs](https://cloud.google.com/sdk/docs/install#linux)
3. if ebi_to_gcp binary is not compiled, run (rustc and cargo must be installed)

```bash
make compile
```

The compiled binary will be located at the root of the repository in `ebi-to-gcp` executable file.

To run with logs enabled specify the `RUST_LOG` environment variable when running the binary. By default logs are written to stdout.

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

Documentation on how to setup a CRON job within the Slurm cluster can be found at [slurm documentation](https://embl.service-now.com/esc?id=kb_article&table=kb_knowledge&sysparm_article=KB0010982#mcetoc_1grgc1g622).

For example, to setup a CRON job to run the `gwas_catalog_data_mover.sh` script every Monday at 7:30am, you can create a file `gwas_catalog_rsync_cron` with the following content:

```bash
#SCRON -t 1
#SCRON --mem=1
#SCRON -J gwas_catalog_rsync_cron
# min hour day-of-month month day-of-week command
30 7 * * 1 sbatch /homes/ochoa/gwas-summary-stats/gwas_catalog_data_mover.sh
```

## Syncing

The sync includes:

- harmonised summary statistics files (`h.tsv.gz`)
- metadata files (`h.tsv.gz.meta.yaml`)

## yaml dump

The script also creates a YAML metadata dump (parquet file) with the following structure:

---

| studyId | ebiDateMetadataLastModified | ebiSummaryStatisticsMd5sum | ebiSummaryStatisticsPath | isHarmonisedByEbi | isLatest |
| ------- | --------------------------- | -------------------------- | ------------------------ | ----------------- | -------- |
| GCSTXXX | 2025-02-07                  | fasfasffasfsdfasafasf      | some_path/h.tsv.gz       | true              | true     |

the parquet output file is run by triggering the `ebi-to-gcp` binary.

### Testing ebi-to-gcp with 1000 YAML Files

This document describes the comprehensive test suite created to verify that the `ebi-to-gcp` application works correctly when processing 1000 YAML metadata files.

#### 1000 YAML Files (`test_main_with_1000_yaml_files`)

- **Purpose**: Verifies the application can handle exactly 1000 YAML files
- **Creates**: 1000 mock YAML metadata files with realistic structure
- **Tests**: Complete end-to-end processing pipeline
- **Measures**: Performance metrics including processing time and throughput
- **Validates**: Output Parquet file creation and non-empty content

#### How to Run the Tests

```bash
# Test with 1000 YAML files
cargo test test_main_with_1000_yaml_files --release -- --nocapture
```
