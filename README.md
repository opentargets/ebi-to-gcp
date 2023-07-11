# gwas-summary-stats

Launching summary stats computation based on changes in the source data

## Usage

Clone the repository and run the 'job scheduler' script in an LSF interactive session (_datamovers_ queue), it's already capped to just the first study in the list of studies.

```bash
# Example, in your LSF home folder
mkdir -p src
cd src
git clone <this_repo_https_url>

# Get an LSF interactive session in the 'datamovers' queue, which is the one that has access to the FTP filesystem
cd gwas-summary-stats
bsub -Is -q datamover bash

# Jump to the cloned repo
cd src/gwas-summary-stats

# Run the job scheduler
./job_scheduler.sh
```

When it's done, it will try to upload the data to the Google Cloud bucket
> gs://open-targets-gwas-summary-stats
