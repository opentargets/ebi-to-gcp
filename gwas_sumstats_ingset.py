"""Dataset class for OTG."""
from __future__ import annotations

import argparse
import logging
import re
import sys

from pyspark.sql import SparkSession
from otg.dataset.summary_statistics import SummaryStatistics

def path_2_study_id(sumstats_location: str) -> str:
    """Extract GWAS Catalog study accession from sumstats file location.

    Args:
        sumstats_location (str): path to the sumstats file.

    Raises:
        ValueError: if the study id cold not be found in the path.

    Returns:
        str: GWAS Catalog study accession GSCT...
    """
    filename = sumstats_location.split("/")[-1]
    study_accession_pattern = r"(GCST\d+)"
    study_pattern = re.search(study_accession_pattern, filename)
    try:
        assert study_pattern is not None
        gwas_study_accession = study_pattern[0]
    except AssertionError:
        raise ValueError(
            f"Could not find GWAS Catalog study accession in file: {filename}"
        )
    return gwas_study_accession


def main(
    spark: SparkSession, input_file: str, output_file: str
) -> None:
    """Process summary statistics.

    Args:
        spark (SparkSession): Sparksession
        input_file (str): Input gzipped tsv
        output_file (str): Output parquet file
    """
    # Parse study accession:
    study_id = path_2_study_id(input_file)
    logging.warning(f"Processing study {study_id}")
    # Read tsv as spark dataframe:
    ss_df = spark.read.csv(input_file, sep="\t", header=True)
    logging.warning(f"Read {ss_df.count()} rows from {input_file}")

    # Converting dataframe into summary stat object:
    (
        SummaryStatistics
        .from_gwas_harmonized_summary_stats(ss_df, study_id)
        .df.write.mode('overwrite').parquet(output_file)
    )

    logging.warning(f"Summary statistics parquet saved to: {output_file}")


def parse_arguments() -> tuple:
    """Parse command line arguments.

    Returns:
        tuple: list of arguments
    """
    parser = argparse.ArgumentParser(
        description="Tool to ingest harmonized GWAS Catalog summary statistics flat file."
    )

    parser.add_argument(
        "--input_file",
        help="Harmonized summary statistics in tsv.gz.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--output_file",
        help="Output processed summary statistics in parquet format.",
        type=str,
        required=True,
    )

    # Tell the parser to set the error exit code if missing arguments
    parser.exit_on_error = True

    args = parser.parse_args()
    return (args.input_file, args.output_file)


if __name__ == "__main__":
    # Parse parameters.
    (input_file, output_file) = parse_arguments()

    # Set up logging:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logging.info(f"Processing summary statistics: {input_file}")
    logging.info(f"Saving summary statistics to folder: {output_file}")
    # Initialize local spark:
    spark = (
        SparkSession
        .builder
        .appName("gwas_sumstats")
        .master("local[12]")
        .config("spark.driver.memory", "15g")
        .getOrCreate()
    )

    # Process data:
    main(spark, input_file, output_file)
