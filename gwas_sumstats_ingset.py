"""Dataset class for OTG."""
from __future__ import annotations

from typing import List
import argparse
import logging
import re
import sys
from scipy.stats import norm

from pyspark.sql import SparkSession, types as t, functions as f, DataFrame, Column


def parse_pvalue(pv: Column) -> List[Column]:
    """This function takes a p-value string and returns two columns mantissa (float), exponent (integer).

    Args:
        pv (Column): P-value as string

    Returns:
        Column: p-value mantissa (float)
        Column: p-value exponent (integer)

    Examples:
        >>> d = [("0.01",),("4.2E-45",),("43.2E5",),("0",),("1",)]
        >>> spark.createDataFrame(d, ['pval']).select('pval',*parse_pvalue(f.col('pval'))).show()
        +-------+--------------+--------------+
        |   pval|pValueMantissa|pValueExponent|
        +-------+--------------+--------------+
        |   0.01|           1.0|            -2|
        |4.2E-45|           4.2|           -45|
        | 43.2E5|          43.2|             5|
        |      0|         2.225|          -308|
        |      1|           1.0|             0|
        +-------+--------------+--------------+
        <BLANKLINE>
    """
    # Making sure there's a number in the string:
    pv = f.when(
        pv == f.lit("0"), f.lit(sys.float_info.min).cast(t.StringType())
    ).otherwise(pv)

    # Get exponent:
    exponent = f.when(
        f.upper(pv).contains("E"),
        f.split(f.upper(pv), "E").getItem(1),
    ).otherwise(f.floor(f.log10(pv)))

    # Get mantissa:
    mantissa = f.when(
        f.upper(pv).contains("E"),
        f.split(f.upper(pv), "E").getItem(0),
    ).otherwise(pv / (10**exponent))

    # Round value:
    mantissa = f.round(mantissa, 3)

    return [
        mantissa.cast(t.FloatType()).alias("pValueMantissa"),
        exponent.cast(t.IntegerType()).alias("pValueExponent"),
    ]


def convert_odds_ratio_to_beta(
    beta: Column, odds_ratio: Column, standard_error: Column
) -> List[Column]:
    """Harmonizes effect and standard error to beta.

    Args:
        beta (Column): Effect in beta
        odds_ratio (Column): Effect in odds ratio
        standard_error (Column): Standard error of the effect

    Returns:
        tuple: beta, standard error

    Examples:
        >>> df = spark.createDataFrame([{"beta": 0.1, "oddsRatio": 1.1, "standardError": 0.1}, {"beta": None, "oddsRatio": 1.1, "standardError": 0.1}, {"beta": 0.1, "oddsRatio": None, "standardError": 0.1}, {"beta": 0.1, "oddsRatio": 1.1, "standardError": None}])
        >>> df.select("*", *convert_odds_ratio_to_beta(f.col("beta"), f.col("oddsRatio"), f.col("standardError"))).show()
        +----+---------+-------------+-------------------+-------------+
        |beta|oddsRatio|standardError|               beta|standardError|
        +----+---------+-------------+-------------------+-------------+
        | 0.1|      1.1|          0.1|                0.1|          0.1|
        |null|      1.1|          0.1|0.09531017980432493|         null|
        | 0.1|     null|          0.1|                0.1|          0.1|
        | 0.1|      1.1|         null|                0.1|         null|
        +----+---------+-------------+-------------------+-------------+
        <BLANKLINE>

    """
    # We keep standard error when effect is given in beta, otherwise drop.
    standard_error = f.when(
        standard_error.isNotNull() & beta.isNotNull(), standard_error
    ).alias("standardError")

    # Odds ratio is converted to beta:
    beta = (
        f.when(beta.isNotNull(), beta)
        .when(odds_ratio.isNotNull(), f.log(odds_ratio))
        .alias("beta")
    )

    return [beta, standard_error]


def pvalue_to_zscore(pval_col: Column) -> Column:
    """Convert p-value column to z-score column.

    Args:
        pval_col (Column): pvalues to be casted to floats.

    Returns:
        Column: p-values transformed to z-scores

    Examples:
        >>> d = [{"id": "t1", "pval": "1"}, {"id": "t2", "pval": "0.9"}, {"id": "t3", "pval": "0.05"}, {"id": "t4", "pval": "1e-300"}, {"id": "t5", "pval": "1e-1000"}, {"id": "t6", "pval": "NA"}]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("zscore", pvalue_to_zscore(f.col("pval"))).show()
        +---+-------+----------+
        | id|   pval|    zscore|
        +---+-------+----------+
        | t1|      1|       0.0|
        | t2|    0.9|0.12566137|
        | t3|   0.05|  1.959964|
        | t4| 1e-300| 37.537838|
        | t5|1e-1000| 37.537838|
        | t6|     NA|      null|
        +---+-------+----------+
        <BLANKLINE>

    """
    pvalue_float = pval_col.cast(t.FloatType())
    pvalue_nozero = f.when(pvalue_float == 0, sys.float_info.min).otherwise(
        pvalue_float
    )
    return f.udf(
        lambda pv: float(abs(norm.ppf((float(pv)) / 2))) if pv else None,
        t.FloatType(),
    )(pvalue_nozero)

def calculate_confidence_interval(
    pvalue_mantissa: Column,
    pvalue_exponent: Column,
    beta: Column,
    standard_error: Column,
) -> tuple:
    """This function calculates the confidence interval for the effect based on the p-value and the effect size.

    If the standard error already available, don't re-calculate from p-value.

    Args:
        pvalue_mantissa (Column): p-value mantissa (float)
        pvalue_exponent (Column): p-value exponent (integer)
        beta (Column): effect size in beta (float)
        standard_error (Column): standard error.

    Returns:
        tuple: betaConfidenceIntervalLower (float), betaConfidenceIntervalUpper (float)

    Examples:
        >>> df = spark.createDataFrame([
        ...     (2.5, -10, 0.5, 0.2),
        ...     (3.0, -5, 1.0, None),
        ...     (1.5, -8, -0.2, 0.1)
        ...     ], ["pvalue_mantissa", "pvalue_exponent", "beta", "standard_error"]
        ... )
        >>> df.select("*", *calculate_confidence_interval(f.col("pvalue_mantissa"), f.col("pvalue_exponent"), f.col("beta"), f.col("standard_error"))).show()
        +---------------+---------------+----+--------------+---------------------------+---------------------------+
        |pvalue_mantissa|pvalue_exponent|beta|standard_error|betaConfidenceIntervalLower|betaConfidenceIntervalUpper|
        +---------------+---------------+----+--------------+---------------------------+---------------------------+
        |            2.5|            -10| 0.5|           0.2|                        0.3|                        0.7|
        |            3.0|             -5| 1.0|          null|         0.7603910153486024|         1.2396089846513976|
        |            1.5|             -8|-0.2|           0.1|       -0.30000000000000004|                       -0.1|
        +---------------+---------------+----+--------------+---------------------------+---------------------------+
        <BLANKLINE>
    """
    # Calculate p-value from mantissa and exponent:
    pvalue = pvalue_mantissa * f.pow(10, pvalue_exponent)

    # Fix p-value underflow:
    pvalue = f.when(pvalue == 0, sys.float_info.min).otherwise(pvalue)

    # Compute missing standard error:
    standard_error = f.when(
        standard_error.isNull(), f.abs(beta) / f.abs(pvalue_to_zscore(pvalue))
    ).otherwise(standard_error)

    # Calculate upper and lower confidence interval:
    ci_lower = (beta - standard_error).alias("betaConfidenceIntervalLower")
    ci_upper = (beta + standard_error).alias("betaConfidenceIntervalUpper")

    return (ci_lower, ci_upper)



# Moving in all the logic:
def from_gwas_harmonized_summary_stats(
    sumstats_df: DataFrame,
    study_id: str,
) -> DataFrame:
    # The effect allele frequency is an optional column, we have to test if it is there:
    allele_frequency_expression = (
        f.col("hm_effect_allele_frequency").cast(t.FloatType())
        if "hm_effect_allele_frequency" in sumstats_df.columns
        else f.lit(None)
    )

    # Processing columns of interest:
    processed_sumstats_df = (
        sumstats_df
        # Dropping rows which doesn't have proper position:
        .filter(f.col("hm_pos").cast(t.IntegerType()).isNotNull())
        .select(
            # Adding study identifier:
            f.lit(study_id).cast(t.StringType()).alias("studyId"),
            # Adding variant identifier:
            f.col("hm_variant_id").alias("variantId"),
            f.col("hm_chrom").alias("chromosome"),
            f.col("hm_pos").cast(t.IntegerType()).alias("position"),
            # Parsing p-value mantissa and exponent:
            *parse_pvalue(f.col("p_value")),
            # Converting/calculating effect and confidence interval:
            *convert_odds_ratio_to_beta(
                f.col("hm_beta").cast(t.DoubleType()),
                f.col("hm_odds_ratio").cast(t.DoubleType()),
                f.col("standard_error").cast(t.DoubleType()),
            ),
            allele_frequency_expression.alias("effectAlleleFrequencyFromSource"),
        )
        # The previous select expression generated the necessary fields for calculating the confidence intervals:
        .select(
            "*",
            *calculate_confidence_interval(
                f.col("pValueMantissa"),
                f.col("pValueExponent"),
                f.col("beta"),
                f.col("standardError"),
            ),
        )
        .repartition(200, "chromosome")
        .sortWithinPartitions("position")
    )

    # Initializing summary statistics object:
    return processed_sumstats_df

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
    from_gwas_harmonized_summary_stats(ss_df, study_id).df.write.mode('overwrite').parquet(output_file)

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
