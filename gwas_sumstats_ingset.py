"""Dataset class for OTG."""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession
from scipy.stats import norm

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType


def _pval_to_zscore(pval_col: Column) -> Column:
    """Convert p-value column to z-score column.

    Args:
        pval_col (Column): pvalues to be casted to floats.

    Returns:
        Column: p-values transformed to z-scores

    Examples:
        >>> d = [{"id": "t1", "pval": "1"}, {"id": "t2", "pval": "0.9"}, {"id": "t3", "pval": "0.05"}, {"id": "t4", "pval": "1e-300"}, {"id": "t5", "pval": "1e-1000"}, {"id": "t6", "pval": "NA"}]
        >>> df = spark.createDataFrame(d)
        >>> df.withColumn("zscore", _pval_to_zscore(f.col("pval"))).show()
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


def parse_spark_schema(schema_json: str = None) -> StructType:
    """It takes a JSON string and returns a Spark StructType object.

    Args:
        schema_json (str): str = None

    Returns:
        A StructType object
    """
    summary_stats_schema_string = """
    {
        "fields": [
            {
            "metadata": {},
            "name": "studyId",
            "nullable": false,
            "type": "string"
            },
            {
            "metadata": {},
            "name": "variantId",
            "nullable": true,
            "type": "string"
            },
            {
            "metadata": {},
            "name": "chromosome",
            "nullable": true,
            "type": "string"
            },
            {
            "metadata": {},
            "name": "position",
            "nullable": true,
            "type": "long"
            },
            {
            "metadata": {},
            "name": "beta",
            "nullable": true,
            "type": "double"
            },
            {
            "metadata": {},
            "name": "betaConfidenceIntervalLower",
            "nullable": true,
            "type": "double"
            },
            {
            "metadata": {},
            "name": "betaConfidenceIntervalUpper",
            "nullable": true,
            "type": "double"
            },
            {
            "metadata": {},
            "name": "pValueMantissa",
            "nullable": true,
            "type": "double"
            },
            {
            "metadata": {},
            "name": "pValueExponent",
            "nullable": true,
            "type": "long"
            },
            {
            "metadata": {},
            "name": "effectAlleleFrequencyFromSource",
            "nullable": true,
            "type": "double"
            }
        ],
        "type": "struct"
    }
    """
    core_schema = json.loads(summary_stats_schema_string)
    return t.StructType.fromJson(core_schema)


@dataclass
class Dataset:
    """Open Targets Genetics Dataset.

    `Dataset` is a wrapper around a Spark DataFrame with a predefined schema. Schemas for each child dataset are described in the `json.schemas` module.
    """

    _df: DataFrame
    _schema: StructType

    def __post_init__(self: Dataset) -> None:
        """Post init."""
        self.validate_schema()

    @property
    def df(self: Dataset) -> DataFrame:
        """Dataframe included in the Dataset."""
        return self._df

    @df.setter
    def df(self: Dataset, new_df: DataFrame) -> None:
        """Dataframe setter."""
        self._df = new_df
        self.validate_schema()

    @property
    def schema(self: Dataset) -> StructType:
        """Dataframe expected schema."""
        return self._schema

    @classmethod
    def from_parquet(
        cls: type[Dataset], session: SparkSession, path: str, schema: StructType
    ) -> Dataset:
        """Reads a parquet file into a Dataset with a given schema.

        Args:
            session (Session): SparkSession
            path (str): Path to parquet file
            schema (StructType): Schema to use

        Returns:
            Dataset: Dataset with given schema
        """
        df = session.read.schema(schema).format("parquet").load(path)
        return cls(_df=df, _schema=schema)

    def validate_schema(self: Dataset) -> None:
        """Validate DataFrame schema against expected class schema.

        Raises:
            ValueError: DataFrame schema is not valid
        """
        expected_schema = self._schema  # type: ignore[attr-defined]
        observed_schema = self.df.schema  # type: ignore[attr-defined]
        # Observed fields not in schema
        missing_struct_fields = [x for x in observed_schema if x not in expected_schema]
        error_message = f"The {missing_struct_fields} StructFields are not included in DataFrame schema: {expected_schema}"
        if missing_struct_fields:
            raise ValueError(error_message)

        # Required fields not in dataset
        required_fields = [x for x in expected_schema if not x.nullable]
        missing_required_fields = [
            x for x in required_fields if x not in observed_schema
        ]
        error_message = f"The {missing_required_fields} StructFields are required but missing from the DataFrame schema: {expected_schema}"
        if missing_required_fields:
            raise ValueError(error_message)

    def persist(self: Dataset) -> Dataset:
        """Persist DataFrame included in the Dataset."""
        self._df = self.df.persist()
        return self


@dataclass
class SummaryStatistics(Dataset):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    _schema: StructType = parse_spark_schema("summary_statistics.json")

    @classmethod
    def from_parquet(
        cls: type[SummaryStatistics], session: SparkSession, path: str
    ) -> SummaryStatistics:
        """Initialise SummaryStatistics from parquet file.

        Args:
            session (SparkSession): SparkSession
            path (str): Path to parquet file

        Returns:
            SummaryStatistics: SummaryStatistics dataset
        """
        return super().from_parquet(session, path, cls._schema)

    @staticmethod
    def _parse_pvalue(pv: Column) -> tuple:
        """Extract p-value mantissa and exponent from p-value.

        Args:
            pv (Column): column with p-values. Will try to cast to float.

        Returns:
            tuple: contains columns pValueMantissa and pValueExponent

        Examples:
            >>> data = [(1.0),(0.5), (1e-20), (3e-3)]
            >>> spark.createDataFrame(data, t.FloatType()).select('value',*parsepv(f.col('value'))).show()
            +-------+--------------+--------------+
            |  value|pValueMantissa|pValueExponent|
            +-------+--------------+--------------+
            |    1.0|           1.0|             0|
            |    0.5|           5.0|            -1|
            |1.0E-20|           1.0|           -20|
            |  0.003|           3.0|            -3|
            +-------+--------------+--------------+
        """
        pv = f.when(pv == 0, sys.float_info.min).otherwise(pv)

        # To get the right exponent we need to do some rounding:
        exponent = f.floor(f.round(f.log10(pv), 4)).alias("pValueExponent")

        # Mantissa also needs to be rounded:
        mantissa = f.round(pv * f.pow(f.lit(10), -exponent), 3).alias("pValueMantissa")

        return (mantissa, exponent)

    @staticmethod
    def _harmonize_effect(
        pvalue: Column,
        beta: Column,
        odds_ratio: Column,
        ci_lower: Column,
        standard_error: Column,
    ) -> tuple:
        """Harmonizing effect to beta.

        - If odds-ratio is given, calculate beta.
        - If odds ratio is given re-calculate lower and upper confidence interval.
        - If no standard error is given, calculate from p-value and effect.

        Args:
            pvalue (Column): mandatory
            beta (Column): optional
            odds_ratio (Column): optional either beta or or should be provided.
            ci_lower (Column): optional
            standard_error (Column): optional

        Returns:
            tuple: columns containing beta, beta_ci_lower and beta_ci_upper
        """
        perc_97th = 1.95996398454005423552

        # Converting all effect to beta:
        beta = (
            f.when(odds_ratio.isNotNull(), f.log(odds_ratio))
            .otherwise(beta)
            .alias("beta")
        )

        # Convert/calculate standard error when needed:
        standard_error = (
            f.when(
                standard_error.isNull(), f.abs(beta) / f.abs(_pval_to_zscore(pvalue))
            )
            .when(
                odds_ratio.isNotNull(),
                (f.log(odds_ratio) - f.log(ci_lower)) / perc_97th,
            )
            .otherwise(standard_error)
        )

        # Calculate upper and lower beta:
        ci_lower = (beta - standard_error).alias("betaConfidenceIntervalLower")
        ci_upper = (beta + standard_error).alias("betaConfidenceIntervalUpper")

        return (beta, ci_lower, ci_upper)

    @staticmethod
    def _path_2_study_id(sumstats_location: str) -> str:
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

    @classmethod
    def from_gwas_harmonized_summary_stats(
        cls: type[SummaryStatistics], sumstats_df: DataFrame, study_id: str
    ) -> SummaryStatistics:
        """Create summary statistics object from harmonized dataset.

        Args:
            cls (type[SummaryStatistics]):
            sumstats_df (DataFrame): Harmonized dataset read as dataframe from GWAS Catalog.
            study_id (str): GWAS Catalog Study accession.

        Returns:
            SummaryStatistics:
        """
        # The effect allele frequency is an optional column, we have to test if it is there:
        allele_frequency_expression = (
            f.col("hm_effect_allele_frequency").cast(t.DoubleType())
            if "hm_effect_allele_frequency" in sumstats_df.columns
            else f.lit(None)
        )

        # Processing columns of interest:
        processed_sumstats_df = sumstats_df.select(
            # Adding study identifier:
            f.lit(study_id).cast(t.StringType()).alias("studyId"),
            # Adding variant identifier:
            f.col("hm_variant_id").alias("variantId"),
            f.col("hm_chrom").alias("chromosome"),
            f.col("hm_pos").cast(t.LongType()).alias("position"),
            # Parsing p-value mantissa and exponent:
            *cls._parse_pvalue(f.col("p_value").cast(t.FloatType())),
            # Converting/calculating effect and confidence interval:
            *cls._harmonize_effect(
                f.col("p_value").cast(t.FloatType()),
                f.col("hm_beta").cast(t.FloatType()),
                f.col("hm_odds_ratio").cast(t.FloatType()),
                f.col("hm_ci_lower").cast(t.FloatType()),
                f.col("standard_error").cast(t.FloatType()),
            ),
            allele_frequency_expression.alias("effectAlleleFrequencyFromSource"),
        )
        # Initializing summary statistics object:
        summary_stats = cls(
            _df=processed_sumstats_df,
        )

        summary_stats._df = summary_stats._df.repartition(
            200,
            "chromosome",
        ).sortWithinPartitions("position")
        return summary_stats


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
    spark: SparkSession, input_file: str, output_file: str, pval_threshold: float
) -> None:
    """Main function to process summary statistics.

    Args:
        spark (SparkSession): Sparksession
        input_file (str): Input gzipped tsv
        output_file (str): Output parquet file
        pval_threshold (float): upper limit for p-value

    Returns:
        None
    """
    study_id = path_2_study_id(input_file)
    ss_df = spark.read.csv(input_file, sep="\t", header=True)
    ss = SummaryStatistics.from_gwas_harmonized_summary_stats(ss_df, study_id)

    print(str(ss))

    # TODO: implement filter functionality.
    # if pval_threshold != 1:
    # # Applying p-value threshold:
    #     ss.filter_by_pvalue(pval_threshold)
    logging.warning(f"Writing summary statistics to {output_file}")
    ss._df.write.mode("overwrite").parquet(output_file)


def parse_arguments() -> tuple:
    """Parsing command line arguments.

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
    parser.add_argument(
        "--pval_threshold",
        help="P-value threshold above which we drop single point associations.",
        type=float,
        required=False,
        default=1.0,
    )

    # Tell the parser to set the error exit code if missing arguments
    parser.exit_on_error = True

    args = parser.parse_args()
    return (args.input_file, args.output_file, args.pval_threshold)


if __name__ == "__main__":
    # Parse parameters.
    (input_file, output_file, pval_threshold) = parse_arguments()

    # Set up logging:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logging.info(f"Processing summary statistics: {input_file}")
    logging.info(f"Applied p-value threshold: {pval_threshold}")

    # Initialize local spark:
    spark = SparkSession.builder.getOrCreate()

    # Process data:
    main(spark, input_file, output_file, pval_threshold)

    logging.info(f"Processed dataset saved to: {output_file}")
