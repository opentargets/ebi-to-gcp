use chrono::Utc;
use clap::Parser;
use ebi_to_gcp::cli::Cli;
use ebi_to_gcp::parallel::process_yaml_files_async;
use ebi_to_gcp::read::MetadataFile;
use log::{error, info};
use polars::lazy::dsl as pl;
use polars::prelude::*;
use std::process::exit;
use walkdir::WalkDir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Check if the dataset path exists
    if !(cli.dataset_path).exists() {
        error!(
            "Dataset path does not exist: {}",
            &cli.dataset_path.display()
        );
        exit(1);
    }
    info!(
        "Processing dataset at path: {}",
        &cli.dataset_path.display()
    );

    // Check if the output path exists
    if (cli.output_path).exists() {
        error!("Output path already exists: {}", &cli.output_path.display());
        exit(1);
    }
    info!(
        "Output will be written to path: {}",
        &cli.output_path.display()
    );

    info!("Searching for metadata files...");
    let start_time = Utc::now();
    // Extract the meta.yaml files and their metadata
    let metadata_files: Vec<MetadataFile> = WalkDir::new(&cli.dataset_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().to_string_lossy().ends_with("h.tsv.gz-meta.yaml"))
        .map(|e| MetadataFile::new(e.path().to_path_buf()))
        .collect();
    let end_time = Utc::now();
    let duration = end_time - start_time;
    info!("Found {} metadata files.", metadata_files.len());
    info!(
        "Time taken to search for metadata files: {} seconds",
        duration.num_seconds()
    );

    let start_time = Utc::now();
    info!("Generating sync table...");

    // Read all yaml files from the MetadataFile.paths as a dataframe and merge them
    let progress_counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // Concatenate all yaml DataFrames into one - n threads
    let yaml_metadata_dfs =
        process_yaml_files_async(metadata_files.as_slice(), progress_counter, cli.n_threads)
            .await
            .expect("Failed to process YAML files")
            .into_iter()
            .reduce(|acc, df| acc.vstack(&df).expect("failed to vstack DataFrames"))
            .expect("No DataFrames to concatenate");
    let end_time = Utc::now();
    let duration = end_time - start_time;
    info!(
        "Time taken to generate sync table: {} seconds",
        duration.num_seconds()
    );

    info!(
        "Transforming and writing sync table to Parquet under {}",
        &cli.output_path.display()
    );
    let mut file = std::fs::File::create(&cli.output_path).expect("Failed to create file");
    let start_time = Utc::now();

    // Process the metadata DataFrame
    let mut metadata_df = yaml_metadata_dfs
        .lazy()
        .select([
            pl::col("gwasId").alias("studyId"),
            pl::col("dateMetadataLastModified")
                .cast(DataType::Date)
                .alias("ebiDateMetadataLastModified"),
            pl::col("dataFileMd5sum").alias("ebiSummaryStatisticsMd5sum"),
            pl::col("dataFileName").alias("ebiSummaryStatisticsPath"),
            pl::col("isHarmonisedByEbi"),
            pl::col("dateMetadataLastModified")
                .cast(DataType::Date)
                .max()
                .over([pl::col("gwasId")])
                .eq(pl::col("dateMetadataLastModified").cast(DataType::Date))
                .alias("isLatest"),
        ])
        .collect()
        .expect("Failed to collect DataFrame");

    _ = ParquetWriter::new(&mut file)
        .finish(&mut metadata_df)
        .expect("Failed to write DataFrame to Parquet");
    let end_time = Utc::now();
    let duration = end_time - start_time;
    info!(
        "Time taken to write sync table: {} seconds",
        duration.num_seconds()
    );
    info!("Sync table generation complete.");
    Ok(())
}
