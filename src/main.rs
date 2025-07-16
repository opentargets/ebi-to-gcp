#![allow(clippy::from_over_into)]
use chrono::{DateTime, Utc};
use clap::Parser;
use polars::lazy::dsl as pl;
use polars::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::{path::PathBuf, process::exit};
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// Path to the dataset directory
    #[arg()]
    path: PathBuf,
    table_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let dataset_path = cli.path;
    let table_path = cli.table_path;

    // Check if the dataset path exists
    if !dataset_path.exists() {
        eprintln!("Dataset path does not exist: {}", dataset_path.display());
        exit(1);
    }

    // Extract the meta.yaml files and their metadata
    let metadata_files: Vec<MetadataFile> = WalkDir::new(dataset_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().to_string_lossy().ends_with("h.tsv.gz-meta.yaml"))
        .map(|e| MetadataFile::new(e.path().to_path_buf()))
        .collect();

    // Read all yaml files from the MetadataFile.paths as a dataframe and merge them
    let mut all_dataframes: Vec<DataFrame> = Vec::new();
    for metadata_file in &metadata_files {
        let mut content: MetadataFileContent =
            MetadataFileContent::from(metadata_file.path.clone());
        content.update_full_path(metadata_file.base_path.clone());
        let df: DataFrame = content.into();
        all_dataframes.push(df)
    }

    // Concatenate all yaml DataFrames into one
    let yaml_metadata_dfs = all_dataframes
        .into_iter()
        .reduce(|acc, df| acc.vstack(&df).expect("failed to vstack DataFrames"))
        .expect("No DataFrames to concatenate");

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

    println!("Joined Metadata DataFrame:\n{:#?}", metadata_df);
    println!(
        "Joined Metadata DataFrame Schema:\n{:#?}",
        metadata_df.schema()
    );

    let mut file = std::fs::File::create(&table_path).expect("Failed to create file");
    _ = ParquetWriter::new(&mut file)
        .finish(&mut metadata_df)
        .expect("Failed to write DataFrame to Parquet");

    Ok(())
}

#[derive(Debug)]
pub struct StudyId {
    pub id: String,
}

impl From<PathBuf> for StudyId {
    fn from(path: PathBuf) -> Self {
        let pattern = Regex::new(r".*(GCST\d+).*").unwrap();
        let path_str = path.to_string_lossy();
        let id = pattern
            .captures(&path_str)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str())
            .unwrap_or_default()
            .to_string();
        StudyId { id }
    }
}

#[derive(Debug)]
pub struct MetadataFile {
    pub path: PathBuf,
    pub datetime: DateTime<Utc>,
    pub study_id: StudyId,
    // Expect the yaml file to be in the same directory as the actual data file
    pub base_path: PathBuf,
}

impl MetadataFile {
    fn new(path: PathBuf) -> Self {
        let metadata = fs::metadata(&path).expect("Failed to read metadata");
        let duration = metadata
            .modified()
            .expect("Failed to get modification time")
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards");
        let secs = duration.as_secs() as i64;
        let nsecs = duration.subsec_nanos();
        let datetime = DateTime::from_timestamp(secs, nsecs).expect("Invalid timestamp");
        let study_id = StudyId::from(path.clone());
        let base_path = path
            .parent()
            .expect("Failed to get parent directory")
            .to_path_buf();
        MetadataFile {
            path,
            datetime,
            study_id,
            base_path,
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
    pub fn datetime(&self) -> DateTime<Utc> {
        self.datetime
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct MetadataFileContent {
    gwas_id: Option<String>,
    data_file_md5sum: Option<String>,
    data_file_name: Option<String>,
    date_metadata_last_modified: Option<String>,
    is_harmonised: Option<bool>,
}

impl From<PathBuf> for MetadataFileContent {
    fn from(path: PathBuf) -> Self {
        // Read the yaml using serde_yaml
        let content = fs::read_to_string(&path).expect("Failed to read file");
        serde_yaml::from_str(&content).expect("Failed to parse YAML")
    }
}
impl MetadataFileContent {
    pub fn update_full_path(&mut self, mut base_path: PathBuf) {
        if let Some(file_name) = &self.data_file_name {
            base_path.push(file_name);
            self.data_file_name = Some(base_path.to_string_lossy().to_string());
        }
    }
}

impl Into<DataFrame> for MetadataFileContent {
    fn into(self) -> DataFrame {
        DataFrame::new(vec![
            Column::new("gwasId".into(), vec![self.gwas_id]),
            Column::new("dataFileMd5sum".into(), vec![self.data_file_md5sum]),
            Column::new("dataFileName".into(), vec![self.data_file_name]),
            Column::new(
                "dateMetadataLastModified".into(),
                vec![self.date_metadata_last_modified],
            ),
            Column::new("isHarmonisedByEbi".into(), vec![self.is_harmonised]),
        ])
        .expect("Failed to create DataFrame")
    }
}
