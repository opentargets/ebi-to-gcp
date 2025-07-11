#![allow(clippy::from_over_into)]
use chrono::{DateTime, Utc};
// use deltalake::DeltaTable;
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
}
fn main() {
    let cli = Cli::parse();
    let dataset_path = cli.path;

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

    // Transform into a polars DataFrame
    let metadata_files_df = metadata_files_to_dataframe(&metadata_files)
        .lazy()
        .with_column(
            pl::col("path")
                .str()
                .split(pl::lit("/"))
                .list()
                .last()
                .alias("ebiSummaryStatisticsFileName"),
        )
        .collect()
        .expect("Failed to collect DataFrame");
    println!("Metadata Files DataFrame:\n{:#?}", metadata_files_df);

    // Read all yaml files from the MetadataFile.paths as a dataframe and merge them
    let mut all_dataframes: Vec<DataFrame> = Vec::new();
    for metadata_file in &metadata_files {
        let content: MetadataFileContent = MetadataFileContent::from(metadata_file.path.clone());
        let df: DataFrame = content.into();
        all_dataframes.push(df)
    }

    // Concatenate all yaml DataFrames into one
    let yaml_metadata_dfs = all_dataframes
        .into_iter()
        .reduce(|acc, df| acc.vstack(&df).expect("failed to vstack DataFrames"))
        .expect("No DataFrames to concatenate");

    println!("YAML Metadata DataFrame:\n{:#?}", yaml_metadata_dfs);
    // Process the metadata DataFrame
    let metadata_df = metadata_files_df
        .join(
            &yaml_metadata_dfs,
            ["studyId", "ebiSummaryStatisticsFileName"],
            ["gwasId", "dataFileName"],
            JoinArgs::new(JoinType::Inner),
            None,
        )
        .expect("Failed to join DataFrames")
        .lazy()
        .select([
            pl::col("studyId"),
            pl::col("path").alias("ebiSummaryStatisticsPath"),
            pl::col("dateMetadataLastModifiedFromOs").alias("ebiMetadataLastModified"),
            pl::col("dataFileMd5sum").alias("ebiSummaryStatisticsMd5sum"),
            pl::col("dataFileName").alias("ebiSummaryStatisticsFileName"),
            pl::col("isHarmonisedByEbi"),
            pl::when(
                pl::col("dateMetadataLastModifiedFromOs")
                    .cast(DataType::Date)
                    .over([pl::col("studyId")])
                    .max()
                    == pl::col("dateMetadataLastModifiedFromOs").cast(DataType::Date),
            )
            .then(pl::lit(true))
            .otherwise(pl::lit(false))
            .alias("isLatest"),
        ])
        .collect()
        .expect("Failed to collect DataFrame");

    println!("Joined Metadata DataFrame:\n{:#?}", metadata_df);
}

fn metadata_files_to_dataframe(metadata_files: &[MetadataFile]) -> DataFrame {
    let paths: Vec<String> = metadata_files
        .iter()
        .map(|m| m.path.display().to_string())
        .collect();
    let datetimes: Vec<String> = metadata_files
        .iter()
        .map(|m| m.datetime.to_rfc3339())
        .collect();
    let study_ids: Vec<String> = metadata_files
        .iter()
        .map(|m| m.study_id.id.clone())
        .collect();

    DataFrame::new(vec![
        Column::new("path".into(), paths),
        Column::new("dateMetadataLastModifiedFromOs".into(), datetimes),
        Column::new("studyId".into(), study_ids),
    ])
    .unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataFile {
    pub path: PathBuf,
    pub datetime: DateTime<Utc>,
    pub study_id: StudyId,
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
        MetadataFile {
            path,
            datetime,
            study_id,
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
