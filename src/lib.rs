#![allow(clippy::from_over_into)]
pub mod cli {

    use clap::Parser;
    use std::path::PathBuf;

    #[derive(Parser)]
    #[command(author, version, about)]
    pub struct Cli {
        /// Path to the dataset directory
        #[arg()]
        pub dataset_path: PathBuf,
        /// Output path to the sync table
        #[arg()]
        pub output_path: PathBuf,
        #[arg(short, long, default_value_t = 50)]
        pub n_threads: usize,
    }
}
pub mod read {

    use chrono::{DateTime, Utc};
    use polars::prelude::*;
    use regex::Regex;
    use serde::{Deserialize, Serialize};
    use std::fs;
    use std::path::PathBuf;

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
        pub fn new(path: PathBuf) -> Self {
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
    pub struct MetadataFileContent {
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
}

pub mod parallel {
    use super::read::{MetadataFile, MetadataFileContent};
    use futures::stream::{self, StreamExt};
    use log::{error, info};
    use polars::frame::DataFrame;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::task;

    pub async fn process_yaml_files_async(
        metadata_files: &[MetadataFile],
        progress_counter: Arc<AtomicUsize>,
        n_threads: usize,
    ) -> Result<Vec<DataFrame>, Box<dyn std::error::Error + Send + Sync>> {
        let results = stream::iter(metadata_files)
            .map(|metadata_file| {
                let progress = progress_counter.clone();
                async move {
                    let result = process_single_yaml_file(metadata_file).await;
                    let completed = progress.fetch_add(1, Ordering::Relaxed) + 1;
                    if completed % 100 == 0 {
                        info!("Processed {}/{} files", completed, metadata_files.len());
                    }
                    result
                }
            })
            .buffer_unordered(n_threads)
            .collect::<Vec<_>>()
            .await;

        // Handle results and errors
        let mut dataframes = Vec::new();
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok(df) => dataframes.push(df),
                Err(e) => {
                    error!("Failed to process YAML file: {e}");
                    errors.push(e);
                }
            }
        }

        if !errors.is_empty() {
            error!("Encountered {} errors while processing files", errors.len());
            // Decide whether to continue or fail based on your requirements
        }

        Ok(dataframes)
    }

    async fn process_single_yaml_file(
        metadata_file: &MetadataFile,
    ) -> Result<DataFrame, Box<dyn std::error::Error + Send + Sync>> {
        // Spawn a blocking task for CPU-intensive YAML parsing
        let path = metadata_file.path.clone();
        let base_path = metadata_file.base_path.clone();

        task::spawn_blocking(move || {
            let mut content = MetadataFileContent::from(path);
            content.update_full_path(base_path);
            let df: DataFrame = content.into();
            Ok::<DataFrame, Box<dyn std::error::Error + Send + Sync>>(df)
        })
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
    }
}
