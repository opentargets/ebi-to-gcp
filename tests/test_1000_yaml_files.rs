use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::process::Command;
use uuid::Uuid;

// NOTE: Tests were built using LLM assistance.

// Helper function to create a mock YAML file with realistic content
fn create_mock_yaml_content(gwas_id: &str, harmonised: bool) -> String {
    format!(
        r#"gwas_id: "{}"
data_file_md5sum: "{}"
data_file_name: "{}.h.tsv.gz"
date_metadata_last_modified: "2024-{:02}-{:02}"
is_harmonised: {}
"#,
        gwas_id,
        Uuid::new_v4().to_string().replace("-", "")[..32].to_string(), // Mock MD5
        gwas_id,
        rand::random::<u8>() % 12 + 1, // Month 1-12
        rand::random::<u8>() % 28 + 1, // Day 1-28
        harmonised
    )
}

// Create a directory structure with YAML files
fn create_test_directory_structure(base_dir: &PathBuf, num_files: usize) -> Vec<PathBuf> {
    let mut yaml_files = Vec::new();

    for i in 0..num_files {
        let gwas_id = format!("GCST{:06}", 1000000 + i);
        let study_dir = base_dir.join(&gwas_id);
        fs::create_dir_all(&study_dir).expect("Failed to create study directory");

        // Create the YAML file
        let yaml_filename = format!("{}.h.tsv.gz-meta.yaml", gwas_id);
        let yaml_path = study_dir.join(&yaml_filename);

        let harmonised = i % 3 == 0; // Every third file is harmonised
        let yaml_content = create_mock_yaml_content(&gwas_id, harmonised);

        fs::write(&yaml_path, yaml_content).expect("Failed to write YAML file");
        yaml_files.push(yaml_path);

        // Create the corresponding data file (empty, just for completeness)
        let data_filename = format!("{}.h.tsv.gz", gwas_id);
        let data_path = study_dir.join(&data_filename);
        fs::write(&data_path, "dummy data").expect("Failed to write data file");
    }

    yaml_files
}

#[tokio::test]
async fn test_main_with_1000_yaml_files() {
    // Initialize logger for the test
    let _ = env_logger::builder().is_test(true).try_init();

    // Create temporary directory for test data
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let dataset_path = temp_dir.path().to_path_buf();

    // Create 1000 YAML files
    println!("Creating 1000 test YAML files...");
    let yaml_files = create_test_directory_structure(&dataset_path, 1000);
    assert_eq!(
        yaml_files.len(),
        1000,
        "Should have created exactly 1000 YAML files"
    );

    // Create output file path
    let output_path = temp_dir.path().join("test_output.parquet");

    // Build the binary first
    let current_dir = std::env::current_dir().expect("Failed to get current directory");
    let build_output = Command::new("cargo")
        .args(&["build", "--release"])
        .current_dir(&current_dir)
        .output()
        .await
        .expect("Failed to execute cargo build");

    assert!(build_output.status.success(), "Build should succeed");

    // Run the main binary with our test data
    println!("Running ebi-to-gcp with 1000 YAML files...");
    let start_time = std::time::Instant::now();

    let executable_path = &current_dir.join("target/release/ebi-to-gcp");

    println!("Using executable at: {:?}", executable_path);
    println!("Using dataset at: {:?}", dataset_path);
    println!("Using output path: {:?}", output_path);
    println!("Using 50 threads for processing");

    let output = Command::new(executable_path)
        .arg(dataset_path.to_str().unwrap())
        .arg(output_path.to_str().unwrap())
        .arg("--n-threads")
        .arg("50") // Use 50 threads for faster processing
        .current_dir(&current_dir)
        .output()
        .await
        .expect("Failed to execute ebi-to-gcp");

    let duration = start_time.elapsed();

    // Check if the command succeeded
    if !output.status.success() {
        panic!(
            "ebi-to-gcp failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Print output for debugging
    println!(
        "Command output:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
    if !output.stderr.is_empty() {
        println!(
            "Command stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Verify the output file was created
    assert!(output_path.exists(), "Output parquet file should exist");

    // Check the size of the output file (should be non-empty)
    let file_metadata = fs::metadata(&output_path).expect("Failed to get file metadata");
    assert!(file_metadata.len() > 0, "Output file should not be empty");

    println!("✅ Test completed successfully!");
    println!("📊 Performance metrics:");
    println!("   - Processed files: 1000");
    println!("   - Total time: {:?}", duration);
    println!("   - Average time per file: {:?}", duration / 1000);
    println!("   - Output file size: {} bytes", file_metadata.len());

    // Optional: Verify the content using polars
    #[cfg(test)]
    {
        use polars::prelude::*;

        let df = LazyFrame::scan_parquet(&output_path, ScanArgsParquet::default())
            .unwrap()
            .collect()
            .expect("Failed to read output parquet file");

        println!("📋 Output DataFrame info:");
        println!("   - Rows: {}", df.height());
        println!("   - Columns: {}", df.width());
        println!("   - Column names: {:?}", df.get_column_names());

        // Verify we have the expected number of rows (should be 1000)
        assert_eq!(df.height(), 1000, "Should have 1000 rows in output");

        // Verify expected columns exist
        let expected_columns = vec![
            "studyId",
            "ebiDateMetadataLastModified",
            "ebiSummaryStatisticsMd5sum",
            "ebiSummaryStatisticsPath",
            "isHarmonisedByEbi",
            "isLatest",
        ];

        for col in expected_columns {
            assert!(
                df.get_column_names().iter().any(|c| c.as_str() == col),
                "Column '{}' should exist",
                col
            );
        }
    }
}
