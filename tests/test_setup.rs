use std::fs;
use tempfile::TempDir;

// Simple test to verify our setup works
#[test]
fn test_yaml_file_creation() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let test_path = temp_dir.path().join("GCST000001");
    fs::create_dir_all(&test_path).expect("Failed to create directory");

    let yaml_content = r#"gwas_id: "GCST000001"
data_file_md5sum: "abcd1234567890abcdef1234567890ab"
data_file_name: "GCST000001.h.tsv.gz"
date_metadata_last_modified: "2024-10-28"
is_harmonised: true
"#;

    let yaml_file = test_path.join("GCST000001.h.tsv.gz-meta.yaml");
    fs::write(&yaml_file, yaml_content).expect("Failed to write YAML file");

    assert!(yaml_file.exists());
    let content = fs::read_to_string(&yaml_file).expect("Failed to read file");
    assert!(content.contains("GCST000001"));

    println!("✅ Basic YAML file creation test passed!");
}
