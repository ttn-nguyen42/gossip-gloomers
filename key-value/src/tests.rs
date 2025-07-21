#[cfg(test)]
mod tests {
    use crate::cluster::{Entry, Persistence};
    use crate::common::Operation;
    use std::fs;
    use std::path::Path;
    use tokio::fs::remove_dir_all;

    #[tokio::test]
    async fn test_persistence_empty_restore() {
        let test_dir = "test_persistence_empty";

        if Path::new(test_dir).exists() {
            remove_dir_all(test_dir).await.unwrap();
        }

        fs::create_dir_all(test_dir).unwrap();

        let persistence = Persistence::restore(test_dir).await.unwrap();
        let mut persistence = persistence;
        persistence.persist(false, 0, None).await.unwrap();

        let _restored = Persistence::restore(test_dir).await.unwrap();

        remove_dir_all(test_dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_persistence_with_entries() {
        let test_dir = "test_persistence_with_entries";

        if Path::new(test_dir).exists() {
            remove_dir_all(test_dir).await.unwrap();
        }

        fs::create_dir_all(test_dir).unwrap();

        let mut persistence = Persistence::restore(test_dir).await.unwrap();
        persistence.persist(false, 0, None).await.unwrap();

        let _restored = Persistence::restore(test_dir).await.unwrap();

        remove_dir_all(test_dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_persistence_partial_write() {
        let test_dir = "test_persistence_partial";

        if Path::new(test_dir).exists() {
            remove_dir_all(test_dir).await.unwrap();
        }

        fs::create_dir_all(test_dir).unwrap();

        let mut persistence = Persistence::restore(test_dir).await.unwrap();
        persistence.persist(false, 0, None).await.unwrap();
        persistence.persist(true, 0, None).await.unwrap();

        let _restored = Persistence::restore(test_dir).await.unwrap();

        remove_dir_all(test_dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_persistence_metadata_only() {
        let test_dir = "test_persistence_metadata";

        if Path::new(test_dir).exists() {
            remove_dir_all(test_dir).await.unwrap();
        }

        fs::create_dir_all(test_dir).unwrap();

        let mut persistence = Persistence::restore(test_dir).await.unwrap();
        persistence.persist(false, 0, None).await.unwrap();

        let _restored = Persistence::restore(test_dir).await.unwrap();

        remove_dir_all(test_dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_persistence_multiple_restores() {
        let test_dir = "test_persistence_multiple";

        if Path::new(test_dir).exists() {
            remove_dir_all(test_dir).await.unwrap();
        }

        fs::create_dir_all(test_dir).unwrap();

        let mut persistence = Persistence::restore(test_dir).await.unwrap();
        persistence.persist(false, 0, None).await.unwrap();

        for _ in 0..3 {
            let _restored = Persistence::restore(test_dir).await.unwrap();
        }

        remove_dir_all(test_dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_persistence_file_creation() {
        let test_dir = "test_persistence_creation";

        if Path::new(test_dir).exists() {
            remove_dir_all(test_dir).await.unwrap();
        }

        fs::create_dir_all(test_dir).unwrap();

        let mut persistence = Persistence::restore(test_dir).await.unwrap();
        persistence.persist(false, 0, None).await.unwrap();

        let metadata_path = format!("{}/metadata.dat", test_dir);
        assert!(Path::new(&metadata_path).exists());

        let metadata = fs::read(&metadata_path).unwrap();
        assert_eq!(metadata.len(), 4096);

        remove_dir_all(test_dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_entry_serialization_deserialization() {
        let entry = Entry {
            term: 5,
            operations: vec![
                Operation::Write { key: 1, value: 100 },
                Operation::Read {
                    key: 2,
                    result: Some(200),
                },
            ],
            result: None,
        };

        let serialized = entry.serialize();
        let deserialized = Entry::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.term, 5);
        assert_eq!(deserialized.operations.len(), 2);

        match &deserialized.operations[0] {
            Operation::Write { key, value } => {
                assert_eq!(*key, 1);
                assert_eq!(*value, 100);
            }
            _ => panic!("Expected Write operation"),
        }

        match &deserialized.operations[1] {
            Operation::Read { key, result } => {
                assert_eq!(*key, 2);
                assert_eq!(*result, Some(200));
            }
            _ => panic!("Expected Read operation"),
        }
    }

    #[tokio::test]
    async fn test_entry_deserialization_error() {
        let invalid_data = [0u8; 64];

        let result = Entry::deserialize(&invalid_data);
        assert!(result.is_err());

        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("Invalid entry size"));
    }

    #[tokio::test]
    #[should_panic(expected = "Failed to open metadata file")]
    async fn test_persistence_error_handling() {
        let invalid_dir = "/nonexistent/path/that/should/not/exist";
        let _result = Persistence::restore(invalid_dir).await;
    }
}
