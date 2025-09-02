// src/pb.rs

// Define the same module tree the generator expects,
// but point the include at an absolute path so it works from anywhere.
pub mod vectordb {
    pub mod v1 {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/pbgen/vectordb.v1.rs"));
    }
}
