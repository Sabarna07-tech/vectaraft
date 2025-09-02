use std::{env, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let proto_dir = manifest_dir.join("proto");
    let out_dir = manifest_dir.join("src").join("pbgen");

    fs::create_dir_all(&out_dir)?;

    let proto = proto_dir.join("vector_db.proto");
    if !proto.exists() {
        panic!("Missing proto file: {}", proto.display());
    }

    println!("cargo:rerun-if-changed={}", proto.display());
    println!("cargo:rerun-if-changed={}", proto_dir.display());

    tonic_build::configure()
        .build_server(true)
        .include_file("mod.rs")
        .out_dir(&out_dir)
        .compile_protos(&[proto], &[proto_dir])?;

    Ok(())
}
