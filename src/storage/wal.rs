use std::{
    fs::{OpenOptions, File},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};
use serde::{Serialize, Deserialize};
use anyhow::Result;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WalRecord {
    Upsert {
        collection: String,
        id: String,
        vector: Vec<f32>,
        payload_json: String,
        ts_ms: i64,
    },
    CreateCollection {
        name: String,
        dim: u32,
        metric: String,
        ts_ms: i64,
    }
}

pub struct Wal {
    path: PathBuf,
}

impl Wal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        OpenOptions::new().create(true).append(true).open(&path)?; // ensure exists
        Ok(Self { path })
    }

    pub fn append(&self, rec: &WalRecord) -> Result<()> {
        let mut f = OpenOptions::new().append(true).open(&self.path)?;
        let line = serde_json::to_string(rec)?;
        f.write_all(line.as_bytes())?;
        f.write_all(b"\n")?;
        f.flush()?;
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<WalRecord>> {
        let f = File::open(&self.path)?;
        let reader = BufReader::new(f);
        let mut out = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            let rec: WalRecord = serde_json::from_str(&line)?;
            out.push(rec);
        }
        Ok(out)
    }
}
