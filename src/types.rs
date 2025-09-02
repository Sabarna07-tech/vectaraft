use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Metric {
    L2,
    Cosine,
    IP,
}

impl Metric {
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "cosine" => Self::Cosine,
            "ip" | "inner_product" => Self::IP,
            _ => Self::L2,
        }
    }
}
