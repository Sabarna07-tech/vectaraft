use std::collections::HashMap;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::index::flat::FlatIndex;
use crate::types::Metric;
use rayon::prelude::*;
use serde_json::Value;

#[derive(Clone)]
pub struct Collection {
    pub name: String,
    pub dim: usize,
    pub metric: Metric,
    pub index: FlatIndex, // v1: flat index only
}

impl Collection {
    pub fn new(name: String, dim: usize, metric: Metric) -> Self {
        Self {
            name: name.clone(),
            dim,
            metric,
            index: FlatIndex::new(dim, metric),
        }
    }

    pub fn validate_dim(&self, vector: &[f32]) -> bool {
        vector.len() == self.dim
    }

    pub fn upsert_batch(
        &mut self,
        ids: Vec<String>,
        vectors: Vec<Vec<f32>>,
        payloads: Vec<String>,
    ) -> usize {
        let count = vectors.len();
        if count == 0 {
            return 0;
        }
        self.index.add_batch(ids, vectors, payloads);
        count
    }

    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        metric_override: Option<Metric>,
        filters: Option<&[(String, String)]>,
    ) -> Vec<(String, f32, String)> {
        let metric = metric_override.unwrap_or(self.metric);
        let dim = self.index.dim;
        let filters = filters.unwrap_or(&[]);

        let mut scored: Vec<(usize, f32)> = (0..self.index.len())
            .into_par_iter()
            .filter_map(|idx| {
                if !filters.is_empty() {
                    let payload = self.index.payloads.get(idx)?.as_str();
                    if !payload_matches_filters(payload, filters) {
                        return None;
                    }
                }

                let offset = idx * dim;
                let vector = &self.index.vectors[offset..offset + dim];
                let score = match metric {
                    Metric::L2 => -query
                        .iter()
                        .zip(vector)
                        .map(|(a, b)| {
                            let d = a - b;
                            d * d
                        })
                        .sum::<f32>(),
                    Metric::IP => query.iter().zip(vector).map(|(a, b)| a * b).sum(),
                    Metric::Cosine => {
                        let dot: f32 = query.iter().zip(vector).map(|(a, b)| a * b).sum();
                        let nq = query.iter().map(|x| x * x).sum::<f32>().sqrt();
                        let nv = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
                        if nq == 0.0 || nv == 0.0 { 0.0 } else { dot / (nq * nv) }
                    }
                };
                Some((idx, score))
            })
            .collect();

        if scored.is_empty() || top_k == 0 {
            return Vec::new();
        }

        let k = top_k.min(scored.len());
        scored.select_nth_unstable_by(k - 1, |a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        scored
            .into_iter()
            .map(|(idx, score)| {
                let id = self.index.ids.get(idx).cloned().unwrap_or_default();
                let payload = self.index.payloads.get(idx).cloned().unwrap_or_default();
                (id, score, payload)
            })
            .collect()
    }
}

pub struct PointWrite {
    pub id: String,
    pub vector: Vec<f32>,
    pub payload_json: String,
}

#[derive(Clone, Default)]
pub struct Catalog {
    inner: Arc<RwLock<HashMap<String, Collection>>>,
}

impl Catalog {
    pub fn create_collection(&self, name: String, dim: usize, metric: Metric) -> bool {
        let mut g = self.inner.write();
        if g.contains_key(&name) {
            return false;
        }
        g.insert(name.clone(), Collection::new(name, dim, metric));
        true
    }

    pub fn get(&self, name: &str) -> Option<CollectionHandle> {
        if self.inner.read().contains_key(name) {
            Some(CollectionHandle { name: name.to_string(), cat: self.clone() })
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub struct CollectionHandle {
    name: String,
    cat: Catalog,
}

impl CollectionHandle {
    pub fn upsert_points(&self, points: Vec<PointWrite>) -> Option<usize> {
        if points.is_empty() {
            return Some(0);
        }
        let dims_ok = self
            .with_ref(|coll| points.iter().all(|p| coll.validate_dim(&p.vector)))
            .unwrap_or(false);
        if !dims_ok {
            return None;
        }
        self.with_mut(|coll| {
            let ids: Vec<String> = points.iter().map(|p| p.id.clone()).collect();
            let payloads: Vec<String> = points.iter().map(|p| p.payload_json.clone()).collect();
            let vectors: Vec<Vec<f32>> = points.into_iter().map(|p| p.vector).collect();
            coll.upsert_batch(ids, vectors, payloads)
        })
    }

    pub fn search(
        &self,
        query: Vec<f32>,
        top_k: usize,
        metric_override: Option<Metric>,
        filters: Vec<(String, String)>,
    ) -> Option<Vec<(String, f32, String)>> {
        if query.is_empty() {
            return Some(vec![]);
        }
        let dim_ok = self
            .with_ref(|coll| coll.validate_dim(&query))
            .unwrap_or(false);
        if !dim_ok {
            return None;
        }
        let filters_opt: Option<&[(String, String)]> = if filters.is_empty() {
            None
        } else {
            Some(filters.as_slice())
        };
        self.with_ref(|coll| coll.search(&query, top_k, metric_override, filters_opt))
    }

    pub fn with_mut<F, T>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&mut Collection) -> T
    {
        let mut g = self.cat.inner.write();
        let coll = g.get_mut(&self.name)?;
        Some(f(coll))
    }

    pub fn with_ref<F, T>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&Collection) -> T
    {
        let g = self.cat.inner.read();
        let coll = g.get(&self.name)?;
        Some(f(coll))
    }
}

fn payload_matches_filters(payload: &str, filters: &[(String, String)]) -> bool {
    if filters.is_empty() {
        return true;
    }
    let Ok(Value::Object(map)) = serde_json::from_str::<Value>(payload) else { return false; };
    filters.iter().all(|(key, expected)| {
        map.get(key).map_or(false, |value| match value {
            Value::String(s) => s == expected,
            Value::Number(n) => n.to_string() == *expected,
            Value::Bool(b) => b.to_string() == *expected,
            _ => false,
        })
    })
}
