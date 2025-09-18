use std::cmp::Ordering;
use rayon::prelude::*;

#[derive(Clone)]
pub struct FlatIndex {
    pub dim: usize,
    // Layout: [v0...vdim-1, v1...vdim-1, ...]
    pub vectors: Vec<f32>,
    pub ids: Vec<String>,
    pub payloads: Vec<String>, // JSON strings
    pub metric: crate::types::Metric,
}

impl FlatIndex {
    pub fn new(dim: usize, metric: crate::types::Metric) -> Self {
        Self { dim, vectors: Vec::new(), ids: Vec::new(), payloads: Vec::new(), metric }
    }

    pub fn len(&self) -> usize { self.ids.len() }

    pub fn add_batch(&mut self, ids: Vec<String>, vecs: Vec<Vec<f32>>, payloads: Vec<String>) {
        assert!(vecs.iter().all(|v| v.len() == self.dim), "all vectors must have dim={}", self.dim);
        for v in vecs.into_iter() { self.vectors.extend_from_slice(&v); }
        self.ids.extend(ids);
        self.payloads.extend(payloads);
    }

    fn l2(q: &[f32], v: &[f32]) -> f32 {
        let mut s = 0.0f32;
        for i in 0..q.len() {
            let d = q[i] - v[i];
            s += d * d;
        }
        // invert distance so higher=better similarity
        -s
    }

    fn dot(q: &[f32], v: &[f32]) -> f32 {
        let mut s = 0.0f32;
        for i in 0..q.len() { s += q[i] * v[i]; }
        s
    }

    fn cosine(q: &[f32], v: &[f32]) -> f32 {
        let dot = Self::dot(q, v);
        let nq = (q.iter().map(|x| x * x).sum::<f32>()).sqrt();
        let nv = (v.iter().map(|x| x * x).sum::<f32>()).sqrt();
        if nq == 0.0 || nv == 0.0 { 0.0 } else { dot / (nq * nv) }
    }

    pub fn search_topk(
        &self,
        query: &[f32],
        top_k: usize,
        metric_override: Option<crate::types::Metric>,
    ) -> Vec<(usize, f32)> {
        assert_eq!(query.len(), self.dim);
        if self.len() == 0 || top_k == 0 { return vec![]; }

        // Parallel scan
        let mut best: Vec<(usize, f32)> = (0..self.len()).into_par_iter().map(|i| {
            let off = i * self.dim;
            let v = &self.vectors[off..off + self.dim];
            let metric = metric_override.unwrap_or(self.metric);
            let score = match metric {
                crate::types::Metric::L2 => Self::l2(query, v),
                crate::types::Metric::IP => Self::dot(query, v),
                crate::types::Metric::Cosine => Self::cosine(query, v),
            };
            (i, score)
        }).collect();

        let k = top_k.min(best.len());
        if k > 0 {
            best.select_nth_unstable_by(k - 1, |a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
            best.truncate(k);
            best.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        }
        best
    }
}
