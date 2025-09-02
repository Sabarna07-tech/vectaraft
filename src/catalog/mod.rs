use std::collections::HashMap;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::index::flat::FlatIndex;
use crate::types::Metric;

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
}

#[derive(Clone, Default)]
pub struct Catalog {
    inner: Arc<RwLock<HashMap<String, Collection>>>,
}

impl Catalog {
    pub fn create_collection(&self, name: String, dim: usize, metric: Metric) {
        let mut g = self.inner.write();
        g.entry(name.clone()).or_insert_with(|| Collection::new(name, dim, metric));
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
