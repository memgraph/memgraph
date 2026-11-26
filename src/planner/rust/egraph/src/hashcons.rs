//! The hashcons: which e-node ids are registered under which hash.
//!
//! Contract, shared by both implementations. The table holds entries
//! `(hash, id)`, each added by `insert` and dropped by `remove`.
//!
//!  - `find(hash, eq)` answers an id from an entry stored under `hash` for
//!    which `eq(id)` holds, and `None` exactly when there is no such entry.
//!  - `insert(hash, id)` adds the entry `(hash, id)`. The caller guarantees no
//!    stored entry under `hash` is `eq` to the new one, so `find` never has two
//!    candidates to choose between.
//!  - `remove(hash, id)` drops the entry `(hash, id)` and answers whether one
//!    was stored.
//!
//! The caller's obligation for this to mean anything: an id's stored hash is
//! the hash of its e-node's current content, so an e-node is never mutated
//! while it is registered. The e-graph removes an e-node before
//! re-canonicalising it and inserts it again afterwards.
//!
//! `VecHashCons` is the specification: a list searched front to back.
//! `TableHashCons` is the one the e-graph uses, built on `hashbrown`. Proofs
//! use the specification, under the trusted assumption that `hashbrown`
//! honours the contract; the two are held to each other by a property test.

use hashbrown::HashTable;

use crate::Id;

#[derive(Default, Clone, Debug)]
pub struct VecHashCons {
    entries: Vec<(u64, Id)>,
}

impl VecHashCons {
    pub fn find(&self, hash: u64, mut eq: impl FnMut(Id) -> bool) -> Option<Id> {
        self.entries.iter().find(|&&(h, id)| h == hash && eq(id)).map(|&(_, id)| id)
    }

    pub fn insert(&mut self, hash: u64, id: Id, _rehash: impl Fn(Id) -> u64) {
        self.entries.push((hash, id));
    }

    pub fn remove(&mut self, hash: u64, id: Id) -> bool {
        match self.entries.iter().position(|&entry| entry == (hash, id)) {
            Some(index) => {
                self.entries.swap_remove(index);
                true
            }
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn reserve(&mut self, additional: usize, _rehash: impl Fn(Id) -> u64) {
        self.entries.reserve(additional);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[derive(Default, Clone, Debug)]
pub struct TableHashCons {
    table: HashTable<Id>,
}

impl TableHashCons {
    pub fn find(&self, hash: u64, mut eq: impl FnMut(Id) -> bool) -> Option<Id> {
        self.table.find(hash, |&id| eq(id)).copied()
    }

    /// `rehash` recovers a stored entry's hash when the table grows.
    pub fn insert(&mut self, hash: u64, id: Id, rehash: impl Fn(Id) -> u64) {
        self.table.insert_unique(hash, id, |&other| rehash(other));
    }

    pub fn remove(&mut self, hash: u64, id: Id) -> bool {
        match self.table.find_entry(hash, |&stored| stored == id) {
            Ok(entry) => {
                entry.remove();
                true
            }
            Err(_) => false,
        }
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn reserve(&mut self, additional: usize, rehash: impl Fn(Id) -> u64) {
        self.table.reserve(additional, |&id| rehash(id));
    }

    pub fn clear(&mut self) {
        self.table.clear();
    }
}

#[cfg(not(kani))]
pub type HashCons = TableHashCons;

#[cfg(kani)]
pub type HashCons = VecHashCons;

#[cfg(test)]
mod table_honours_the_specification {
    use super::*;
    use proptest::prelude::*;

    /// Each id stands for content `key[id]`; `eq` compares contents, the way
    /// the e-graph compares e-nodes. Hashes are drawn from a tiny range so
    /// that distinct contents share hashes and probe sequences collide.
    #[derive(Debug, Clone)]
    enum Op {
        Insert { key: u8 },
        Remove { pick: u16 },
        Find { key: u8 },
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (0u8..12).prop_map(|key| Op::Insert { key }),
            2 => any::<u16>().prop_map(|pick| Op::Remove { pick }),
            3 => (0u8..12).prop_map(|key| Op::Find { key }),
        ]
    }

    fn hash_of(key: u8) -> u64 {
        u64::from(key % 3)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(3000))]

        #[test]
        fn same_answers_on_every_operation(ops in prop::collection::vec(op(), 0..200)) {
            let mut spec = VecHashCons::default();
            let mut table = TableHashCons::default();
            let mut key_of: Vec<u8> = Vec::new();
            let mut registered: Vec<Id> = Vec::new();
            for op in ops {
                match op {
                    Op::Insert { key } => {
                        let eq = |id: Id| key_of[id.index()] == key;
                        // The caller's guarantee: nothing equal is registered.
                        if spec.find(hash_of(key), eq).is_some() {
                            continue;
                        }
                        let id = Id(key_of.len() as u32);
                        key_of.push(key);
                        let rehash = |id: Id| hash_of(key_of[id.index()]);
                        spec.insert(hash_of(key), id, rehash);
                        table.insert(hash_of(key), id, rehash);
                        registered.push(id);
                    }
                    Op::Remove { pick } => {
                        if registered.is_empty() {
                            continue;
                        }
                        let id = registered.swap_remove(pick as usize % registered.len());
                        let hash = hash_of(key_of[id.index()]);
                        prop_assert_eq!(spec.remove(hash, id), table.remove(hash, id));
                        prop_assert!(!spec.remove(hash, id), "removed twice");
                        prop_assert!(!table.remove(hash, id), "removed twice");
                    }
                    Op::Find { key } => {
                        let eq = |id: Id| key_of[id.index()] == key;
                        prop_assert_eq!(spec.find(hash_of(key), eq), table.find(hash_of(key), eq));
                    }
                }
                prop_assert_eq!(spec.len(), table.len());
            }
        }
    }
}
