//! Table des jobs terminés. Deux implémentations à l'API identique, choisies à la compilation :
//! `HashMap` standard (défaut) ou table dense à adressage ouvert (feature `dense-done`).

use crate::shard::{Done, Id};

/// Instrumentation de la table dense (zéros pour `HashMap`, qui ne l'expose pas).
#[derive(Debug, Default, Clone, Copy)]
pub struct TableStats {
    pub rehashes: u64,
    pub rehash_max_us: u64,
    pub rehash_total_us: u64,
    pub max_probe: u32,
}

#[cfg(not(feature = "dense-done"))]
pub(crate) use std_table::DoneTable;
#[cfg(feature = "dense-done")]
pub(crate) use dense::DoneTable;

#[cfg(not(feature = "dense-done"))]
mod std_table {
    use super::*;
    use std::collections::HashMap;

    #[derive(Default)]
    pub(crate) struct DoneTable(HashMap<Id, Done>);

    impl DoneTable {
        pub fn len(&self) -> usize {
            self.0.len()
        }
        pub fn capacity(&self) -> usize {
            self.0.capacity()
        }
        /// Octets réservés par la table (hashbrown : un octet de contrôle par emplacement).
        pub fn table_bytes(&self) -> usize {
            self.0.capacity() * (std::mem::size_of::<(Id, Done)>() + 1)
        }
        pub fn get(&self, id: &Id) -> Option<&Done> {
            self.0.get(id)
        }
        pub fn contains_key(&self, id: &Id) -> bool {
            self.0.contains_key(id)
        }
        pub fn insert(&mut self, id: Id, d: Done) {
            self.0.insert(id, d);
        }
        pub fn remove(&mut self, id: &Id) -> Option<Done> {
            self.0.remove(id)
        }
        pub fn iter(&self) -> impl Iterator<Item = (Id, &Done)> {
            self.0.iter().map(|(k, v)| (*k, v))
        }
        pub fn shrink_to(&mut self, n: usize) {
            self.0.shrink_to(n)
        }
        pub fn stats(&self) -> TableStats {
            TableStats::default()
        }
    }
}

#[cfg(feature = "dense-done")]
mod dense {
    use super::*;

    /// Plafond de remplissage (3/4) et croissance ×1,25 : remplissage entre 60 et 75 % (hashbrown : 44–87 %, moyenne ≈ 54 % observée).
    const MAX_LOAD_NUM: usize = 3;
    const MAX_LOAD_DEN: usize = 4;
    const MIN_SLOTS: usize = 64;

    #[derive(Clone, Copy)]
    struct Slot {
        key: Id,
        val: Done,
    }

    const EMPTY: Slot = Slot { key: Id([0, 0]), val: Done::EMPTY };

    /// Adressage ouvert, sondage linéaire, suppression par décalage (pas de pierres tombales).
    /// La clé nulle marque un emplacement libre : l'identifiant 0 vit dans `zero`.
    pub(crate) struct DoneTable {
        slots: Vec<Slot>,
        len: usize,
        zero: Option<Done>,
        stats: TableStats,
    }

    impl Default for DoneTable {
        fn default() -> Self {
            DoneTable { slots: Vec::new(), len: 0, zero: None, stats: TableStats::default() }
        }
    }

    fn is_zero(id: &Id) -> bool {
        id.0 == [0, 0]
    }

    fn hash(id: &Id) -> u64 {
        let mut h = id.0[0] ^ id.0[1].rotate_left(32);
        h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^ (h >> 31)
    }

    /// Réduction de plage sans modulo : monotone en `h`, donc compatible avec le décalage à la suppression.
    fn home(id: &Id, n: usize) -> usize {
        ((hash(id) as u128 * n as u128) >> 64) as usize
    }

    impl DoneTable {
        pub fn len(&self) -> usize {
            self.len + self.zero.is_some() as usize
        }
        pub fn capacity(&self) -> usize {
            self.slots.len() * MAX_LOAD_NUM / MAX_LOAD_DEN
        }
        pub fn table_bytes(&self) -> usize {
            self.slots.capacity() * std::mem::size_of::<Slot>()
        }

        fn find(&self, id: &Id) -> Option<usize> {
            let n = self.slots.len();
            if n == 0 {
                return None;
            }
            let mut i = home(id, n);
            loop {
                let s = &self.slots[i];
                if s.key == *id {
                    return Some(i);
                }
                if is_zero(&s.key) {
                    return None;
                }
                i += 1;
                if i == n {
                    i = 0;
                }
            }
        }

        pub fn get(&self, id: &Id) -> Option<&Done> {
            if is_zero(id) {
                return self.zero.as_ref();
            }
            self.find(id).map(|i| &self.slots[i].val)
        }

        pub fn contains_key(&self, id: &Id) -> bool {
            self.get(id).is_some()
        }

        pub fn stats(&self) -> TableStats {
            self.stats
        }

        fn rebuild(&mut self, n: usize) {
            let started = std::time::Instant::now();
            let old = std::mem::replace(&mut self.slots, vec![EMPTY; n]);
            for s in old.into_iter().filter(|s| !is_zero(&s.key)) {
                self.place(s);
            }
            let us = started.elapsed().as_micros() as u64;
            self.stats.rehashes += 1;
            self.stats.rehash_total_us += us;
            self.stats.rehash_max_us = self.stats.rehash_max_us.max(us);
        }

        fn place(&mut self, s: Slot) {
            let n = self.slots.len();
            let mut i = home(&s.key, n);
            let mut probe = 0u32;
            while !is_zero(&self.slots[i].key) {
                i += 1;
                probe += 1;
                if i == n {
                    i = 0;
                }
            }
            self.stats.max_probe = self.stats.max_probe.max(probe);
            self.slots[i] = s;
        }

        pub fn insert(&mut self, id: Id, d: Done) {
            if is_zero(&id) {
                self.zero = Some(d);
                return;
            }
            if let Some(i) = self.find(&id) {
                self.slots[i].val = d;
                return;
            }
            if (self.len + 1) * MAX_LOAD_DEN > self.slots.len() * MAX_LOAD_NUM {
                let n = (self.slots.len() + self.slots.len() / 4).max(MIN_SLOTS);
                self.rebuild(n);
            }
            self.place(Slot { key: id, val: d });
            self.len += 1;
        }

        pub fn remove(&mut self, id: &Id) -> Option<Done> {
            if is_zero(id) {
                return self.zero.take();
            }
            let mut i = self.find(id)?;
            let removed = self.slots[i].val;
            let n = self.slots.len();
            let mut j = i;
            loop {
                j += 1;
                if j == n {
                    j = 0;
                }
                if is_zero(&self.slots[j].key) {
                    break;
                }
                let k = home(&self.slots[j].key, n);
                // L'entrée en `j` peut combler `i` si sa position d'origine `k` n'est pas dans ]i, j] (circulaire).
                let movable = if i <= j { k <= i || k > j } else { k <= i && k > j };
                if movable {
                    self.slots[i] = self.slots[j];
                    i = j;
                }
            }
            self.slots[i] = EMPTY;
            self.len -= 1;
            Some(removed)
        }

        pub fn iter(&self) -> impl Iterator<Item = (Id, &Done)> {
            self.slots
                .iter()
                .filter(|s| !is_zero(&s.key))
                .map(|s| (s.key, &s.val))
                .chain(self.zero.as_ref().map(|d| (Id([0, 0]), d)))
        }

        pub fn shrink_to(&mut self, n: usize) {
            let want = (n * MAX_LOAD_DEN / MAX_LOAD_NUM + 1).max(MIN_SLOTS);
            if want < self.slots.len() && self.len < want * MAX_LOAD_NUM / MAX_LOAD_DEN {
                self.rebuild(want);
                self.slots.shrink_to_fit();
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::collections::HashMap;

        fn d(n: u64) -> Done {
            Done { fencing_token: n, ..Done::EMPTY }
        }

        /// Comparaison avec `HashMap` sur une séquence pseudo-aléatoire d'insertions et de suppressions,
        /// avec des clés regroupées pour provoquer de longues grappes et des enroulements.
        #[test]
        fn matches_std_hashmap_under_churn() {
            let mut t = DoneTable::default();
            let mut m: HashMap<Id, u64> = HashMap::new();
            let mut x = 12345u64;
            for step in 0..200_000u64 {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let k = Id([(x >> 33) % 3000, 0]);
                if (x >> 20) & 3 == 0 {
                    assert_eq!(t.remove(&k).map(|v| v.fencing_token), m.remove(&k));
                } else {
                    t.insert(k, d(step));
                    m.insert(k, step);
                }
                if step % 5000 == 0 {
                    assert_eq!(t.len(), m.len());
                    assert_eq!(t.iter().count(), m.len());
                }
            }
            for (k, v) in &m {
                assert_eq!(t.get(k).unwrap().fencing_token, *v);
            }
            assert_eq!(t.len(), m.len());
        }

        /// Remplissage au seuil (3/4) : toute entrée reste trouvable, y compris après suppressions en grappe.
        #[test]
        fn probe_invariant_near_max_load() {
            let mut t = DoneTable::default();
            let mut ids = Vec::new();
            let mut i = 1u64;
            while t.slots.len() < 4096 || t.len < t.slots.len() * 3 / 4 - 1 {
                let k = Id([i.wrapping_mul(0x9E37_79B9_7F4A_7C15), 0]);
                t.insert(k, d(i));
                ids.push((k, i));
                i += 1;
            }
            for (n, (k, v)) in ids.iter().enumerate() {
                if n % 3 == 0 {
                    assert_eq!(t.remove(k).unwrap().fencing_token, *v);
                }
            }
            for (n, (k, v)) in ids.iter().enumerate() {
                assert_eq!(t.get(k).map(|x| x.fencing_token), (n % 3 != 0).then_some(*v));
            }
        }

        #[test]
        fn zero_id_and_shrink() {
            let mut t = DoneTable::default();
            t.insert(Id([0, 0]), d(7));
            for i in 1..10_000u64 {
                t.insert(Id([i, i]), d(i));
            }
            assert_eq!(t.get(&Id([0, 0])).unwrap().fencing_token, 7);
            for i in 1..10_000u64 {
                t.remove(&Id([i, i])).unwrap();
            }
            t.shrink_to(0);
            assert_eq!(t.len(), 1);
            assert!(t.slots.len() <= 128);
            assert_eq!(t.remove(&Id([0, 0])).unwrap().fencing_token, 7);
            assert_eq!(t.len(), 0);
        }
    }
}

#[cfg(test)]
mod latency {
    use super::*;
    use std::time::Instant;

    fn d(n: u64) -> Done {
        Done { fencing_token: n, ..Done::EMPTY }
    }

    /// À lancer à la main : `cargo test --release -p solid-engine-core latency -- --ignored --nocapture`
    /// (avec ou sans `--features dense-done`). Fenêtre glissante de 250 000 entrées (un shard à 50 k jobs/s, rétention 5 s).
    #[test]
    #[ignore]
    fn insert_remove_latency_report() {
        const TOTAL: u64 = 2_000_000;
        const WINDOW: u64 = 250_000;
        let mut t = DoneTable::default();
        let mut ins = Vec::with_capacity(TOTAL as usize);
        let mut rem = Vec::with_capacity(TOTAL as usize);
        let key = |i: u64| Id([i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1, i >> 7]);
        for i in 0..TOTAL {
            let s = Instant::now();
            t.insert(key(i), d(i));
            ins.push(s.elapsed().as_nanos() as u64);
            if i >= WINDOW {
                let s = Instant::now();
                assert!(t.remove(&key(i - WINDOW)).is_some());
                rem.push(s.elapsed().as_nanos() as u64);
            }
        }
        assert_eq!(t.len() as u64, WINDOW);
        let q = |v: &mut Vec<u64>, p: f64| {
            v.sort_unstable();
            v[((v.len() as f64 * p) as usize).min(v.len() - 1)]
        };
        for (name, v) in [("insert", &mut ins), ("remove", &mut rem)] {
            println!("{name} ns: p50={} p99={} p99.9={} p99.99={} max={}", q(v, 0.5), q(v, 0.99), q(v, 0.999), q(v, 0.9999), v.last().unwrap());
        }
        println!("table_bytes={} capacity={} len={} stats={:?}", t.table_bytes(), t.capacity(), t.len(), t.stats());
    }
}
