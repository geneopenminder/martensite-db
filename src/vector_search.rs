// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Evgeniy Pshenitsin (geneopenminder) <geneopenminder@gmail.com>

use faiss::index::io::{read_index, write_index};
use faiss::{Index, MetricType, index::IndexImpl, index_factory};

pub fn create_idx() {}
/*
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let index_file = "index.faiss";

    let mut index = if std::path::Path::new(index_file).exists() {

        let idx = read_index(index_file);

    } else {
        let mut idx = index_factory(8, "Flat", MetricType::L2)?;
        let v1 = vec![1.0, 0.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0, 0.0];
        let v3 = vec![0.0, 0.0, 1.0, 0.0];
        idx.add(&v1)?;
        idx.add(&v2)?;
        idx.add(&v3)?;

        write_index(&idx, index_file);

        //idx
    };

    let query = vec![0.9, 0.1, 0.0, 0.0];

    let (labels, distances) = idx.search(&query, 2)?;

    for (i, (label, dist)) in labels[0].iter().zip(distances[0].iter()).enumerate() {
        println!("  {}. id = {}, distance = {}", i + 1, label, dist);
    }

    Ok(())
}
        */

// RUSTFLAGS="-Awarnings" cargo test test_create_table_struct -- --nocapture
#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use faiss::{IdMap, Idx, Index, MetricType, index_factory};

    //#[test]
    fn test_search() {
        let mut index = index_factory(8, "Flat", MetricType::InnerProduct).unwrap();
        let mut index = IdMap::new(index).unwrap();
        let some_data = &[
            0.28410476, 0.7937578, 0.08255077, 0.12863483, 0.12645306, 0.4247582, 0.4965156,
            0.87145853,
        ];
        index.add_with_ids(some_data, &[Idx::new(10)]).unwrap();
        let some_data = &[
            0.33278704,
            0.77352536,
            20.36933973,
            0.08000911,
            0.3248043,
            0.13767178,
            0.42928705,
            0.7664685,
        ];
        index.add_with_ids(some_data, &[Idx::new(11)]).unwrap();
        let some_data = &[
            0.6395982, 0.25682494, 0.48980752, 0.35527703, 0.78299326, 0.448646, 0.23446749,
            0.29709548,
        ];
        index.add_with_ids(some_data, &[Idx::new(12)]).unwrap();
        let some_data = &[
            0.44823584,
            0.6476691,
            0.7459561,
            0.009480214,
            0.6156617,
            0.7647773,
            0.4922104,
            0.7113472,
        ];
        index.add_with_ids(some_data, &[Idx::new(13)]).unwrap();

        //res Ok(SearchResult { distances: [2.8698092, 2.7454114, 2.4767473, 1.4922005, -3.4028235e38], labels: [Idx(3), Idx(0), Idx(1), Idx(2), Idx(-1)] })
        let my_query = [0.1, 0.15, 0.3, 0.12, 0.05, 0.1, 0.4, 0.6];
        let result = index.search(&my_query, 4).unwrap();

        println!("result {:?}", result);
    }
}
