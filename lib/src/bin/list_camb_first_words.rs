//! Little helper to get the first word of a cambridge csv.
use clap::Parser;
use qu::ick_use::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

#[derive(Debug, Parser)]
struct Opt {
    path: PathBuf,
    #[clap(long, short)]
    for_meta: bool,
}

#[qu::ick]
fn main(opt: Opt) -> Result {
    let mut map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for record in csv::Reader::from_path(&opt.path)?.into_records() {
        let record = record?;
        let mut record = record.get(2).unwrap().splitn(2, ' ');
        let word1 = record.next().unwrap();
        let entry = map.entry(word1.to_lowercase()).or_insert(BTreeSet::new());
        if let Some(word) = record.next() {
            entry.insert(word.to_lowercase());
        }
    }

    if opt.for_meta {
        for word in map.keys() {
            println!("{:?},", word);
        }
    } else {
        for (word, rest) in map {
            println!("{}", word);
            for word in rest {
                println!("    {}", word);
            }
        }
    }
    Ok(())
}
