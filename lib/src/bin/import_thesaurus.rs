use eadapt_needs_analysis::read2::ReadCode;
use qu::ick_use::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    fs, io,
};

#[derive(Debug, Serialize, Deserialize)]
struct ReadImport {
    _term: String,
    _unknown: u8,
    description_short: String,
    description_med: Option<String>,
    description_long: Option<String>,
    _synonym: String,
    _lang: Language,
    code: ReadCode,
    _unknown2: (),
}

impl ReadImport {
    fn insert(self, th: &mut Thesaurus) {
        let entry = th.codes.entry(self.code).or_insert_with(HashSet::new);
        entry.insert(self.description_short);
        if let Some(med) = self.description_med {
            entry.insert(med);
        }
        if let Some(long) = self.description_long {
            entry.insert(long);
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Thesaurus {
    codes: BTreeMap<ReadCode, HashSet<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Language {
    #[serde(alias = "EN")]
    En,
}

#[qu::ick]
fn main() -> Result {
    let mut th = Thesaurus {
        codes: BTreeMap::new(),
    };

    let med_codes = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'|')
        .trim(csv::Trim::All)
        .from_path("../data/read_db/drugs.txt")?;
    for rec in med_codes.into_deserialize() {
        let rec: ReadImport = rec?;
        rec.insert(&mut th);
    }

    let nonmed_codes = csv::ReaderBuilder::new()
        .has_headers(false)
        .trim(csv::Trim::All)
        .from_path("../data/read_db/nondrugs.txt")?;
    for rec in nonmed_codes.into_deserialize() {
        let rec: ReadImport = rec?;
        rec.insert(&mut th);
    }

    let mut out = io::BufWriter::new(fs::File::create("../data/read_db/all.bin")?);
    bincode::serialize_into(&mut out, &th)?;
    Ok(())
}
