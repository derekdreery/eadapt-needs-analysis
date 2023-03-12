use qu::ick_use::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    sync::Arc,
};

use crate::{
    read2::{CodeSet, ReadCode, TermCodeSet, TermSet},
    ArcStr, Table,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
/// All data from the Read v2 database loaded into memory.
pub struct Thesaurus {
    pub codes: Arc<BTreeMap<ReadCode, BTreeSet<ArcStr>>>,
}

impl Thesaurus {
    /// Load this table of Read codes from the readbrowser database files.
    ///
    /// Parameter is the root path of the readbrowser files.
    pub fn load() -> Result<Self> {
        fn inner() -> Result<Thesaurus> {
            let input = io::BufReader::new(fs::File::open("../data/read_db/all.bin")?);
            bincode::deserialize_from(input).map_err(Into::into)
        }
        inner().context("loading thesaurus from \"../data/read_db/all.bin\"")
    }

    /// Helper to show some records from the Read browser. Mostly there to check it's loaded
    /// correctly.
    pub fn evcxr_display(&self) {
        Table::new(self.codes.iter(), |&(code, description), _| {
            (code, format!("{:?}", description))
        })
        .with_headers(["code", "description"])
        .evcxr_display();
    }

    pub fn term_table(&self) -> term_data_table::Table {
        use term_data_table::{Cell, Row, Table};
        let mut table = Table::new();
        for (code, desc) in self.iter() {
            table.add_row(
                Row::new()
                    .with_cell(Cell::from(code.to_string()))
                    .with_cell(Cell::from(format!("{:?}", desc))),
            );
        }
        table
    }

    /// Get the description for a read code.
    pub fn get(&self, code: ReadCode) -> Option<&BTreeSet<ArcStr>> {
        self.codes.get(&code)
    }

    /// Filter the read codes
    ///
    /// First the list is whitelisted against includes, then blacklisted against excludes.
    /// Both parameters are interpreted as regexes.
    pub fn filter<'any>(&self, term_set: TermSet) -> TermCodeSet {
        let code_set = CodeSet::from_iter(term_set.filter(self.iter()).map(|(code, _)| code));
        TermCodeSet::new(code_set, term_set, self.clone())
    }

    /// An iterator over (code, description) pairs
    pub fn iter(&self) -> impl Iterator<Item = (ReadCode, &BTreeSet<ArcStr>)> + '_ {
        self.codes.iter().map(|(code, set)| (*code, set))
    }

    /// An iterator over (code, description) pairs
    pub fn iter_cloned(&self) -> impl Iterator<Item = (ReadCode, BTreeSet<ArcStr>)> + '_ {
        self.iter().map(|(k, v)| (k, (*v).clone()))
    }

    /// Iterate over the descendants of a Read code
    // this function relies on the `Ord` implementation on `ReadCode`, specifically the fact that
    // `.` comes before alphanumeric, and the fact that we store read codes in an ordered
    // collection (a b-tree).
    pub fn iter_descendants(
        &self,
        parent: ReadCode,
    ) -> impl Iterator<Item = (ReadCode, &BTreeSet<ArcStr>)> + '_ {
        self.codes
            .range(parent..)
            // skip the parent
            .skip(1)
            .take_while(move |(code, _)| parent.is_parent_of(**code))
            .map(|(code, set)| (*code, set))
    }
}

impl<'a> IntoParallelIterator for &'a Thesaurus {
    type Item = (&'a ReadCode, &'a BTreeSet<ArcStr>);
    type Iter = rayon::collections::btree_map::Iter<'a, ReadCode, BTreeSet<Arc<str>>>;
    fn into_par_iter(self) -> Self::Iter {
        (&*self.codes).into_par_iter()
    }
}
