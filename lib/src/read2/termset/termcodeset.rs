use qu::ick_use::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_set, BTreeSet},
    iter,
    path::{Path, PathBuf},
};

use crate::{
    header,
    read2::{show_descriptions, CodeSet, ReadCode, TermSet, Thesaurus},
    termset_path, util, ArcStr, Table,
};

/// A termset with corresponding codeset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermCodeSet {
    /// The codes in this code set.
    pub code_set: CodeSet,
    /// The term set used to create this code set, if any.
    pub term_set: TermSet,
    /// A thesaurus
    th: Thesaurus,
}

impl TermCodeSet {
    /// Create a new collection of codes.
    pub fn new(code_set: CodeSet, term_set: TermSet, th: Thesaurus) -> Self {
        Self {
            code_set,
            term_set,
            th,
        }
    }

    pub fn add_include(&mut self, term: ArcStr) -> Result {
        self.term_set.add_include(term)?;
        self.code_set = self
            .term_set
            .filter(self.th.iter())
            .map(|(code, _)| code)
            .collect();
        Ok(())
    }

    pub fn add_exclude(&mut self, term: ArcStr) -> Result {
        self.term_set.add_exclude(term)?;
        self.code_set = self
            .term_set
            .filter(self.th.iter())
            .map(|(code, _)| code)
            .collect();
        Ok(())
    }

    pub fn save(&self, path: impl AsRef<Path>, overwrite: bool) -> Result {
        self.save_direct(termset_path(path.as_ref()), overwrite)
    }

    /// Save without transforming the path to the correct directory.
    fn save_direct(&self, path: PathBuf, overwrite: bool) -> Result {
        ensure!(
            overwrite || !util::path_exists(&path)?,
            "directory already exists"
        );

        self.term_set.save(&path, overwrite)?;
        self.code_set.save(&path.join("codes.txt"), overwrite)?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>, th: Thesaurus) -> Result<Self> {
        Self::load_direct(termset_path(path.as_ref()), th)
    }

    pub fn load_direct(path: PathBuf, th: Thesaurus) -> Result<Self> {
        let term_set = TermSet::load(&path)?;
        let code_set = CodeSet::load(&path.join("codes.txt"))?;
        Ok(Self {
            term_set,
            code_set,
            th,
        })
    }

    pub fn iter<'a>(
        &'a self,
    ) -> iter::Map<
        iter::Copied<btree_set::Iter<'a, ReadCode>>,
        impl FnMut(ReadCode) -> (ReadCode, &'a BTreeSet<ArcStr>),
    > {
        self.code_set
            .iter()
            .map(|code| (code, self.th.get(code).unwrap_or(&*util::EMPTY_DESC)))
    }

    /// Check to see if a code matches
    pub fn contains(&self, code: ReadCode) -> bool {
        self.code_set.contains(code)
    }

    /// Check to see if a description matches the term set.
    pub fn is_match(&self, desc: impl IntoIterator<Item = impl AsRef<str>>) -> bool {
        self.term_set.is_match_multi(desc)
    }

    /// Get all the child code/description pairs where the child isn't explicitly included or
    /// excluded.
    ///
    /// Term set authors should consider explicitaly excluding such codes.
    pub fn descendants_not_included_or_excluded(&self) -> CodeSet {
        let mut unmatched_descendants = BTreeSet::new();
        for parent in self.code_set.iter() {
            for (child, desc) in self.th.iter_descendants(parent) {
                if !desc.iter().any(|d| self.term_set.is_match_inc_or_ex(d)) {
                    unmatched_descendants.insert(child);
                }
            }
        }
        CodeSet::from(unmatched_descendants)
    }

    /// Checks that the included codes do actually match the termset
    pub fn check(&self) -> CheckReport {
        let mut report = CheckReport::new(self.th.clone());
        // codes that shouldn't match but did
        for code in self.code_set.iter() {
            match self.th.get(code) {
                Some(descs) => {
                    if !self.is_match(descs) {
                        report.extra.insert(code);
                    }
                }
                None => report.missing_codes.insert(code),
            };
        }
        // codes that should match but didn't
        for (code, descs) in self.th.iter() {
            if self.is_match(descs) {
                if !self.code_set.contains(code) {
                    report.missing.insert(code);
                }
            }
        }

        // unmatched descendantss
        report.unmatched_descendants = self.descendants_not_included_or_excluded();
        report
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

    pub fn evcxr_display(&self) {
        Table::new(self.iter(), |(code, description), _| {
            (*code, format!("{:?}", description))
        })
        .with_headers(["code", "description"])
        .evcxr_display();
    }
}

#[derive(Debug)]
pub struct CheckReport {
    /// Codes that we found but that don't match our query.
    pub extra: CodeSet,
    /// Codes that we didn't find but that do match our query.
    pub missing: CodeSet,
    /// Unmatched descendants.
    pub unmatched_descendants: CodeSet,
    /// Codes in the codeset were not present in the thesaurus
    pub missing_codes: CodeSet,
    th: Thesaurus,
}

impl CheckReport {
    fn new(th: Thesaurus) -> Self {
        Self {
            extra: CodeSet::default(),
            missing: CodeSet::default(),
            unmatched_descendants: CodeSet::default(),
            missing_codes: CodeSet::default(),
            th,
        }
    }

    pub fn print_term_tables(&self) {
        use term_data_table::{Cell, Row, Table};

        header("Missing codes");
        println!("Codes that should match our termset but aren't in the codeset");
        let mut table = Table::new().with_row(
            Row::new()
                .with_cell(Cell::from("Code"))
                .with_cell(Cell::from("Descriptions")),
        );
        for code in self.missing.iter() {
            table.add_row(
                Row::new()
                    .with_cell(Cell::from(code.to_string()))
                    .with_cell(Cell::from(show_descriptions(
                        self.th.get(code).expect("unreachable"),
                    ))),
            );
        }
        println!("{}", table.for_terminal());

        header("Unexpected codes");
        println!("Codes that shouldn't match our termset but are in the codeset");
        let mut table = Table::new().with_row(
            Row::new()
                .with_cell(Cell::from("Code"))
                .with_cell(Cell::from("Descriptions")),
        );
        for code in self.extra.iter() {
            table.add_row(
                Row::new()
                    .with_cell(Cell::from(code.to_string()))
                    .with_cell(Cell::from(show_descriptions(
                        self.th.get(code).expect("unreachable"),
                    ))),
            );
        }
        println!("{}", table.for_terminal());

        header("Codes missing from thesaurus");
        println!("Codes in the codeset that we couldn't find in our thesaurus");
        let mut table = Table::new().with_row(Row::new().with_cell(Cell::from("Code")));
        for code in self.extra.iter() {
            table.add_row(Row::new().with_cell(Cell::from(code.to_string())));
        }
        println!("{}", table.for_terminal());

        header("Unmatched descendants");
        println!("Child and descendant codes we haven't explicitaly included or excluded");
        let mut table = Table::new().with_row(
            Row::new()
                .with_cell(Cell::from("Code"))
                .with_cell(Cell::from("Descriptions")),
        );
        for code in self.unmatched_descendants.iter() {
            table.add_row(
                Row::new()
                    .with_cell(Cell::from(code.to_string()))
                    .with_cell(Cell::from(show_descriptions(
                        self.th.get(code).expect("unreachable"),
                    ))),
            );
        }
        println!("{}", table.for_terminal());
    }
}
