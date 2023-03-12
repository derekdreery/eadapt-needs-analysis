use crate::{
    read2::{show_descriptions, ReadCode, Thesaurus},
    util, Events, PatientId,
};

use aho_corasick::AhoCorasick;
use chrono::NaiveDate;
use qu::ick_use::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_set, BTreeSet, HashMap},
    fmt, fs,
    io::prelude::*,
    iter, ops,
    path::Path,
    sync::Arc,
};

/// A set of codes.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CodeSet {
    codes: Arc<BTreeSet<ReadCode>>,
}

impl CodeSet {
    /// Duplicates will be removed.
    fn new(codes: BTreeSet<ReadCode>) -> Self {
        Self {
            codes: Arc::new(codes),
        }
    }

    fn update<T>(&mut self, f: impl FnOnce(&mut BTreeSet<ReadCode>) -> T) -> T {
        let out = f(Arc::make_mut(&mut self.codes));
        out
    }

    /// Save a codeset to a list of codes - 1 per line.
    pub fn save(&self, path: impl AsRef<Path>, overwrite: bool) -> Result {
        fn inner(this: &CodeSet, path: &Path, overwrite: bool) -> Result {
            ensure!(
                overwrite || !util::path_exists(path)?,
                "file already exists"
            );
            let mut file = fs::File::create(path)?;
            for code in this.iter() {
                writeln!(file, "{}", code)?;
            }
            Ok(())
        }

        let path = path.as_ref();
        inner(self, path, overwrite)
            .with_context(|| format!("error writing codeset to file \"{}\"", path.display()))
    }

    /// Load a codeset from a list of codes - 1 per line.
    ///
    /// We use the csv deserializer to get nicer error messages.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        fn inner(path: &Path) -> Result<CodeSet> {
            let reader = fs::File::open(path)?;
            Ok(CodeSet::new(
                csv::Reader::from_reader(reader)
                    .into_deserialize()
                    .map(|v| v.map_err(Error::from))
                    .collect::<Result<BTreeSet<ReadCode>>>()?,
            ))
        }

        let path = path.as_ref();
        inner(path).with_context(|| format!("loading codeset from file \"{}\"", path.display()))
    }

    /// Load a codeset from a file in the cprd@cambridge medcodes format.
    pub fn load_camb(path: impl AsRef<Path>) -> Result<Self> {
        fn inner(path: &Path) -> Result<CodeSet> {
            let reader = fs::File::open(path)?;
            Ok(CodeSet::new(
                csv::Reader::from_reader(reader)
                    .into_records()
                    .filter_map(|field| {
                        let field = match field {
                            Ok(f) => f,
                            Err(e) => return Some(Err(Error::from(e))),
                        };
                        if !matches!(field.get(3), Some(v) if v == "readcode") {
                            return None;
                        }
                        let raw = field.get(1).unwrap();
                        Some(ReadCode::from_str(raw).map_err(Error::from))
                    })
                    .collect::<Result<BTreeSet<ReadCode>>>()?,
            ))
        }

        let path = path.as_ref();
        inner(path).with_context(|| format!("loading codeset from file \"{}\"", path.display()))
    }

    pub fn contains(&self, code: ReadCode) -> bool {
        self.codes.contains(&code)
    }

    pub fn len(&self) -> usize {
        self.codes.len()
    }

    pub fn iter(&self) -> iter::Copied<btree_set::Iter<'_, ReadCode>> {
        self.codes.iter().copied()
    }

    pub fn insert(&mut self, code: ReadCode) {
        self.update(|codes| codes.insert(code));
    }

    /// Remove a code from this code set.
    pub fn remove(&mut self, code: ReadCode) {
        self.update(|codes| codes.remove(&code));
    }

    pub fn term_table(&self, th: Option<&Thesaurus>) -> term_data_table::Table<'_> {
        use term_data_table::{Cell, Row, Table};
        if let Some(th) = th {
            let mut table = Table::new().with_row(
                Row::new()
                    .with_cell(Cell::from("Code"))
                    .with_cell(Cell::from("Descriptions")),
            );
            for code in self.iter() {
                table.add_row(
                    Row::new()
                        .with_cell(Cell::from(code.to_string()))
                        .with_cell(Cell::from(show_descriptions(
                            th.get(code).unwrap_or(&BTreeSet::new()),
                        ))),
                );
            }
            table
        } else {
            let mut table = Table::new().with_row(Row::new().with_cell(Cell::from("Code")));
            for code in self.iter() {
                table.add_row(Row::new().with_cell(Cell::from(code.to_string())));
            }
            table
        }
    }

    /// A version of `CodeSet` that can match codes quickly.
    pub fn into_matcher(self) -> CodeSetMatcher {
        CodeSetMatcher::new(self)
    }
}

impl FromIterator<ReadCode> for CodeSet {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = ReadCode>,
    {
        Self::new(iter.into_iter().collect())
    }
}

impl From<BTreeSet<ReadCode>> for CodeSet {
    fn from(f: BTreeSet<ReadCode>) -> Self {
        Self::new(f)
    }
}

/// Subtraction for `CodeSet`s is defined as the 'set minus' operation, i.e. A - B := the set of
/// all read codes that are in A but *not* in B
impl ops::Sub<CodeSet> for CodeSet {
    type Output = CodeSet;
    fn sub(self, rhs: Self) -> Self::Output {
        Self::from_iter(self.codes.difference(&rhs.codes).copied())
    }
}

impl fmt::Display for CodeSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        let mut codes = self.codes.iter();
        if let Some(code) = codes.next() {
            write!(f, "{}", code)?;
        }
        for code in codes {
            write!(f, ", {}", code)?;
        }
        write!(f, "}}")
    }
}

// CodeSet with a matcher

pub struct CodeSetMatcher {
    code_set: CodeSet,
    matcher: AhoCorasick,
}

impl CodeSetMatcher {
    fn new(code_set: CodeSet) -> Self {
        let matcher = AhoCorasick::new(code_set.iter().map(|code| code));
        Self { code_set, matcher }
    }

    pub fn contains(&self, code: ReadCode) -> bool {
        self.matcher.is_match(code)
    }

    pub fn earliest_code(&self, events: &Events) -> HashMap<PatientId, NaiveDate> {
        let mut map = HashMap::new();
        for evt in events.iter().filter(|evt| self.contains(evt.read_code)) {
            let entry = map.entry(evt.patient_id).or_insert(evt.date);
            if *entry < evt.date {
                *entry = evt.date;
            }
        }
        map
    }

    pub fn into_inner(self) -> CodeSet {
        self.code_set
    }
}

impl ops::Deref for CodeSetMatcher {
    type Target = CodeSet;

    fn deref(&self) -> &Self::Target {
        &self.code_set
    }
}
