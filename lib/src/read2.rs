//! Get at the data in the Read browser, and use it to build a query utility for read v2.

mod codeset;
pub use codeset::{CodeSet, CodeSetMatcher};
mod termset;
pub use termset::{TermCodeSet, TermSet, User};
mod thesaurus;
pub use thesaurus::Thesaurus;

use crate::ArcStr;
use qu::ick_use::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::BTreeSet,
    fmt::{self, Write},
    str::{self, FromStr},
};

/// With Read v2, the codes themselves expose the hierarchical structure.
///
/// For example `2X...` is a parent of `2X3..` or `2XFAD` (made up codes).
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct ReadCode([u8; 5]);

impl ReadCode {
    pub fn has_children(self) -> bool {
        self.0[4] == b'.'
    }

    pub fn is_child_of(self, parent: ReadCode) -> bool {
        if self == parent {
            return false;
        }
        for i in 0..5 {
            if self.0[i] != parent.0[i] && parent.0[i] != b'.' {
                return false;
            }
        }
        true
    }

    pub fn is_parent_of(self, child: ReadCode) -> bool {
        child.is_child_of(self)
    }

    pub fn from_bytes(v: &[u8]) -> Result<Self> {
        // validate
        if v.len() == 5 {
            ensure!(
                v.iter().copied().all(|ch| is_read_ch(ch)),
                "read codes contain characters [a-zA-Z0-9.]"
            );
        } else if v.len() == 7 {
            let mut iter = v.iter().copied();
            for _ in 0..5 {
                ensure!(
                    matches!(iter.next(), Some(ch) if is_read_ch(ch)),
                    "Read codes contain characters [a-zA-Z0-9.]"
                );
            }
            for _ in 0..2 {
                ensure!(
                    matches!(iter.next(), Some(ch) if ch.is_ascii_digit()),
                    "Read code synonyms contain only numbers"
                );
            }
        } else {
            bail!(
                "expected a 5 or 7 characters long ascii string, found {}",
                v.len()
            );
        }

        // convert
        Ok(ReadCode([v[0], v[1], v[2], v[3], v[4]]))
    }

    pub fn from_str(v: &str) -> Result<Self> {
        Self::from_bytes(v.as_bytes())
    }
}

impl fmt::Debug for ReadCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&String::from_utf8_lossy(&self.0), f)
    }
}

impl fmt::Display for ReadCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

// Parents come directly before children (depth-first order)
impl PartialOrd for ReadCode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadCode {
    fn cmp(&self, other: &Self) -> Ordering {
        for idx in 0..5 {
            match (self.0[idx], other.0[idx]) {
                (b'.', b'.') => (), // continue
                (b'.', _) => return Ordering::Less,
                (_, b'.') => return Ordering::Greater,
                // compare lexographically apart from '.'
                (o1, o2) if o1 != o2 => return char::from(o1).cmp(&char::from(o2)),
                _ => (), // continue
            }
        }
        Ordering::Equal
    }
}

impl<'a> TryFrom<&'a str> for ReadCode {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_str(s)
    }
}

impl<'a> TryFrom<&'a [u8]> for ReadCode {
    type Error = Error;
    fn try_from(s: &[u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(s)
    }
}

impl FromStr for ReadCode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str(s)
    }
}

impl AsRef<str> for ReadCode {
    fn as_ref(&self) -> &str {
        str::from_utf8(&self.0).expect("Read code should be valid utf8")
    }
}

impl AsRef<[u8]> for ReadCode {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for ReadCode {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if s.is_human_readable() {
            s.serialize_str(str::from_utf8(&self.0).expect("we know we are an ascii string"))
        } else {
            s.serialize_bytes(&self.0)
        }
    }
}

impl<'de> Deserialize<'de> for ReadCode {
    fn deserialize<D>(deserializer: D) -> Result<ReadCode, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(ReadCodeVisitor)
    }
}

struct ReadCodeVisitor;

impl<'de> serde::de::Visitor<'de> for ReadCodeVisitor {
    type Value = ReadCode;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a Read code (either as a byte array of a string)")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        ReadCode::from_str(v).map_err(serde::de::Error::custom)
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        ReadCode::from_bytes(v).map_err(serde::de::Error::custom)
    }
}

/// A code/free-text pair
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CodeRubric {
    pub code: ReadCode,
    pub rubric: ArcStr,
}

impl CodeRubric {
    pub fn new(code: ReadCode, rubric: impl Into<ArcStr>) -> Self {
        Self {
            code,
            rubric: rubric.into(),
        }
    }
}

fn is_read_ch(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'.'
}

/// Helper to render to string a set of descriptions from a thesaurus.
fn show_descriptions(descs: &BTreeSet<ArcStr>) -> String {
    let mut out = String::new();
    let mut parts = descs.iter();
    if let Some(desc) = parts.next() {
        write!(out, "{:?}", desc).unwrap();
    }
    for desc in parts {
        write!(out, ", {:?}", desc).unwrap();
    }
    out
}
