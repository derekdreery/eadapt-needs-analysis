use chrono::{DateTime, Utc};
use lalrpop_util::lalrpop_mod;
use logos::Logos;
use qu::ick_use::*;
use regex::{RegexSet, RegexSetBuilder};
use serde::{
    de::{self, Deserializer, MapAccess, SeqAccess, Visitor},
    Deserialize, Serialize,
};
use std::{
    collections::BTreeSet,
    fmt, fs,
    path::{Path, PathBuf},
};

use crate::{
    read2::{ReadCode, Thesaurus},
    util, ArcStr,
};

mod termcodeset;
pub use termcodeset::TermCodeSet;

lalrpop_mod!(parser, "/read2/termset/parser.rs");

/// A list of inclusion and exclusion terms, interpreted as regular expressions.
///
/// We use the same layout as `getset.ga`'s `meta.json`, to facilitate interoperability.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TermSet {
    include_terms: Vec<ArcStr>,
    exclude_terms: Vec<ArcStr>,
    /// Inclusion test regexes
    ///
    /// The way this regex is calculated:
    ///  - All characters are interpreted literally, except for `*`
    ///  - If there is a `*` at the beginning of the term, then allow a match to start mid-word,
    ///    otherwise only start matching at the beginning of a word. Same for end of term.
    ///  - If there is a `*` in the middle of the term, then match 0 or more characters at that
    ///    location (like `.*` in a regex).
    #[serde(skip)]
    includes: FilterSet,
    /// Exclusion test regexes
    ///
    /// Same as for [`TermSet::includes`].
    #[serde(skip)]
    excludes: FilterSet,
    /// Code terminology used (always Readv2 in our case)
    terminology: Terminology,
    /// The name given to the termset
    name: Option<ArcStr>,
    /// The description given to the termset by its author
    description: Option<ArcStr>,
    /// Code terminology version string
    version: ArcStr,
    /// Who created this termset.
    created_by: Option<User>,
    /// When the termset was created.
    created_on: DateTime<Utc>,
    /// When the termset was last updated.
    last_updated: DateTime<Utc>,
}

// manually deserialize to make sure we compute `includes` and `excludes`.
impl<'de> Deserialize<'de> for TermSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            IncludeTerms,
            ExcludeTerms,
            Terminology,
            Name,
            Description,
            Version,
            CreatedBy,
            CreatedAt,
            LastUpdated,
        }

        // This part could also be generated independently by:
        //
        //    #[derive(Deserialize)]
        //    #[serde(field_identifier, rename_all = "lowercase")]
        //    enum Field { Secs, Nanos }
        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`secs` or `nanos`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "includeTerms" => Ok(Field::IncludeTerms),
                            "excludeTerms" => Ok(Field::ExcludeTerms),
                            "terminology" => Ok(Field::Terminology),
                            "name" => Ok(Field::Name),
                            "description" => Ok(Field::Description),
                            "version" => Ok(Field::Version),
                            "createdBy" => Ok(Field::CreatedBy),
                            "createdOn" => Ok(Field::CreatedAt),
                            "lastUpdated" => Ok(Field::LastUpdated),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct TermSetVisitor;

        impl<'de> Visitor<'de> for TermSetVisitor {
            type Value = TermSet;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct TermSet")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<TermSet, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let include_terms = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let exclude_terms = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let terminology = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let version = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                let created_by = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(4, &self))?;
                let created_on = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(5, &self))?;
                let last_updated = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(6, &self))?;
                TermSet::from_parts(
                    include_terms,
                    exclude_terms,
                    terminology,
                    None,
                    None,
                    version,
                    created_by,
                    created_on,
                    last_updated,
                )
                .map_err(<V::Error as de::Error>::custom)
            }

            fn visit_map<V>(self, mut map: V) -> Result<TermSet, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut include_terms = None;
                let mut exclude_terms = None;
                let mut terminology = None;
                let mut name = None;
                let mut description = None;
                let mut version = None;
                let mut created_by: Option<Option<User>> = None;
                let mut created_on = None;
                let mut last_updated = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::IncludeTerms => {
                            if include_terms.is_some() {
                                return Err(de::Error::duplicate_field("includeTerms"));
                            }
                            include_terms = Some(map.next_value()?);
                        }
                        Field::ExcludeTerms => {
                            if exclude_terms.is_some() {
                                return Err(de::Error::duplicate_field("excludeTerms"));
                            }
                            exclude_terms = Some(map.next_value()?);
                        }
                        Field::Terminology => {
                            if terminology.is_some() {
                                return Err(de::Error::duplicate_field("terminology"));
                            }
                            terminology = Some(map.next_value()?);
                        }
                        Field::Name => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                        Field::Description => {
                            if description.is_some() {
                                return Err(de::Error::duplicate_field("description"));
                            }
                            description = Some(map.next_value()?);
                        }
                        Field::Version => {
                            if version.is_some() {
                                return Err(de::Error::duplicate_field("version"));
                            }
                            version = Some(map.next_value()?);
                        }
                        Field::CreatedBy => {
                            if created_by.is_some() {
                                return Err(de::Error::duplicate_field("createdBy"));
                            }
                            created_by = Some(map.next_value()?);
                        }
                        Field::CreatedAt => {
                            if created_on.is_some() {
                                return Err(de::Error::duplicate_field("createdOn"));
                            }
                            created_on = Some(map.next_value()?);
                        }
                        Field::LastUpdated => {
                            if last_updated.is_some() {
                                return Err(de::Error::duplicate_field("lastUpdated"));
                            }
                            last_updated = Some(map.next_value()?);
                        }
                    }
                }
                let include_terms =
                    include_terms.ok_or_else(|| de::Error::missing_field("includeTerms"))?;
                let exclude_terms =
                    exclude_terms.ok_or_else(|| de::Error::missing_field("excludeTerms"))?;
                let terminology =
                    terminology.ok_or_else(|| de::Error::missing_field("terminology"))?;
                let version = version.ok_or_else(|| de::Error::missing_field("version"))?;
                let created_on = created_on.ok_or_else(|| de::Error::missing_field("createdOn"))?;
                let last_updated =
                    last_updated.ok_or_else(|| de::Error::missing_field("lastUpdated"))?;
                TermSet::from_parts(
                    include_terms,
                    exclude_terms,
                    terminology,
                    name,
                    description,
                    version,
                    created_by.flatten(),
                    created_on,
                    last_updated,
                )
                .map_err(<V::Error as de::Error>::custom)
            }
        }

        const FIELDS: &'static [&'static str] = &[
            "includeTerms",
            "excludeTerms",
            "terminology",
            "name",
            "description",
            "version",
            "createdBy",
            "createdOn",
            "lastUpdated",
        ];
        deserializer.deserialize_struct("TermSet", FIELDS, TermSetVisitor)
    }
}

impl TermSet {
    /// Create a new termset from a set of include and exclude regexes.
    ///
    /// Returns an Arc for easy cloning.
    pub fn new(
        name: Option<ArcStr>,
        description: Option<ArcStr>,
        include_terms: impl IntoIterator<Item = ArcStr>,
        exclude_terms: impl IntoIterator<Item = ArcStr>,
        created_by: Option<User>,
    ) -> Result<Self> {
        TermSet::from_parts(
            include_terms.into_iter().collect(),
            exclude_terms.into_iter().collect(),
            Terminology::Readv2,
            name,
            description,
            "v20160401".into(),
            created_by,
            Utc::now(),
            Utc::now(),
        )
    }

    fn from_parts(
        include_terms: Vec<ArcStr>,
        exclude_terms: Vec<ArcStr>,

        terminology: Terminology,
        name: Option<ArcStr>,
        description: Option<ArcStr>,
        version: ArcStr,
        created_by: Option<User>,
        created_on: DateTime<Utc>,
        last_updated: DateTime<Utc>,
    ) -> Result<Self> {
        let includes = FilterSet::new(include_terms.iter())?;
        let excludes = FilterSet::new(exclude_terms.iter())?;
        Ok(TermSet {
            include_terms,
            exclude_terms,
            includes,
            excludes,
            terminology,
            name,
            description,
            version,
            created_by,
            created_on,
            last_updated,
        })
    }

    pub fn add_include(&mut self, term: ArcStr) -> Result {
        self.include_terms.push(term);
        self.includes = FilterSet::new(self.include_terms.iter())?;
        Ok(())
    }

    pub fn remove_include(&mut self, term: ArcStr) {
        let mut changed = false;
        self.include_terms.retain(|t| {
            if *t == term {
                changed = true;
                false
            } else {
                true
            }
        });
        if changed {
            self.includes = FilterSet::new(self.include_terms.iter()).unwrap();
        }
    }

    pub fn add_exclude(&mut self, term: ArcStr) -> Result {
        self.exclude_terms.push(term);
        self.excludes = FilterSet::new(self.exclude_terms.iter())?;
        Ok(())
    }

    pub fn remove_exclude(&mut self, term: ArcStr) -> Result {
        let mut changed = false;
        self.exclude_terms.retain(|t| {
            if *t == term {
                changed = true;
                false
            } else {
                true
            }
        });
        if changed {
            self.excludes = FilterSet::new(self.exclude_terms.iter())?;
        }
        Ok(())
    }

    pub fn include_filter(&self) -> &FilterSet {
        &self.includes
    }

    pub fn exclude_filter(&self) -> &FilterSet {
        &self.excludes
    }

    /// Does a code description match this termset.
    ///
    /// We only need to check the description to test.
    pub fn is_match(&self, description: &str) -> bool {
        self.includes.is_match(description) && !self.excludes.is_match(description)
    }

    /// Does a code match this termset.
    ///
    /// This will match if
    ///
    /// 1. any description matches an include, and
    /// 2. no description matches an exclude
    pub fn is_match_multi<'a>(
        &self,
        description: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> bool {
        let mut include = false;
        let mut exclude = false;
        for desc in description {
            let desc = desc.as_ref();
            if self.includes.is_match(desc) {
                include = true;
            }
            if self.excludes.is_match(desc) {
                exclude = true;
            }
        }
        include && !exclude
    }

    /// Whether the description matches any of the include or exclude terms.
    ///
    /// Used to check that we've accounted for all child codes.
    fn is_match_inc_or_ex(&self, desc: &str) -> bool {
        self.includes.is_match(desc) || self.excludes.is_match(desc)
    }

    pub fn match_thesaurus(&self, th: Thesaurus) -> TermCodeSet {
        let codes = self.filter(th.iter()).map(|(code, _)| code).collect();
        TermCodeSet::new(codes, self.clone(), th)
    }

    /// Filter an iterator of codes to only contain matching codes.
    pub fn filter<'a>(
        &'a self,
        codes_descriptions: impl IntoIterator<Item = (ReadCode, &'a BTreeSet<ArcStr>)> + 'a,
    ) -> impl Iterator<Item = (ReadCode, &'a BTreeSet<ArcStr>)> + 'a {
        codes_descriptions
            .into_iter()
            .filter(|(_, desc)| self.is_match_multi(desc.iter()))
    }

    /// An identifier for the author.
    pub fn created_by(&self) -> Option<User> {
        self.created_by.clone()
    }

    /// When the termset was created.
    pub fn created_on(&self) -> DateTime<Utc> {
        self.created_on
    }

    /// When the termset was last updated.
    pub fn last_updated(&self) -> DateTime<Utc> {
        self.last_updated
    }

    /// Load a termset from file
    ///
    /// `path` is the path of the parent directory - since we assume termsets are always part of a
    /// `meta.json`, `codes.txt` pair when loading.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        fn inner(path: &Path) -> Result<TermSet> {
            let text = fs::read_to_string(path)?;
            serde_json::from_str(&text).map_err(Error::from)
        }
        let path = path.into().join("meta.json");
        inner(&path).with_context(|| format!("loading termset \"{}\"", path.display()))
    }

    /// Save the termset to a file
    ///
    /// Currently must be a toml file. Filetype is inferred from the extension.
    pub fn save(&self, path: impl Into<PathBuf>, overwrite: bool) -> Result {
        let path = path.into().join("meta.json");
        let parent = path
            .parent()
            .expect("termset save location has no parent directory");
        fs::create_dir_all(parent).context("creating termset directory")?;
        ensure!(
            !util::path_exists(&path)? || overwrite,
            "file already exists"
        );

        let text = serde_json::to_string_pretty(self).context("serializing termset")?;
        fs::write(path, &text).context("saving termset")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub name: ArcStr,
    pub email: ArcStr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Terminology {
    Readv2,
}

// Termset filter parser/codegen
// -----------------------------

/// An object that can be tested against a string to see if it matches.
#[derive(Debug, Clone)]
pub struct FilterSet {
    inner: Vec<Filter>,
}

impl FilterSet {
    /// Build a new filterset from a list of terms (in input form)
    pub fn new(iter: impl Iterator<Item = impl AsRef<str>>) -> Result<Self> {
        Ok(FilterSet {
            inner: iter
                .map(|s| TermFilter::parse(s.as_ref()).map(|tf| tf.codegen()))
                .collect::<Result<_, _>>()?,
        })
    }

    pub fn is_match(&self, input: &str) -> bool {
        self.inner.iter().any(|re| re.is_match(input))
    }

    pub fn filters(&self) -> &[Filter] {
        &self.inner
    }
}

#[derive(Debug, Clone)]
pub struct Filter {
    inner: RegexSet,
}

impl Filter {
    fn new(inner: RegexSet) -> Self {
        Self { inner }
    }

    pub fn is_match(&self, input: &str) -> bool {
        // all regexes in the set must match
        self.inner.matches(&input).iter().count() == self.inner.len()
    }
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use term_data_table::{Cell, Row, Table};
        let mut tbl = Table::new().with_row(Row::new().with_cell(Cell::from("regex")));
        for pattern in self.inner.patterns() {
            tbl.add_row(Row::new().with_cell(Cell::from(pattern)));
        }
        tbl.fmt(f)
    }
}

/// # from 10.1371/journal.pone.0212291
///
/// ## Search rules
///
/// - Case insensitive
/// - Words are matched in any order
/// - All words must be present
/// - Use quotes to match exactly
/// - Wildcards allow partial word searching
/// - Exact matches are never excluded
///
/// After some clarification here is the algorithm used:
///
/// - filter text is tokenized by whitespace, with quotes used for literal tokens.
/// - searches must match *all* tokens in any order.
/// - tokens must match a whole word (e.g. `foo` matches `foo` but not `foobar`)
/// - `*` is a wildcard representing 0 or more characters, which also allows for partial word
///   matches
#[derive(Debug)]
pub struct TermFilter<'input> {
    parts: Vec<Term<'input>>,
}

impl<'input> TermFilter<'input> {
    fn new() -> Self {
        TermFilter { parts: vec![] }
    }

    fn push(mut self, el: Term<'input>) -> Self {
        self.parts.push(el);
        self
    }

    fn parse(input: &'input str) -> Result<Self> {
        parser::TermFilterParser::new()
            .parse(input, TermFilterTok::lalrpop_lex(input))
            // render out error
            .map_err(|e| format_err!("error parsing termset filter: {}", e))
    }

    fn codegen(self) -> Filter {
        Filter::new(
            RegexSetBuilder::new(self.parts.iter().map(|term| term.to_regex()))
                .case_insensitive(true)
                .build()
                .unwrap(),
        )
    }
}

/// These are match terms that are unquoted. If they start and/or end with `*`, then we can match
/// mid-word at the start or end respectively. `parts` represents the interior, divided where we
/// find an asterisk.
#[derive(Debug)]
pub struct Term<'input> {
    parts: Vec<TermPart<'input>>,
}

impl<'input> Term<'input> {
    fn new() -> Self {
        Term { parts: vec![] }
    }

    fn push_literal(mut self, literal: &'input str) -> Self {
        self.parts.push(TermPart::Literal(literal));
        self
    }

    fn push_asterisk(mut self) -> Self {
        self.parts.push(TermPart::Asterisk);
        self
    }

    fn to_regex(&self) -> String {
        let mut out = String::new();
        let mut parts = self.parts.iter().peekable();
        if matches!(parts.peek(), Some(TermPart::Asterisk)) {
            parts.next(); // discard
        } else {
            out.push_str(r"\b");
        }
        while let Some(part) = parts.next() {
            match part {
                TermPart::Asterisk => {
                    if parts.peek().is_some() {
                        out.push_str(r"\S*");
                    } else {
                        // nothing to do - we skipped the word boundary using peek last iter.
                    }
                }
                TermPart::Literal(part) => {
                    out.push_str(&regex::escape(part));
                    // add on word boundary if we are at the end.
                    if parts.peek().is_none() {
                        out.push_str(r"\b");
                    }
                }
            }
        }
        out
    }
}

#[derive(Debug)]
pub enum TermPart<'input> {
    Literal(&'input str),
    Asterisk,
}

#[derive(Logos, Copy, Clone, Debug, PartialEq)]
pub enum TermFilterTok<'input> {
    #[regex(r#""[^"]+""#, |lex| lex.slice().trim_matches('"'))]
    #[regex(r#"'[^']+'"#, |lex| lex.slice().trim_matches('\''))]
    #[regex(r#"[^*" \t\n\f]+"#, |lex| lex.slice())]
    Literal(&'input str),
    #[regex(r"[ \t\n\f]+")]
    Whitespace,
    #[token("*")]
    Asterisk,
    #[error]
    Error,
}

impl<'input> TermFilterTok<'input> {
    fn lalrpop_lex(input: &'input str) -> impl Iterator<Item = Spanned<'input>> {
        LalrpopIter(TermFilterTok::lexer(input))
    }
}

impl fmt::Display for TermFilterTok<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TermFilterTok::Literal(lit) => write!(f, "Literal({:?})", lit),
            TermFilterTok::Whitespace => write!(f, "Whitespace"),
            TermFilterTok::Asterisk => write!(f, "Asterisk"),
            TermFilterTok::Error => write!(f, "lexer error"),
        }
    }
}

type Spanned<'input> = Result<(usize, TermFilterTok<'input>, usize), Error>;

struct LalrpopIter<'input>(logos::Lexer<'input, TermFilterTok<'input>>);

impl<'input> Iterator for LalrpopIter<'input> {
    type Item = Spanned<'input>;
    fn next(&mut self) -> Option<Self::Item> {
        let tok = self.0.next()?;
        if matches!(tok, TermFilterTok::Error) {
            return Some(Err(format_err!("lexing failed")));
        }
        let span = self.0.span();
        Some(Ok((span.start, tok, span.end)))
    }
}

#[cfg(test)]
mod test {
    use super::{FilterSet, Term, TermFilter};
    use std::iter;

    #[test]
    fn term_set() {
        let input = "lymphoma/";
        let filter = TermFilter::new()
            .push(Term::new().push_literal("lymphoma"))
            .codegen();
        assert!(filter.is_match(input))
    }

    #[test]
    fn multi() {
        let input = "secondary and unspecified";
        let filter = FilterSet::new(iter::once(input)).unwrap();
        assert!(filter.is_match(input));
    }
}
