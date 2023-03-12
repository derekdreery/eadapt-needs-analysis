//! Code to calculate which lymphoma subtype a patietn has from the record.
//!
//! We want to report the distribution of lymphoma subtypes amongst our sample. This is more involved
//! because a) we must map Read codes to subtypes, and b) because a patient may have multiple lymphoma
//! codes against them. We need to convert Read codes to subtype labels, and we cannot assume that
//! totals sum to the total patient number (people may have multiple subtypes).
//!
//! Firstly, we manually inspected all code/free text combinations, and allocated each to the
//! most appropriate lymphoma subtype. These subtypes are
//!
//! ```text
//!            lymphoma
//!          /         \
//!      Hodgkin     non-Hodgkin
//!                       |
//!                   non-Hodgin subtypes...
//!```
//!
//!The algorithm for allocating patients to each of these fields is:
//!
//! 1. For Hodgkin and each non-Hodgkin subtype, allocate all patients who have at least one
//!    code from that subtype to the set.
//! 2. Allocate all patients with a non-Hodgkin code AND no code in any subtype to `non-Hodgkin`.
//! 3. Allocate all patients with a lymphoma code and no more specific code to `lymphoma`.
//!
//! This approach means that we count patients at most once in any branch, at the most specific
//! level possible. We may count patients more than once if they are in multiple brances (e.g.
//! Hodgkins and DLBCL) as the patient could have legitimately been diagnosed with two lymphoma
//! subtypes.
//!
//! In order to find all patients allocated in multiple subtypes, we need to take 2 sets of crosses.
//!
//! 1. Between Hodgkin and non-Hodgkin (including subtypes)
//! 2. Between different non-Hodgkin subtypes
//!
use crate::{load, read2::CodeRubric, save, Events, PatientId};
use itertools::Itertools;
use qu::ick_use::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::Path,
};
use term_data_table as tdt;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum LymphomaSubtype {
    Unspecified,
    Hodgkin,
    NonHodgkin(NonHodgkinSubtype),
}

impl std::str::FromStr for LymphomaSubtype {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        use LymphomaSubtype::*;
        match input.trim() {
            "lymphoma" => Ok(Unspecified),
            "hodgkin" => Ok(Hodgkin),
            _ => NonHodgkinSubtype::from_str(input)
                .map(NonHodgkin)
                .map_err(|_| format_err!("didn't recognise lymphoma subtype \"{}\"", input)),
        }
    }
}

impl fmt::Display for LymphomaSubtype {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl LymphomaSubtype {
    /// A human-readable label for the subtype.
    pub fn label(self) -> &'static str {
        use LymphomaSubtype::*;
        match self {
            Unspecified => "Lymphoma (unspecified)",
            Hodgkin => "Hodgkin lymphoma",
            NonHodgkin(subtype) => subtype.label(),
        }
    }

    pub fn code(self) -> &'static str {
        use LymphomaSubtype::*;
        match self {
            Unspecified => "lymphoma",
            Hodgkin => "hodgkin",
            NonHodgkin(subtype) => subtype.code(),
        }
    }

    /// Is `other` a subtype of `self`
    pub fn is_subtype_of(&self, other: &Self) -> bool {
        use LymphomaSubtype::*;
        use NonHodgkinSubtype as NH;
        match (self, other) {
            // everything apart from itself is a subtype of Unspecified
            (Unspecified, Unspecified) => false,
            (_, Unspecified) => true,
            // all NH apart from Unspecified is a subtype of NH::Unspecified
            (NonHodgkin(NH::Unspecified), NonHodgkin(NH::Unspecified)) => false,
            (NonHodgkin(_), NonHodgkin(NH::Unspecified)) => true,
            // that's it
            _ => false,
        }
    }
}

/// Subtypes of non-Hodgkin lymphoma observed in data.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum NonHodgkinSubtype {
    Unspecified,
    Small,
    Splenic,
    Lymphoplasmacytic,
    ExtraMarginal,
    Follicular,
    Mantle,
    DLBCL,
    Mediastinal,
    Burkitt,
    Nasal,
    SubcutaneousT,
    Peripheral,
    Angioimmunoblastic,
    AlkPos,
    AlkNeg,
}

impl std::str::FromStr for NonHodgkinSubtype {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        use NonHodgkinSubtype::*;
        Ok(match input {
            "nonhodgkin" => Unspecified,
            "small" => Small,
            "splenic" => Splenic,
            "lymphoplasmacytic" => Lymphoplasmacytic,
            "extra_marginal" => ExtraMarginal,
            "follicular" => Follicular,
            "mantle" => Mantle,
            "dlbcl" => DLBCL,
            "mediastinal" => Mediastinal,
            "burkitt" => Burkitt,
            "nasal" => Nasal,
            "subcutaneous_t" => SubcutaneousT,
            "peripheral" => Peripheral,
            "angioimmunoblastic" => Angioimmunoblastic,
            "alk_pos" => AlkPos,
            "alk_neg" => AlkNeg,
            _ => bail!("unrecognised non-hodgkin subtype \"{}\"", input),
        })
    }
}

impl NonHodgkinSubtype {
    /// A human-readable label for the subtype.
    ///
    /// Text for non-Hodgkin lymphoma subtypes comes from 'WHO classification of non-Hodgkin
    /// lymphomas 2016'.
    pub fn label(self) -> &'static str {
        use NonHodgkinSubtype::*;
        match self {
            Unspecified => "non-Hodgkin lymphoma (unspecified)",
            Small => "Small lymphocytic lymphoma/chronic lymphocytic leukaemia",
            Splenic => "Splenic marginal zone lymphoma",
            Lymphoplasmacytic => "Lymphoplasmacytic lymphoma",
            ExtraMarginal => "Extranodal marginal zone lymphoma of mucosa-associated lymphoid",
            Follicular => "Follicular lymphoma",
            Mantle => "Mantle cell lymphoma",
            DLBCL => "Diffuse large B-cell lymphoma (DLBCL)",
            Mediastinal => "Primary mediastinal (thymic) large B-cell lymphoma",
            Burkitt => "Burkitt lymphoma",
            Nasal => "Extranodal NK/T-cell lymphoma, nasal type",
            SubcutaneousT => "Subcutaneous T-cell lymphoma",
            Peripheral => "Peripheral T-cell lymphoma",
            Angioimmunoblastic => "Angioimmunoblastic T-cell lymphoma",
            AlkPos => "Anaplastic large-cell lymphoma, ALK positive",
            AlkNeg => "Anaplastic large-cell lymphoma, ALK negative",
        }
    }

    pub fn code(self) -> &'static str {
        use NonHodgkinSubtype::*;
        match self {
            Unspecified => "unspecified",
            Small => "small",
            Splenic => "splenic",
            Lymphoplasmacytic => "lymphoplasmacytic",
            ExtraMarginal => "extra_marginal",
            Follicular => "follicular",
            Mantle => "mantle",
            DLBCL => "dlbcl",
            Mediastinal => "mediastinal",
            Burkitt => "burkitt",
            Nasal => "nasal",
            SubcutaneousT => "subcutaneous_t",
            Peripheral => "peripheral",
            Angioimmunoblastic => "angioimmunoblastic",
            AlkPos => "alk_pos",
            AlkNeg => "alk_neg",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CodeSubtypeMap(BTreeMap<CodeRubric, LymphomaSubtype>);

impl CodeSubtypeMap {
    pub fn save(&self, path: impl AsRef<Path>) -> Result {
        Ok(save(&self.0.iter().collect::<Vec<_>>(), path)?)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let data = load(path)?;
        Ok(CodeSubtypeMap(data.into_iter().collect()))
    }

    pub fn get(&self, code_rubric: &CodeRubric) -> Option<LymphomaSubtype> {
        self.0.get(code_rubric).map(|x| *x)
    }

    /// Takes a collection of record events and classifies the patient IDs.
    ///
    /// See the module documentation for details of how this is accomplished.
    pub fn classify(&self, events: &Events) -> BTreeMap<LymphomaSubtype, BTreeSet<PatientId>> {
        // first, collect all matching patients into each subtype
        let mut subtype_map = events.into_iter().fold(
            BTreeMap::new(),
            |mut map: BTreeMap<LymphomaSubtype, BTreeSet<PatientId>>, event| {
                if let Some(&subtype) = self.0.get(&event.code_rubric()) {
                    map.entry(subtype).or_default().insert(event.patient_id);
                }
                map
            },
        );

        // collect all non-hodgkin subtype ids to remove from `non-hodgkin` and `lymphoma`
        let mut excl_ids = subtype_map.iter().filter(|(subtype, _)| {
            matches!(subtype, LymphomaSubtype::NonHodgkin(s) if !matches!(s, NonHodgkinSubtype::Unspecified))
        }).flat_map(|(_, ids)| ids.iter().copied()).collect::<BTreeSet<_>>();

        // remove from `non-hodgkin`
        let with_excluded = subtype_map
            .get(&LymphomaSubtype::NonHodgkin(NonHodgkinSubtype::Unspecified))
            .map(|set| set.difference(&excl_ids).copied().collect())
            .unwrap_or(BTreeSet::new());
        subtype_map.insert(
            LymphomaSubtype::NonHodgkin(NonHodgkinSubtype::Unspecified),
            with_excluded,
        );

        // add in hodgkin and non-hodgkin ids to remove from `lymphoma`
        excl_ids.extend(
            subtype_map
                .iter()
                .filter(|(subtype, _)| !matches!(subtype, LymphomaSubtype::Unspecified))
                .flat_map(|(_, ids)| ids.iter().copied()),
        );

        // remove from `lymphoma`
        let with_excluded = subtype_map
            .get(&LymphomaSubtype::Unspecified)
            .map(|set| set.difference(&excl_ids).copied().collect())
            .unwrap_or(BTreeSet::new());
        subtype_map.insert(LymphomaSubtype::Unspecified, with_excluded);

        subtype_map
    }

    /// To display in the console/terminal.
    pub fn term_table(&self) -> tdt::Table<'static> {
        self.0.iter().fold(tdt::Table::new(), |tbl, (cr, subtype)| {
            tbl.with_row(
                tdt::Row::new()
                    .with_cell(tdt::Cell::from(cr.code.to_string()))
                    .with_cell(tdt::Cell::from(cr.rubric.to_string()))
                    .with_cell(tdt::Cell::from(subtype.to_string())),
            )
        })
    }

    /// Take a map from subtypes to patient IDs, and return all patient
    /// IDs that belong to more than 1 subtype.
    pub fn find_multiple(
        &self,
        map: &BTreeMap<LymphomaSubtype, BTreeSet<PatientId>>,
    ) -> BTreeMap<(LymphomaSubtype, LymphomaSubtype), BTreeSet<PatientId>> {
        // could be more efficient but small numbers mean it doesn't matter
        let mut product_map = BTreeMap::new();
        for ((ty1, ids1), (ty2, ids2)) in map.iter().cartesian_product(map.iter()) {
            if ty1 < ty2 {
                let mut intersect = ids1.intersection(ids2).peekable();
                if intersect.peek().is_some() {
                    product_map.insert((*ty1, *ty2), intersect.copied().collect());
                }
            } else {
                // skip when subtypes are the same or the other way round
            }
        }
        product_map
    }
}

impl From<BTreeMap<CodeRubric, LymphomaSubtype>> for CodeSubtypeMap {
    fn from(from: BTreeMap<CodeRubric, LymphomaSubtype>) -> Self {
        Self(from)
    }
}
