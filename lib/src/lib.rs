pub mod ltcs;
mod range;
pub mod read2;
pub mod subtypes;
mod util;

pub use anyhow::{Context, Error};
use chrono::{Datelike, NaiveDate, Utc};
use itertools::Either;
use qu::ick_use::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs, io, iter,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

pub use crate::{
    range::{Range, RangeSet, RangeSetCounts, RangeSetCountsWithMissing},
    read2::ReadCode,
    util::{header, ResultExt, Table},
};
use crate::{
    read2::{CodeRubric, CodeSet, Thesaurus},
    subtypes::{CodeSubtypeMap, LymphomaSubtype},
    util::{adapt_date, bool_01, imd, maybe_read, opt_adapt_date, optional_string},
};

pub fn date_of_extract() -> NaiveDate {
    NaiveDate::from_ymd_opt(2021, 11, 17).unwrap()
}

pub type ArcStr = Arc<str>;
pub type Result<T = (), E = anyhow::Error> = std::result::Result<T, E>;
pub type PatientId = u64;

#[derive(Debug, Clone, Deserialize)]
struct PatientRaw {
    #[serde(rename = "PatID")]
    patient_id: PatientId,
    #[serde(rename = "YearOfBirth")]
    year_of_birth: u16,
    #[serde(rename = "Sex")]
    sex: Sex,
    #[serde(rename = "Ethnicity", deserialize_with = "optional_string")]
    ethnicity: Option<ArcStr>,
    #[serde(rename = "LSOA", deserialize_with = "optional_string")]
    _lsoa: Option<ArcStr>,
    #[serde(rename = "GPCode")]
    _gp_code: ArcStr,
    #[serde(
        rename = "imdDecile-1-is-most-deprived-10percent",
        deserialize_with = "imd"
    )]
    imd: Imd,
    #[serde(rename = "charlson-0-is-healthy")]
    charlson: f32,
}

/// A row in the patients dataset.
///
/// In this and future datastructures, `id` (PadID) always identifies the same patient.
///
/// Lymphoma diagnosis codes are empty until they are calculated from the event data & lymphoma
/// termset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Patient {
    pub patient_id: PatientId,
    pub year_of_birth: u16,
    pub sex: Sex,
    pub ethnicity: Option<ArcStr>,
    pub imd: Imd,
    pub charlson: f32,
    /// This should be the earilest lymphoma code, even if a later, more specific one is used
    /// below.
    pub lymphoma_diagnosis_date: Option<NaiveDate>,
    /// This code should be as specific as possible.
    pub lymphoma_diagnosis_subtype: Option<LymphomaSubtype>,
}

impl From<PatientRaw> for Patient {
    fn from(from: PatientRaw) -> Self {
        Self {
            patient_id: from.patient_id,
            year_of_birth: from.year_of_birth,
            sex: from.sex,
            ethnicity: from.ethnicity,
            imd: from.imd,
            charlson: from.charlson,
            lymphoma_diagnosis_date: None,
            lymphoma_diagnosis_subtype: None,
        }
    }
}

impl Patient {
    pub fn age_at(&self, date: impl Datelike) -> i32 {
        date.year() - self.year_of_birth as i32
    }
}

/// The parsed list of patients, with a pre-built index for the `id` field.
pub struct Patients {
    els: Arc<Vec<Patient>>,
    id_idx: BTreeMap<u64, usize>,
}

impl Patients {
    pub fn load_orig(
        path: impl AsRef<Path>,
        events: &Events,
        lymphoma_subtype_map: &CodeSubtypeMap,
    ) -> Result<Self, Error> {
        let patients_raw: Vec<PatientRaw> = load_orig(path)?;
        let mut patients = Self::new(patients_raw.into_iter().map(Into::into).collect());
        patients.calc_lymphoma_data(events, lymphoma_subtype_map);
        Ok(patients)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self::new(load(path)?))
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result {
        Ok(save(&self.els, path)?)
    }

    /// This takes our mapping for read code/rubric combos and our code to lymphoma mapping and
    /// fills in lymphoma diagnosis information for patients.
    ///
    /// There should always be a mapping because we made it from the events, so we assume
    /// non-mapping events are not lymphoma.
    fn calc_lymphoma_data(&mut self, events: &Events, map: &CodeSubtypeMap) {
        for event in events.iter() {
            let Some(subtype) = map.get(&event.code_rubric()) else {
                continue
            };
            let Some(patient) = self.find_by_id_mut(event.patient_id) else {
                event!(Level::WARN, "no patient with ID {}", event.patient_id);
                continue
            };

            // update diagnosis date if applicable
            match patient.lymphoma_diagnosis_date {
                None => patient.lymphoma_diagnosis_date = Some(event.date),
                Some(v) if v > event.date => patient.lymphoma_diagnosis_date = Some(event.date),
                _ => (),
            }

            if let Some(old_subtype) = &patient.lymphoma_diagnosis_subtype {
                if subtype.is_subtype_of(old_subtype) {
                    patient.lymphoma_diagnosis_subtype = Some(subtype);
                }
            } else {
                patient.lymphoma_diagnosis_subtype = Some(subtype);
            }
        }
    }

    pub fn find_by_id(&self, id: u64) -> Option<&Patient> {
        let idx = self.id_idx.get(&id)?;
        let el = self.els.get(*idx)?;
        Some(el)
    }

    /// Note this will clone the patients internally if they are shared. Other clones of `self`
    /// will not be updated
    pub fn find_by_id_mut(&mut self, id: u64) -> Option<&mut Patient> {
        let idx = self.id_idx.get(&id)?;
        let el = Arc::make_mut(&mut self.els).get_mut(*idx)?;
        Some(el)
    }

    pub fn count_sexes(&self) -> BTreeMap<Sex, usize> {
        // B Tree so we get a predictable ordering.
        let mut map = BTreeMap::new();
        // Manually insert to make sure all categories are included.
        map.insert(Sex::Male, 0);
        map.insert(Sex::Female, 0);
        for el in self.els.iter() {
            *map.entry(el.sex).or_insert(0) += 1;
        }
        map
    }

    pub fn bucket_ages(&self, ranges: &RangeSet<u16>) -> RangeSetCounts<u16> {
        let now = Utc::now();
        ranges.clone().bucket_values(
            self.iter()
                .map(|pat| u16::try_from(pat.age_at(now)).unwrap()),
        )
    }

    pub fn count_imd(&self) -> BTreeMap<Imd, usize> {
        // B Tree so we get a predictable ordering.
        let mut map = BTreeMap::new();
        // Manually insert to make sure all categories are included.
        map.insert(Imd::Missing, 0);
        map.insert(Imd::_1, 0);
        map.insert(Imd::_2, 0);
        map.insert(Imd::_3, 0);
        map.insert(Imd::_4, 0);
        map.insert(Imd::_5, 0);
        map.insert(Imd::_6, 0);
        map.insert(Imd::_7, 0);
        map.insert(Imd::_8, 0);
        map.insert(Imd::_9, 0);
        map.insert(Imd::_10, 0);
        for el in self.els.iter() {
            *map.entry(el.imd).or_insert(0) += 1;
        }
        map
    }

    pub fn iter(&self) -> impl Iterator<Item = Patient> + '_ {
        self.els.iter().cloned()
    }

    pub fn iter_ref(&self) -> impl Iterator<Item = &Patient> + '_ {
        self.els.iter()
    }

    pub fn filter(&self, f: impl Fn(&Patient) -> bool) -> Self {
        Patients::new(self.iter().filter(f).collect())
    }

    pub fn retain(&mut self, f: impl Fn(&Patient) -> bool) {
        Arc::make_mut(&mut self.els).retain(f)
    }

    pub fn term_table(&self) -> term_data_table::Table {
        term_data_table::Table::from_serde(self.iter()).unwrap()
    }

    pub fn evcxr_display(&self) {
        Table::new(&*self.els, |row, _| {
            (
                row.patient_id,
                row.year_of_birth,
                row.sex,
                row.imd,
                row.charlson,
            )
        })
        .with_headers(["patient ID", "birth year", "sex", "IMD", "charlson"])
        .evcxr_display();
    }

    fn new(els: Vec<Patient>) -> Self {
        let mut this = Patients {
            els: els.into(),
            id_idx: BTreeMap::new(),
        };
        this.rebuild_index();
        this
    }

    fn rebuild_index(&mut self) {
        self.id_idx.clear();
        for (idx, el) in self.els.iter().enumerate() {
            self.id_idx.insert(el.patient_id, idx);
        }
    }
}

impl Deref for Patients {
    type Target = [Patient];
    fn deref(&self) -> &Self::Target {
        &*self.els
    }
}

#[derive(Debug, Deserialize)]
pub struct EventRaw {
    #[serde(rename = "PatID")]
    pub patient_id: PatientId,
    #[serde(rename = "EntryDate")]
    pub date: NaiveDate,
    #[serde(rename = "ReadCode", deserialize_with = "maybe_read")]
    pub read_code: Option<ReadCode>,
    #[serde(rename = "Rubric")]
    pub rubric: ArcStr,
    #[serde(rename = "CodeValue")]
    pub code_value: Option<ArcStr>,
    #[serde(rename = "CodeUnits")]
    pub code_units: Option<ArcStr>,
    #[serde(rename = "Source")]
    pub source: ArcStr,
}

/// A row in the events dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub patient_id: PatientId,
    pub date: NaiveDate,
    pub read_code: ReadCode,
    pub rubric: ArcStr,
    pub code_value: Option<ArcStr>,
    pub code_units: Option<ArcStr>,
    pub source: ArcStr,
}

impl Event {
    fn from_raw(raw: EventRaw) -> Option<Self> {
        match raw.read_code {
            Some(read_code) => Some(Event {
                patient_id: raw.patient_id,
                date: raw.date,
                read_code,
                rubric: raw.rubric,
                code_value: raw.code_value,
                code_units: raw.code_units,
                source: raw.source,
            }),
            None => None,
        }
    }

    /// Extract the Read code and free text from this event.
    pub fn code_rubric(&self) -> CodeRubric {
        CodeRubric {
            code: self.read_code,
            rubric: self.rubric.clone(),
        }
    }
}

/// The parsed list of events, with a pre-built index for the `id` field.
pub struct Events {
    els: Arc<Vec<Event>>,
    id_idx: BTreeMap<u64, Vec<usize>>,
}

impl Events {
    pub fn load_orig(path: impl AsRef<Path>) -> Result<Self, Error> {
        let els: Vec<EventRaw> = load_orig(path)?;
        let els: Vec<Event> = els.into_iter().filter_map(Event::from_raw).collect();
        Ok(Self::new(els))
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self::new(load(path)?))
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result {
        Ok(save(&self.els, path)?)
    }

    pub fn events_for_patient(
        &self,
        patient_id: PatientId,
    ) -> impl Iterator<Item = &Event> + Clone + '_ {
        let evt_idxs = match self.id_idx.get(&patient_id) {
            Some(idxs) => idxs,
            None => return Either::Left(iter::empty()),
        };
        Either::Right(evt_idxs.iter().map(|idx| {
            self.els
                .get(*idx)
                .expect("inconsistent event patient_id index")
        }))
    }

    /// Iterate over events in this store.
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.els.iter().cloned()
    }

    /// Get an `Events` object containing only events that match the filter.
    pub fn filter(&self, f: impl Fn(&Event) -> bool) -> Self {
        Events::new(self.iter().filter(f).collect())
    }

    pub fn retain(&mut self, f: impl Fn(&Event) -> bool) {
        Arc::make_mut(&mut self.els).retain(f)
    }

    /// Creates a new `Events` object with only those events with read codes matching the codeset.
    pub fn filter_by_codeset(&self, codeset: &CodeSet) -> Self {
        let els = self
            .iter()
            .filter(|evt| codeset.contains(evt.read_code))
            .collect();
        Events::new(els)
    }

    /// Get the earliest code recorded for a particular patient.
    ///
    /// Useful in combination with `filter*` methods. If `None`, then there were no events with
    /// valid dates for the patient.
    pub fn earliest_event_for_patient(&self, id: PatientId) -> Option<NaiveDate> {
        let _1900_date = NaiveDate::from_ymd_opt(1900, 01, 01).unwrap();
        self.iter()
            .filter(|event| {
                // Dates seem to default to 1900-01-01 when they are missing
                event.patient_id == id && event.date != _1900_date
            })
            .map(|event| event.date)
            .min()
    }

    pub fn filter_by_patient_id(&self, id: PatientId) -> Self {
        let idxs = match self.id_idx.get(&id) {
            Some(idxs) => idxs,
            None => return Self::new(vec![]),
        };

        Self::new(idxs.iter().map(|idx| self.els[*idx].clone()).collect())
    }

    // TODO we already have this method as `CodeRubricCounts::from_events`.
    pub fn code_rubrics(&self) -> CodeRubricCounts {
        todo!()
    }

    pub fn matching_code_rubrics(&self, _codeset: &CodeSet) -> CodeRubricCounts {
        todo!()
    }

    pub fn term_table(&self) -> term_data_table::Table {
        term_data_table::Table::from_serde(self.iter()).unwrap()
    }

    pub fn evcxr_display(&self) {
        Table::new(self.els.iter(), |evt, _| {
            (
                evt.patient_id,
                evt.date,
                evt.read_code,
                &evt.rubric,
                evt.code_value.as_ref().map(Arc::as_ref).unwrap_or(""),
                evt.code_units.as_ref().map(Arc::as_ref).unwrap_or(""),
                &evt.source,
            )
        })
        .with_headers([
            "patient ID",
            "date",
            "Readv2 code",
            "Rubric (free text)",
            "code value",
            "code units",
            "source",
        ])
        .evcxr_display()
    }

    fn new(els: Vec<Event>) -> Self {
        let mut this = Events {
            els: Arc::new(els),
            id_idx: BTreeMap::new(),
        };
        this.rebuild_id_map();
        this
    }

    fn rebuild_id_map(&mut self) {
        self.id_idx.clear();
        for (idx, event) in self.els.iter().enumerate() {
            self.id_idx
                .entry(event.patient_id)
                .or_insert_with(Vec::new)
                .push(idx);
        }
    }
}

impl Deref for Events {
    type Target = [Event];
    fn deref(&self) -> &Self::Target {
        &*self.els
    }
}

impl<'a> IntoIterator for &'a Events {
    type IntoIter = <&'a [Event] as IntoIterator>::IntoIter;
    type Item = &'a Event;
    fn into_iter(self) -> Self::IntoIter {
        self.els.iter()
    }
}

impl FromIterator<Event> for Events {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Event>,
    {
        Self::new(iter.into_iter().collect())
    }
}

#[derive(Debug, Deserialize)]
struct AdaptRaw {
    #[serde(rename = "PatID")]
    id: u64,
    diagnosis: ArcStr,
    #[serde(deserialize_with = "opt_adapt_date")]
    #[serde(rename = "diagnosisDate")]
    diagnosis_date: Option<NaiveDate>,
    #[serde(deserialize_with = "adapt_date")]
    #[serde(rename = "treatmentEndDate")]
    treatment_end_date: NaiveDate,
    #[serde(deserialize_with = "adapt_date")]
    #[serde(rename = "lastReviewDate")]
    last_review_date: NaiveDate,
    #[serde(deserialize_with = "adapt_date")]
    #[serde(rename = "adaptFormCompletedDate")]
    adapt_form_completed_date: NaiveDate,
    #[serde(deserialize_with = "adapt_date")]
    #[serde(rename = "adaptFormSentDate")]
    adapt_form_sent_date: NaiveDate,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "chemoDoxorubicin")]
    chemo_doxorubicin: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "radiationHeart")]
    radiation_heart: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "femaleSub50ChemoDoxorubicinRadiationHeart")]
    female_sub_50_chemo_doxorubicin_radiation_heart: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "chemoDoxorubicinRadiationHeart")]
    chemo_doxorubicin_radiation_heart: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "radiationLungs")]
    radiation_lungs: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "chemoBleomycin")]
    chemo_bleomycin: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "currentOrExSmoker")]
    current_or_ex_smoker: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "femaleSub36RadiationChest")]
    female_sub_36_radiation_chest: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "radiationThyroid")]
    radiation_thyroid: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "maleChemo")]
    male_chemo: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "anyRadiotherapy")]
    any_radiotherapy: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "radiationHeadNeck")]
    radiation_head_neck: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "radiationGulletStomach")]
    radiation_gullet_stomach: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "radiationBowels")]
    radiation_bowels: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "chemoVincristineVinblastine")]
    chemo_vincristine_vinblastine: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "chemoPrednisoloneDexamethasone")]
    chemo_prednisone_dexamethasone: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "LowEnergyLast12Months")]
    low_energy_last_12_months: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "chemoCisplatinCarboplatin")]
    chemo_cisplatin_carboplatin: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "radiationAbdomenKidney")]
    radiation_abdomen_kidney: bool,
    #[serde(deserialize_with = "bool_01")]
    #[serde(rename = "hodgkinLymphomaStemCellTransplant")]
    hodgkin_lymphoma_stem_cell_transplant: bool,
}

/// A row in the adapt dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Adapt {
    pub id: u64,
    pub diagnosis: ArcStr,
    pub diagnosis_date: Option<NaiveDate>,
    pub treatment_end_date: NaiveDate,
    pub last_review_date: NaiveDate,
    pub adapt_form_completed_date: NaiveDate,
    pub adapt_form_sent_date: NaiveDate,
    pub chemo_doxorubicin: bool,
    pub radiation_heart: bool,
    pub female_sub_50_chemo_doxorubicin_radiation_heart: bool,
    pub chemo_doxorubicin_radiation_heart: bool,
    pub radiation_lungs: bool,
    pub chemo_bleomycin: bool,
    pub current_or_ex_smoker: bool,
    pub female_sub_36_radiation_chest: bool,
    pub radiation_thyroid: bool,
    pub male_chemo: bool,
    pub any_radiotherapy: bool,
    pub radiation_head_neck: bool,
    pub radiation_gullet_stomach: bool,
    pub radiation_bowels: bool,
    pub chemo_vincristine_vinblastine: bool,
    pub chemo_prednisone_dexamethasone: bool,
    pub low_energy_last_12_months: bool,
    pub chemo_cisplatin_carboplatin: bool,
    pub radiation_abdomen_kidney: bool,
    pub hodgkin_lymphoma_stem_cell_transplant: bool,
}

impl From<AdaptRaw> for Adapt {
    fn from(from: AdaptRaw) -> Self {
        Self {
            id: from.id,
            diagnosis: from.diagnosis,
            diagnosis_date: from.diagnosis_date,
            treatment_end_date: from.treatment_end_date,
            last_review_date: from.last_review_date,
            adapt_form_completed_date: from.adapt_form_completed_date,
            adapt_form_sent_date: from.adapt_form_sent_date,
            chemo_doxorubicin: from.chemo_doxorubicin,
            radiation_heart: from.radiation_heart,
            female_sub_50_chemo_doxorubicin_radiation_heart: from
                .female_sub_50_chemo_doxorubicin_radiation_heart,
            chemo_doxorubicin_radiation_heart: from.chemo_doxorubicin_radiation_heart,
            radiation_lungs: from.radiation_lungs,
            chemo_bleomycin: from.chemo_bleomycin,
            current_or_ex_smoker: from.current_or_ex_smoker,
            female_sub_36_radiation_chest: from.female_sub_36_radiation_chest,
            radiation_thyroid: from.radiation_thyroid,
            male_chemo: from.male_chemo,
            any_radiotherapy: from.any_radiotherapy,
            radiation_head_neck: from.radiation_head_neck,
            radiation_gullet_stomach: from.radiation_gullet_stomach,
            radiation_bowels: from.radiation_bowels,
            chemo_vincristine_vinblastine: from.chemo_vincristine_vinblastine,
            chemo_prednisone_dexamethasone: from.chemo_prednisone_dexamethasone,
            low_energy_last_12_months: from.low_energy_last_12_months,
            chemo_cisplatin_carboplatin: from.chemo_cisplatin_carboplatin,
            radiation_abdomen_kidney: from.radiation_abdomen_kidney,
            hodgkin_lymphoma_stem_cell_transplant: from.hodgkin_lymphoma_stem_cell_transplant,
        }
    }
}

/// The parsed list of adapt patient records, with a pre-built index for the `id` field.
///
/// The naming is used because it is consistent, not because it is good.
pub struct Adapts {
    els: Vec<Adapt>,
    id_idx: BTreeMap<u64, usize>,
}

impl Adapts {
    fn new(els: Vec<Adapt>) -> Self {
        let mut this = Self {
            els,
            id_idx: BTreeMap::new(),
        };
        this.rebuild_index();
        this
    }

    fn rebuild_index(&mut self) {
        self.id_idx = self
            .els
            .iter()
            .enumerate()
            .map(|(idx, el): (usize, &Adapt)| (el.id, idx))
            .collect();
    }

    pub fn load_orig(path: impl AsRef<Path>) -> Result<Self, Error> {
        let els: Vec<AdaptRaw> = load_orig(path)?;
        let els: Vec<Adapt> = els.into_iter().map(Into::into).collect();
        let id_idx = els
            .iter()
            .enumerate()
            .map(|(idx, el): (usize, &Adapt)| (el.id, idx))
            .collect();
        Ok(Self { els, id_idx })
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self::new(load(path)?))
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result {
        Ok(save(&self.els, path)?)
    }

    pub fn find_by_id(&self, id: u64) -> Option<&Adapt> {
        let idx = self.id_idx.get(&id)?;
        let el = self.els.get(*idx)?;
        Some(el)
    }
}

impl Deref for Adapts {
    type Target = [Adapt];
    fn deref(&self) -> &Self::Target {
        &*self.els
    }
}

/// Contains all Read code/rubric text combinations along with their count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CodeRubricCount {
    /// The code/free-text pair
    pub code_rubric: CodeRubric,
    /// The description of the read code from the thesaurus.
    pub description: BTreeSet<ArcStr>,
    /// IDs of patients with an event in their record matching the code_rubric.
    pub patient_ids: BTreeSet<PatientId>,
}

/// The parsed list of Read code/rubric pairs, with a pre-built index for the `read_code` field.
pub struct CodeRubricCounts {
    read_code_idx: BTreeMap<ReadCode, Vec<usize>>,
    // Safety: this value must be dropped last
    els: Vec<CodeRubricCount>,
}

impl fmt::Debug for CodeRubricCounts {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CodeRubricCounts")
            .field("els", &self.els)
            .finish()
    }
}

impl CodeRubricCounts {
    /// Collect all code/rubric pairs from the given events.
    pub fn from_events(events: &Events, th: &Thesaurus) -> Self {
        let mut cr = BTreeMap::new();
        for event in events.iter() {
            cr.entry(CodeRubric::new(event.read_code, event.rubric))
                .or_insert(BTreeSet::new())
                .insert(event.patient_id);
        }

        let mut els = Vec::with_capacity(cr.len());
        for (code_rubric, patient_ids) in cr.into_iter() {
            let description = th.get(code_rubric.code);
            els.push(CodeRubricCount {
                code_rubric,
                patient_ids,
                description: description.cloned().unwrap_or(BTreeSet::new()),
            })
        }
        Self::new(els)
    }

    /// Get all patient IDs that appear at least once.
    pub fn all_patient_ids(&self) -> BTreeSet<PatientId> {
        self.iter().fold(BTreeSet::new(), |mut set, itm| {
            for id in itm.patient_ids.iter() {
                set.insert(*id);
            }
            set
        })
    }

    /// Iterate over code rubric counts.
    pub fn iter(&self) -> impl Iterator<Item = CodeRubricCount> + '_ {
        self.els.iter().cloned()
    }

    /// Remove records that don't match the predecate.
    pub fn filter(&self, f: impl Fn(&CodeRubricCount) -> bool) -> Self {
        Self::new(self.iter().filter(f).collect())
    }

    pub fn filter_by_codeset(&self, codeset: &CodeSet) -> Self {
        self.filter(|cr| codeset.contains(cr.code_rubric.code))
    }

    /// Find all the code/rubric pairs with the given code.
    ///
    /// # Panics
    ///
    /// This function panics if `code` isn't a valid Read code
    pub fn find_by_code<'a>(
        &'a self,
        code: impl TryInto<ReadCode>,
    ) -> impl Iterator<Item = &'a CodeRubricCount> + 'a {
        let code = code.try_into().ok().expect("not a valid read code");
        let iter = match self.read_code_idx.get(&code) {
            Some(v) => Either::Left(v.iter()),
            None => Either::Right(std::iter::empty()),
        };

        let this = self;
        iter.filter_map(move |idx| this.els.get(*idx))
    }

    /// Display all or some of the code rubrics.
    ///
    /// Set count to `0` to show all. Set to `None` to let the system decide how many to show.
    pub fn display(&self, count: Option<usize>) {
        let mut table = Table::new(&self.els, |cr, _| {
            (
                cr.code_rubric.code,
                &cr.code_rubric.rubric,
                cr.patient_ids.len(),
                format!("{:?}", cr.description),
            )
        })
        .with_headers([
            "Read code",
            "rubric (free text)",
            "number of patients",
            "thesaurus",
        ]);
        if let Some(count) = count {
            table = table.set_max_rows(count);
        }
        table.evcxr_display();
    }

    pub fn term_table(&self) -> term_data_table::Table {
        term_data_table::Table::from_serde(self.iter()).unwrap()
    }

    pub fn evcxr_display(&self) {
        self.display(None)
    }

    fn new(els: Vec<CodeRubricCount>) -> Self {
        let mut this = Self {
            els,
            read_code_idx: BTreeMap::new(),
        };
        this.rebuild_index();
        this
    }

    fn rebuild_index(&mut self) {
        // Build Read code index.
        self.read_code_idx.clear();
        for (idx, el) in self.els.iter().enumerate() {
            self.read_code_idx
                .entry(el.code_rubric.code)
                .or_insert(vec![])
                .push(idx);
        }
    }
}

impl Deref for CodeRubricCounts {
    type Target = [CodeRubricCount];
    fn deref(&self) -> &Self::Target {
        &*self.els
    }
}

// Sub-types

/// Index of multiple deprivation
///
/// Ordering is arbitrary.
#[derive(Serialize, Deserialize, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Imd {
    #[serde(rename = "", alias = "null")]
    Missing,
    _1,
    _2,
    _3,
    _4,
    _5,
    _6,
    _7,
    _8,
    _9,
    _10,
}

impl fmt::Debug for Imd {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Imd::*;
        match self {
            Missing => f.write_str("missing"),
            _1 => f.write_str("0% - 10%"),
            _2 => f.write_str("10% - 20%"),
            _3 => f.write_str("20% - 30%"),
            _4 => f.write_str("30% - 40%"),
            _5 => f.write_str("40% - 50%"),
            _6 => f.write_str("50% - 60%"),
            _7 => f.write_str("60% - 70%"),
            _8 => f.write_str("70% - 80%"),
            _9 => f.write_str("80% - 90%"),
            _10 => f.write_str("90% - 100%"),
        }
    }
}

impl fmt::Display for Imd {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Sex is encoded 'M' or 'F'. No other values exist in the data. If another value
/// is added in the future, this will throw an error, forcing us to handle the situation.
///
/// Ordering is arbitrary.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, Hash, Ord, PartialOrd)]
pub enum Sex {
    #[serde(rename = "M", alias = "m")]
    Male,
    #[serde(rename = "F", alias = "f")]
    Female,
}

impl fmt::Display for Sex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Sex::Male => f.write_str("Male"),
            Sex::Female => f.write_str("Female"),
        }
    }
}

/// Read an unencrypted set of Read codes - 1 per line.
pub fn load_codes(path: impl AsRef<Path>) -> io::Result<impl Iterator<Item = io::Result<String>>> {
    Ok(io::BufRead::lines(io::BufReader::new(fs::File::open(
        path,
    )?)))
}

/// collect `load_codes` output into a `Vec`.
pub fn load_codes_vec(path: impl AsRef<Path>) -> io::Result<Vec<String>> {
    load_codes(path)?.collect::<io::Result<Vec<_>>>()
}

/// Load data into memory.
fn load<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    fn inner<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
        let path = output_path(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let reader = io::BufReader::new(fs::File::open(path)?);
        bincode::deserialize_from(reader).map_err(Into::into)
    }
    let path = path.as_ref();
    check_extension(&path, "bin")?;

    inner(path).with_context(|| format!("unable to load data from \"{}\"", path.display()))
}

/// Save data to disk.
fn save<T: Serialize>(contents: &[T], path: impl AsRef<Path>) -> Result {
    fn inner<T: Serialize>(contents: &[T], path: &Path) -> Result {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).context("could not create parent")?;
        }
        // it seems File::options().create_new(true) doesn't work on the server, so fall back to
        // checking for existence.
        if util::path_exists(path)? {
            event!(
                Level::WARN,
                "overwriting existing file at \"{}\"",
                path.display()
            );
        }
        let mut out = io::BufWriter::new(fs::File::create(path)?);
        bincode::serialize_into(&mut out, contents)?;
        Ok(())
    }
    let path = path.as_ref();
    let path = output_path(path);
    check_extension(&path, "bin")?;

    inner(contents, &path).with_context(|| format!("unable to save data to \"{}\"", path.display()))
}

/// Load data into memory from the original database extract.
fn load_orig<T: serde::de::DeserializeOwned>(
    path: impl AsRef<Path>,
) -> Result<Vec<T>, anyhow::Error> {
    let path = path.as_ref();
    let path = orig_path(path);
    csv::ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_path(&path)?
        .into_deserialize()
        .collect::<Result<Vec<T>, _>>()
        .with_context(|| format!("while loading \"{}\"", path.display()))
}

/// Note: No protection from escaping the root directory.
pub fn orig_path(input: &Path) -> PathBuf {
    Path::new("../data/sir_data").join(input)
}

/// Note: No protection from escaping the root directory.
pub fn output_path(input: &Path) -> PathBuf {
    Path::new("../data/output").join(input)
}

/// Note: No protection from escaping the root directory.
pub fn termset_path(input: &Path) -> PathBuf {
    Path::new("../data/termsets").join(input)
}

pub fn file_exists(path: &Path) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if matches!(e.kind(), io::ErrorKind::NotFound) => Ok(false),
        Err(e) => Err(e),
    }
}

pub fn check_extension(path: &Path, ext: &str) -> Result<()> {
    ensure!(
        matches!(path.extension(), Some(p) if p == ext),
        "filename should end with `.{}`",
        ext
    );
    Ok(())
}
