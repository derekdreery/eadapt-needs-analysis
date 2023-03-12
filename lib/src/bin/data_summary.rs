use chrono::NaiveDate;
use eadapt_needs_analysis::{
    read2::CodeSet, read2::Thesaurus, subtypes::CodeSubtypeMap, Adapts, Events, Patients,
};

use qu::ick_use::*;
use term_data_table::Table;

#[qu::ick]
pub fn main() -> Result {
    let patients = Patients::load("patients_clean.bin")?;
    let events = Events::load("events_clean.bin")?;
    let adapt = Adapts::load("adapt.bin")?;
    let thesaurus = Thesaurus::load()?;
    let codes_subtypes_map = CodeSubtypeMap::load("code_subtype_map.bin")?;

    println!("{}", Table::from_serde(patients.iter_ref().take(10))?);
    Ok(())
}
