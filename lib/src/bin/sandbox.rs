#![allow(unused)]
use chrono::NaiveDate;
use eadapt_needs_analysis::{ltcs, read2, Event, Events, Patients};
use noisy_float::prelude::*;
use qu::ick_use::*;
use std::{
    collections::{BTreeSet, HashSet},
    str::FromStr,
};

#[qu::ick]
pub fn main() -> Result {
    let patients = Patients::load("patients_clean.bin")?;
    let events = Events::load("events_clean.bin")?;
    let conditions = ltcs::Conditions::load()?;
    let thesaurus = read2::Thesaurus::load()?;
    let lymphoma_codeset = read2::TermCodeSet::load("lymphoma_clean", thesaurus.clone())?;

    let diagnosis_dates = lymphoma_codeset
        .code_set
        .into_matcher()
        .earliest_code(&events);

    let mut with_code = 0;
    let mut without_code = 0;
    let mut different_values = BTreeSet::new();
    for event in events
        .iter()
        .filter(|evt| conditions.ckd147.contains(evt.read_code))
    {
        if let Some(value) = get_value(&event) {
            with_code += 1;
            different_values.insert(value);
        } else {
            without_code += 1;
        }
    }
    let total = with_code + without_code;
    println!(
        "{} of {} ({:.1}%) of blood tests have data",
        with_code,
        total,
        with_code as f64 / total as f64 * 100.
    );

    println!("different values seen: {:#?}", different_values);
    Ok(())
}

fn get_value(evt: &Event) -> Option<R64> {
    let val = evt.code_value.as_ref()?;
    let val = val.parse::<f64>().ok()?;
    R64::try_new(val)
}
