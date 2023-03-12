use chrono::NaiveDate;
use eadapt_needs_analysis::{
    header,
    read2::{TermCodeSet, Thesaurus},
    subtypes::{CodeSubtypeMap, LymphomaSubtype},
    Adapts, CodeRubricCounts, Events, Imd, Patients, Range, RangeSet,
};
use qu::ick_use::*;
use std::collections::{BTreeMap, BTreeSet};
use term_data_table::{Cell, Row, Table};

#[qu::ick]
pub fn main() -> Result {
    let patients = Patients::load("patients_clean.bin")?;
    let events = Events::load("events_clean.bin")?;
    let adapt = Adapts::load("adapt.bin")?;
    let thesaurus = Thesaurus::load()?;
    let codes_subtypes_map = CodeSubtypeMap::load("code_subtype_map.bin")?;
    let lymphoma_codeset = TermCodeSet::load("lymphoma_clean", thesaurus.clone())?;

    // Build a map from code/rubric pairs to patient IDs.
    let _code_rubrics = CodeRubricCounts::from_events(&events, &thesaurus);

    header("Data stats");
    let patients_len = patients.len();
    println!("total patients: {}", patients_len);
    println!("total events: {}", events.len());
    println!("total patient adapt info: {}", adapt.len());
    if let Some(date) = events.iter().map(|evt| evt.date).max() {
        println!("latest event date: {}", date);
    }
    if let Some(date) = events
        .iter()
        .map(|evt| evt.date)
        .filter(|date| *date > NaiveDate::from_ymd(1900, 1, 1))
        .min()
    {
        println!("earliest event date: {}", date);
    }

    header("Sexes");
    let mut table = Table::new().with_row(
        Row::new()
            .with_cell(Cell::from("Sex"))
            .with_cell(Cell::from("Count"))
            .with_cell(Cell::from("Percentage")),
    );
    for (label, count) in patients.count_sexes() {
        table.add_row(
            Row::new()
                .with_cell(Cell::from(label.to_string()))
                .with_cell(Cell::from(count.to_string()))
                .with_cell(Cell::from(format!(
                    "{:.1}%",
                    count as f64 / patients_len as f64 * 100.
                ))),
        );
    }
    println!("{}", table);

    header("Ages");
    let age_buckets = RangeSet::new(vec![
        Range::new(0, Some(18)),
        Range::new(18, Some(35)),
        Range::new(35, Some(50)),
        Range::new(50, Some(65)),
        Range::new(65, Some(80)),
        Range::new(80, None),
    ]);
    let mut table = Table::new().with_row(
        Row::new()
            .with_cell(Cell::from("Age range"))
            .with_cell(Cell::from("Count"))
            .with_cell(Cell::from("Percentage")),
    );
    for (label, count) in patients.bucket_ages(&age_buckets).iter() {
        table.add_row(
            Row::new()
                .with_cell(Cell::from(label.to_string()))
                .with_cell(Cell::from(count.to_string()))
                .with_cell(Cell::from(format!(
                    "{:.1}%",
                    count as f64 / patients_len as f64 * 100.
                ))),
        );
    }
    println!("{}", table);

    header("Ethnicity");
    println!("Skipping ethnicity becase 0 patients have ethnicity info");

    header("Age at diagnosis");
    let mut table = Table::new().with_row(
        Row::new()
            .with_cell(Cell::from("Age range"))
            .with_cell(Cell::from("Count"))
            .with_cell(Cell::from("Percentage")),
    );
    let lymphoma_events = events.filter_by_codeset(&lymphoma_codeset.code_set);
    let ages_at_diagnosis = patients.iter().map(|pat| {
        lymphoma_events
            .earliest_event_for_patient(pat.patient_id)
            .map(|d| u16::try_from(pat.age_at(d)).unwrap())
    });

    for (label, count) in age_buckets
        .bucket_values_with_missing(ages_at_diagnosis)
        .for_display()
    {
        table.add_row(
            Row::new()
                .with_cell(Cell::from(label.to_string()))
                .with_cell(Cell::from(count.to_string()))
                .with_cell(Cell::from(format!(
                    "{:.1}%",
                    count as f64 / patients_len as f64 * 100.
                ))),
        );
    }
    println!("{}", table);

    header("Date of diagnosis");
    let mut table = Table::new().with_row(
        Row::new()
            .with_cell(Cell::from("Date range"))
            .with_cell(Cell::from("Count"))
            .with_cell(Cell::from("Percentage")),
    );
    let mut date_buckets = RangeSet::new(
        (1900..2020)
            .step_by(10)
            .map(|year| {
                Range::new(
                    NaiveDate::from_ymd(year, 1, 1),
                    Some(NaiveDate::from_ymd(year + 10, 1, 1)),
                )
            })
            .collect(),
    );
    date_buckets.push(Range::new(NaiveDate::from_ymd(2020, 1, 1), None));
    let diagnosis_dates = patients
        .iter()
        .map(|pat| lymphoma_events.earliest_event_for_patient(pat.patient_id));
    for (label, count) in date_buckets
        .bucket_values_with_missing(diagnosis_dates)
        .for_display()
    {
        table.add_row(
            Row::new()
                .with_cell(Cell::from(label.to_string()))
                .with_cell(Cell::from(count.to_string()))
                .with_cell(Cell::from(format!(
                    "{:.1}%",
                    count as f64 / patients_len as f64 * 100.
                ))),
        );
    }
    println!("{}", table);

    header("IMD");
    let mut table = Table::new().with_row(
        Row::new()
            .with_cell(Cell::from("IMD range"))
            .with_cell(Cell::from("Count"))
            .with_cell(Cell::from("Percentage")),
    );
    let imd_counts = patients.count_imd();
    for (label, count) in [
        (
            "0% - 20%",
            imd_counts.get(&Imd::_1).unwrap() + imd_counts.get(&Imd::_2).unwrap(),
        ),
        (
            "20% - 40%",
            imd_counts.get(&Imd::_3).unwrap() + imd_counts.get(&Imd::_4).unwrap(),
        ),
        (
            "40% - 60%",
            imd_counts.get(&Imd::_5).unwrap() + imd_counts.get(&Imd::_6).unwrap(),
        ),
        (
            "60% - 80%",
            imd_counts.get(&Imd::_7).unwrap() + imd_counts.get(&Imd::_8).unwrap(),
        ),
        (
            "80% - 100%",
            imd_counts.get(&Imd::_9).unwrap() + imd_counts.get(&Imd::_10).unwrap(),
        ),
        ("missing", *imd_counts.get(&Imd::Missing).unwrap()),
    ] {
        table.add_row(
            Row::new()
                .with_cell(Cell::from(label.to_string()))
                .with_cell(Cell::from(count.to_string()))
                .with_cell(Cell::from(format!(
                    "{:.1}%",
                    count as f64 / patients_len as f64 * 100.
                ))),
        );
    }
    println!("{}", table);

    header("Lymphoma subtypes");
    let subtype_counts = patients.iter().fold(
        BTreeMap::new(),
        |mut map: BTreeMap<LymphomaSubtype, usize>, patient| {
            if let Some(ref subtype) = patient.lymphoma_diagnosis_subtype {
                *map.entry(*subtype).or_default() += 1;
            }
            map
        },
    );
    let mut table = Table::new().with_row(
        Row::new()
            .with_cell(Cell::from("Subtype"))
            .with_cell(Cell::from("Count"))
            .with_cell(Cell::from("Percentage")),
    );
    for (subtype, count) in subtype_counts.iter() {
        table.add_row(
            Row::new()
                .with_cell(Cell::from(subtype.label()))
                .with_cell(Cell::from(count.to_string()))
                .with_cell(Cell::from(format!(
                    "{:.1}%",
                    *count as f64 / patients_len as f64 * 100.
                ))),
        );
    }
    println!("{}", table);

    header("Multiple subtypes");
    println!("Displays patients who have codes for more than 1 different lymphoma subtype\n");
    let subtype_ids = codes_subtypes_map.classify(&events);
    let multiple_subtype_ids = codes_subtypes_map.find_multiple(&subtype_ids);
    println!(
        "total number of patients with multiple subtype diagnoses: {}",
        multiple_subtype_ids
            .values()
            .flat_map(|ids| ids.iter())
            .collect::<BTreeSet<_>>()
            .len()
    );
    let mut table = Table::new().with_row(
        Row::new()
            .with_cell(Cell::from("Subtype 1"))
            .with_cell(Cell::from("Subtype 2"))
            .with_cell(Cell::from("Count")),
    );
    let multiple_subtype_ids = codes_subtypes_map.find_multiple(&subtype_ids);
    for ((subtype1, subtype2), set) in multiple_subtype_ids.iter() {
        let len = set.len();
        table.add_row(
            Row::new()
                .with_cell(Cell::from(subtype1.label()))
                .with_cell(Cell::from(subtype2.label()))
                .with_cell(Cell::from(len.to_string())),
        );
    }
    println!("{}", table);

    Ok(())
}
