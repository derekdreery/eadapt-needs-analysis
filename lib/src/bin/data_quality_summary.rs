use chrono::NaiveDate;
use eadapt_needs_analysis::{Events, Range, RangeSet};

use qu::ick_use::*;
use term_data_table::{Cell, Row, Table};

#[qu::ick]
pub fn main() -> Result {
    //let patients = Patients::load("patients_clean.bin")?;
    let events = Events::load("events_clean.bin")?;
    let events_len = events.len();
    //let adapt = Adapts::load("adapt.bin")?;
    //let thesaurus = Thesaurus::load("../../readbrowser")?;
    //let codes_subtypes_map = CodeSubtypeMap::load("code_subtype_map.bin")?;
    //let lymphoma_codeset = CodeSet::load("lymphoma_codes_clean.toml")?;

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
    // filter out dates we know are bogus.
    let dates = events.iter().map(|evt| {
        if evt.date > NaiveDate::from_ymd(1900, 1, 1) {
            Some(evt.date)
        } else {
            None
        }
    });
    let bucketed = date_buckets.bucket_values_with_missing(dates);
    for (label, count) in bucketed.for_display() {
        table.add_row(
            Row::new()
                .with_cell(Cell::from(label.to_string()))
                .with_cell(Cell::from(count.to_string()))
                .with_cell(Cell::from(format!(
                    "{:.1}%",
                    count as f64 / events_len as f64 * 100.
                ))),
        );
    }
    println!("{}", table);
    Ok(())
}
