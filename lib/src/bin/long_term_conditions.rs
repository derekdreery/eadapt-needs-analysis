use eadapt_needs_analysis::{ltcs, read2, Events, Patients};
use qu::ick_use::*;
//use std::collections::BTreeSet;

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

    let report = conditions.report(&patients, &events, &diagnosis_dates);
    println!("{}", report.term_table().for_terminal());

    // TODO just make sure that my quantile function is accurate, then copy table into write-up &
    // send to Niels, then WRITE WRITE WRITE.
    println!(
        "{}",
        report
            .test_significance(0.05, 10, true)
            .term_table()
            .for_terminal()
    );

    /*
    // let's also list what cancer codes people are getting (that aren't lymphoma codes)
    for patient in patients.iter() {
        let evts = events.events_for_patient(patient.patient_id);
        let cancer_codes = conditions.get_can(evts);
        if !cancer_codes.is_empty() {
            println!("\nfor {}", patient.patient_id);
        }
        for (code, date) in cancer_codes {
            println!(
                "{date} {code} {:?}",
                thesaurus.get(code).unwrap_or(&BTreeSet::new())
            );
        }
    }
    */

    Ok(())
}
