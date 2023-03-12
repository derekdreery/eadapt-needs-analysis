use clap::Parser;
use eadapt_needs_analysis::{
    read2::{ReadCode, TermCodeSet, Thesaurus},
    Adapts, CodeRubricCounts, Events, Patients,
};
use qu::ick_use::*;
use std::collections::HashSet;

#[derive(Parser)]
struct Opt {
    #[clap(long, short)]
    overwrite: bool,
}

#[qu::ick]
pub fn main(opt: Opt) -> Result {
    let mut patients = Patients::load("patients.bin")?;
    let mut events = Events::load("events.bin")?;
    let adapt = Adapts::load("adapt.bin")?;
    let thesaurus = Thesaurus::load()?;
    let mut lymphoma_termset = TermCodeSet::load("lymphoma", thesaurus.clone())?;

    // Build a map from code/rubric pairs to patient IDs.
    let code_rubrics = CodeRubricCounts::from_events(&events, &thesaurus);

    header("Before cleaning");
    println!("total patients: {}", patients.len());
    println!("total events: {}", events.len());
    println!("total patient adapt info: {}", adapt.len());

    // codes and descriptions we will remove before any analysis.
    //
    // We got these by manually inspecting all code/free text combinations.
    let codes_to_remove = HashSet::from([ReadCode::try_from("M1628").unwrap()]);
    let descriptions_to_remove = HashSet::from([
        "Lymphomatoid papulosis",
        "Haematological malignacy - suspected",
        "Cancer Quality Indicators v20.0.00",
        "Cancer Quality Indicators v23.0.00",
    ]);

    // We can exclude the code from the termset directly
    let old_lymphoma_codes = lymphoma_termset.code_set.clone();
    lymphoma_termset.add_exclude("lymphomatoid papulosis".into())?;
    let lymphoma_codes = lymphoma_termset.code_set.clone();

    let kept_patids = events
        .iter()
        .filter_map(|evt| {
            if lymphoma_termset.code_set.contains(evt.read_code) {
                Some(evt.patient_id)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();
    patients.retain(|pat| kept_patids.contains(&pat.patient_id));
    events.retain(|evt| kept_patids.contains(&evt.patient_id));

    header("After removing M1628 (lymphomatoid papulosis)");
    // check which codes we removed by adding the description of our removed codes to the excludes
    println!("codes removed: {}", old_lymphoma_codes - lymphoma_codes);

    println!("total patients: {}", patients.len());
    println!("total events: {}", events.len());
    println!("total patient adapt info: {}", adapt.len());
    // descriptions that mean we can't be sure if the diagnosis was recent
    //let maybe_recent_codes = HashSet::from([ReadCode::try_from("ZV107").unwrap()]);

    // Now create a set of code_rubrics to include, made by getting all the code/free text pairs in
    // our dataset and removing the free text we want to exclude.
    let lymphoma_coderubrics =
        code_rubrics.filter(|cr| !codes_to_remove.contains(&cr.code_rubric.code));
    // Collect all patients matching the new reduced code rubric.
    let retained_patient_ids = lymphoma_coderubrics.all_patient_ids();
    // Rebuild tables without excluded participants.
    let patients = patients.filter(|pat| retained_patient_ids.contains(&pat.patient_id));
    let events = events.filter(|ev| retained_patient_ids.contains(&ev.patient_id));

    let lymphoma_coderubrics =
        code_rubrics.filter(|cr| !descriptions_to_remove.contains(&*cr.code_rubric.rubric));
    let retained_patient_ids = lymphoma_coderubrics.all_patient_ids();
    // Rebuild tables without excluded participants.
    let patients = patients.filter(|pat| retained_patient_ids.contains(&pat.patient_id));
    let events = events.filter(|ev| retained_patient_ids.contains(&ev.patient_id));

    header("Final dataset for analysis");
    println!("total patients: {}", patients.len());
    println!("total events: {}", events.len());
    println!("total patient adapt info: {}", adapt.len());

    println!(
        "Number of patients with ADAPT info: {}, of which {} are contained in our dataset.",
        adapt.len(),
        adapt
            .iter()
            .filter(|el| patients.find_by_id(el.id).is_some())
            .count()
    );
    println!(
        "Number of patients with ethnicity info: {}",
        patients.iter().filter(|v| v.ethnicity.is_some()).count()
    );

    // write out clean data
    patients.save("patients_clean.bin")?;
    events.save("events_clean.bin")?;
    lymphoma_termset.save("lymphoma_clean", opt.overwrite)?;
    Ok(())
}

fn header(header: &str) {
    let len = header.len();
    print!("\n{}\n", header);
    for _ in 0..len {
        print!("=");
    }
    println!("\n")
}
