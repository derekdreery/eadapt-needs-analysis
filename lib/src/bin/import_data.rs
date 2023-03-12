use qu::ick_use::*;

use eadapt_needs_analysis::{subtypes::CodeSubtypeMap, Adapts, Events, Patients};

#[qu::ick]
fn main() -> Result {
    let events = Events::load_orig("full.records.csv")?;
    events.save("events.bin")?;

    let code_subtype_map = CodeSubtypeMap::load("code_subtype_map.bin")?;
    let patients = Patients::load_orig("full.patients.txt", &events, &code_subtype_map)?;
    patients.save("patients.bin")?;

    let adapts = Adapts::load_orig("full.adapt.csv")?;
    adapts.save("adapt.bin")?;
    Ok(())
}
