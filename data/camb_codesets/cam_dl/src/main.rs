use std::{fs, io};

use eadapt_needs_analysis::CodeList;
use qu::ick_use::*;

const LIST_INDEX: &str = include_str!("../camb_code_lists.csv");

fn code_lists() -> Result<Vec<CodeList>> {
    csv::Reader::from_reader(io::Cursor::new(LIST_INDEX))
        .into_records()
        .map(|row| CodeList::from_csv_row(row?))
        .collect()
}

#[qu::ick]
fn main() -> Result {
    let code_lists = code_lists()?;
    for code_list in &code_lists {
        let raw = reqwest::blocking::get(&code_list.url())?.bytes()?;
        let mut ar = zip::ZipArchive::new(io::Cursor::new(raw))?;

        for i in 0..ar.len() {
            let mut file = ar.by_index(i)?;
            let out_path = if file.name().contains("DESCRIPTION") {
                format!(
                    "../{}_{}.description.csv",
                    code_list.name.to_lowercase(),
                    code_list.ty.to_string().to_lowercase()
                )
            } else {
                format!(
                    "../{}_{}.csv",
                    code_list.name.to_lowercase(),
                    code_list.ty.to_string().to_lowercase()
                )
            };
            log::info!("Writing {} to {}", file.name(), out_path);
            let mut out_file = fs::File::create(&out_path)?;
            io::copy(&mut file, &mut out_file)?;
        }
    }
    Ok(())
}
