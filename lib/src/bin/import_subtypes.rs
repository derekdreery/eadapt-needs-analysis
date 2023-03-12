//! Import lymphoma subtypes mappings from an excel file

use calamine::{Reader, Xlsx};
use eadapt_needs_analysis::{
    read2::{CodeRubric, ReadCode},
    subtypes::{CodeSubtypeMap, LymphomaSubtype},
};
use qu::ick_use::*;
use std::collections::BTreeMap;

#[qu::ick]
fn main() -> Result {
    let path = "../data/code_subtype_mapping.xlsx";
    let mut workbook: Xlsx<_> = calamine::open_workbook(path)?;
    let wksht = workbook
        .worksheet_range("code_subtype_mapping")
        .context("missing `code_subtype_mapping` worksheet")??;
    ensure!(
        matches!(wksht.start(), Some((0, 0))),
        "workbook doesn't start at top-left"
    );
    let end = wksht.end().context("no data in workbook")?;
    println!("Code subtype mapping workbook size: {:?}", end);
    let map = CodeSubtypeMap::from(
        (0..end.0)
            .skip(1) // headers
            .map(|idx| {
                let read = get_read_code((idx, 0), &wksht)?;
                let rubric = get_text((idx, 1), &wksht)?;
                let label = get_text((idx, 2), &wksht)?;
                let label: LymphomaSubtype = label.parse()?;
                Ok((CodeRubric::new(read, rubric), label))
            })
            .collect::<Result<BTreeMap<_, _>>>()?,
    );

    println!("{}", map.term_table());

    map.save("code_subtype_map.bin")?;
    Ok(())
}

fn get_text(idx: (u32, u32), wksht: &calamine::Range<calamine::DataType>) -> Result<&str> {
    let text = wksht.get_value(idx).context("index out of bounds")?;
    Ok(text
        .get_string()
        .with_context(|| format!("`{}` not text", text))?
        .trim())
}

fn get_read_code(idx: (u32, u32), wksht: &calamine::Range<calamine::DataType>) -> Result<ReadCode> {
    ReadCode::try_from(get_text(idx, wksht)?)
}
