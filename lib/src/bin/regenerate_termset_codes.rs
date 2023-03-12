use eadapt_needs_analysis::read2;
use qu::ick_use::*;
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(clap::Parser, Debug)]
struct Opt {
    path: Option<PathBuf>,
}

#[qu::ick]
fn main(opt: Opt) -> Result {
    let th = read2::Thesaurus::load()?;
    for dir in fs::read_dir("../data/termsets")? {
        let dir = dir?;
        let name = dir
            .file_name()
            .into_string()
            .map_err(|_| format_err!("path not utf8"))?;
        let dir_path = dir.path();
        if let Some(path) = opt.path.as_ref() {
            if *path != dir_path {
                continue;
            }
        } else {
            if !name.ends_with("meds") {
                // ignore
                continue;
            }
        }
        regenerate_codes(&dir_path, &name, &th)?;
    }
    Ok(())
}

fn regenerate_codes(path: &Path, name: &str, th: &read2::Thesaurus) -> Result {
    let termset = read2::TermSet::load(path)?;
    event!(Level::INFO, "Regenerating codes for termset \"{}\"", name);
    event!(Level::INFO, "  calculating codes");
    let full_set = termset.match_thesaurus(th.clone());
    let out_path = path.join("codes.txt");
    event!(Level::INFO, "  writing codes to \"{}\"", out_path.display());
    full_set.code_set.save(&out_path, true)?;
    Ok(())
}
