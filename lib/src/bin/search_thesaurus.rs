use clap::Parser;
use eadapt_needs_analysis::read2;
use qu::ick_use::*;
use std::{collections::BTreeSet, path::PathBuf};

#[derive(Parser)]
struct Opt {
    /// Include codes where the description matches this regex
    #[clap(short, long)]
    include: Vec<String>,
    /// Exclude codes where the description matches this regex
    #[clap(short, long)]
    exclude: Vec<String>,
    /// A pre-existing term set to use
    #[clap(short, long)]
    term_set_path: Option<PathBuf>,
    /// The Read code to search for.
    #[clap(short, long)]
    code: Option<read2::ReadCode>,
    /// Save the outputted codeset to the given directory.
    #[clap(short, long)]
    name: Option<String>,
    #[clap(short, long)]
    email: Option<String>,
    #[clap(long)]
    save: Option<PathBuf>,
    /// If set, allow overwriting an existing file at the save location
    #[clap(long)]
    overwrite: bool,
    /// If set, output first words of descriptions of unmatched descenants
    ///
    /// This can be useful for copy/pasting into an include or exclude
    #[clap(long)]
    unmatched_first_words: bool,
    /// If set, outputs the descriptions of descendant codes that didn't match
    ///
    /// Descriptions are 1-per-line in lexical order.
    #[clap(long)]
    unmatched_descriptions: bool,
}

enum Mode {
    IncludeExclude,
    Code,
    TermSet,
}

#[qu::ick]
pub fn main(opt: Opt) -> Result {
    let mut mode = None;
    if !opt.include.is_empty() {
        mode = Some(Mode::IncludeExclude);
    }
    if opt.code.is_some() {
        if mode.is_some() {
            bail!("please supply exactly one of --include, --code, --term-set");
        }
        mode = Some(Mode::Code);
    }
    if opt.term_set_path.is_some() {
        if mode.is_some() {
            bail!("please supply exactly one of --include, --code, --term-set");
        }
        mode = Some(Mode::TermSet);
    }
    let mode = if let Some(mode) = mode {
        mode
    } else {
        bail!("please supply exactly one of --include, --code, --term-set");
    };
    let rt = read2::Thesaurus::load()?;

    let user = if let (Some(name), Some(email)) = (opt.name, opt.email) {
        Some(read2::User {
            name: name.into(),
            email: email.into(),
        })
    } else {
        None
    };

    if matches!(mode, Mode::Code) {
        let code = opt.code.unwrap();
        if let Some(descs) = rt.get(code) {
            println!("Descriptions for code {}", code);
            for desc in descs.iter() {
                println!("  {}", desc);
            }
        } else {
            println!("Code {} not found", code);
        }
        return Ok(());
    }

    let termset = if let Some(path) = opt.term_set_path {
        read2::TermSet::load(path)?
    } else {
        read2::TermSet::new(
            None,
            None,
            opt.include.iter().map(|s| s.clone().into()),
            opt.exclude.iter().map(|s| s.clone().into()),
            user,
        )?
    }
    .match_thesaurus(rt.clone());

    println!("Matches\n-------\n");
    println!("{}\n", termset.term_table().for_terminal());
    println!("{} codes matched", termset.code_set.len());

    let unmatched_descendants = termset.descendants_not_included_or_excluded();

    println!("Unmatched descendants\n---------------------\n");
    println!(
        "{}\n",
        unmatched_descendants.term_table(Some(&rt)).for_terminal()
    );
    println!("{} unmatched descendants", unmatched_descendants.len());

    if opt.unmatched_first_words {
        // Create an ordered list of the first words in descriptions for unmatched descendants.
        // (can save time when adding as includes)
        let mut first_words_unmatched = BTreeSet::new();
        for code in unmatched_descendants.iter() {
            let desc = rt.get(code).unwrap();
            let desc = desc.iter().max_by_key(|v| v.len()).unwrap();
            if let Some(first) = desc.split(' ').next() {
                first_words_unmatched.insert(first.trim_matches('*').to_lowercase());
            }
        }

        for word in first_words_unmatched {
            println!("{:?},", word);
        }
    }

    if opt.unmatched_descriptions {
        let mut descriptions = BTreeSet::new();
        for code in unmatched_descendants.iter() {
            let desc = rt.get(code).unwrap();
            for desc in desc.iter() {
                descriptions.insert(desc.to_string());
            }
        }

        for desc in descriptions {
            println!("{:?},", desc);
        }
    }

    if let Some(loc) = &opt.save {
        termset.save(loc, opt.overwrite)?;
    }
    Ok(())
}
