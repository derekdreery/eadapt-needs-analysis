//! Long term conditions.
use crate::{date_of_extract, read2, Event, Events, PatientId, Patients};
use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use itertools::chain;
use noisy_float::prelude::*;
use statrs::distribution::{Binomial, DiscreteCDF};
use std::{
    collections::{BTreeMap, HashMap},
    iter,
    path::Path,
};
use term_data_table as tdt;

/// A struct that knows how to test for long term conditions at a particular time.
pub struct Conditions {
    pub alc138: read2::CodeSetMatcher,
    pub ano139: read2::CodeSetMatcher,
    pub anx140: read2::CodeSetMatcher,
    pub anx141: read2::CodeSetMatcher,
    pub ast127: read2::CodeSetMatcher,
    pub ast142: read2::CodeSetMatcher,
    pub atr143: read2::CodeSetMatcher,
    pub bli144: read2::CodeSetMatcher,
    pub bro145: read2::CodeSetMatcher,
    pub can146: read2::CodeSetMatcher,
    pub chd126: read2::CodeSetMatcher,
    pub ckd147: read2::CodeSetMatcher,
    pub cld148: read2::CodeSetMatcher,
    pub con150: read2::CodeSetMatcher,
    pub cop151: read2::CodeSetMatcher,
    pub dem131: read2::CodeSetMatcher,
    pub dep152: read2::CodeSetMatcher,
    pub dep153: read2::CodeSetMatcher,
    pub dib128: read2::CodeSetMatcher,
    pub div154: read2::CodeSetMatcher,
    pub epi155: read2::CodeSetMatcher,
    pub epi156: read2::CodeSetMatcher,
    pub hef158: read2::CodeSetMatcher,
    pub hel157: read2::CodeSetMatcher,
    pub hyp159: read2::CodeSetMatcher,
    pub ibd160: read2::CodeSetMatcher,
    pub ibs161: read2::CodeSetMatcher,
    pub ibs162: read2::CodeSetMatcher,
    pub lea163: read2::CodeSetMatcher,
    pub mig164: read2::CodeSetMatcher,
    pub msc165: read2::CodeSetMatcher,
    pub pep135: read2::CodeSetMatcher,
    pub pnc166: read2::CodeSetMatcher,
    pub pnc167: read2::CodeSetMatcher,
    pub prk169: read2::CodeSetMatcher,
    pub pro170: read2::CodeSetMatcher,
    pub psm173: read2::CodeSetMatcher,
    pub pso171: read2::CodeSetMatcher,
    pub pso172: read2::CodeSetMatcher,
    pub pvd168: read2::CodeSetMatcher,
    pub rhe174: read2::CodeSetMatcher,
    pub scz175: read2::CodeSetMatcher,
    pub scz176: read2::CodeSetMatcher,
    pub sin149: read2::CodeSetMatcher,
    pub str130: read2::CodeSetMatcher,
    pub thy179: read2::CodeSetMatcher,

    lymphoma_leukaemia: read2::CodeSetMatcher,
}

impl Conditions {
    /// Alcohol problems
    pub fn test_alc<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.alc138.contains(evt.read_code))
    }

    /// Anorexia and Bulemia
    pub fn test_ano<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.ano139.contains(evt.read_code))
    }

    /// Combine anxiety and depression as advised by CPRD@Cambridge.
    pub fn test_anx_dep<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        // could do this in 1 pass
        let med_code = events.any(|evt| {
            (self.anx140.contains(evt.read_code) || self.dep152.contains(evt.read_code))
                && evt.date <= date
                && evt.date > date_y(date, -1)
        });
        let prod_code = events
            .filter(|evt| {
                (self.anx141.contains(evt.read_code) || self.dep153.contains(evt.read_code))
                    && evt.date <= date
                    && evt.date > date_y(date, -1)
            })
            .count()
            >= 4;
        med_code || prod_code
    }

    /// Asthma (currently treated)
    pub fn test_ast<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        let diag_code = events.any(|evt| evt.date <= date && self.ast142.contains(evt.read_code));
        let prod_code = events.any(|evt| {
            evt.date <= date && evt.date > date_y(date, -1) && self.ast127.contains(evt.read_code)
        });
        diag_code && prod_code
    }

    /// Atrial fibrillation
    pub fn test_atr<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.atr143.contains(evt.read_code))
    }

    /// Blindness and low vision
    pub fn test_bli<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.bli144.contains(evt.read_code))
    }

    /// Blindness and low vision
    pub fn test_bro<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.bro145.contains(evt.read_code))
    }

    /// New cancer diagnosis in last 5 years.
    pub fn test_can<'a>(
        &'a self,
        events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        // used to keep track of earliest cancer read code, we only report a match if it was within
        // 5 years.
        let mut diags = HashMap::new();

        for evt in events {
            if evt.date <= date
                && self.can146.contains(evt.read_code)
                && !self.lymphoma_leukaemia.contains(evt.read_code)
            {
                let entry = diags.entry(evt.read_code).or_insert(evt.date);
                if evt.date < *entry {
                    *entry = evt.date;
                }
            }
        }

        diags.values().any(|d| *d > date_y(date, -5))
    }

    /// Get all non-lymphoma cancer diagnoses
    ///
    /// This method is for inspecting returned codes, to ensure our method is not bringing in
    /// lymphoma diagnoses.
    pub fn get_can<'a>(
        &'a self,
        events: impl Iterator<Item = &'a Event>,
    ) -> Vec<(read2::ReadCode, NaiveDate)> {
        events
            .filter(|evt| {
                self.can146.contains(evt.read_code)
                    && !self.lymphoma_leukaemia.contains(evt.read_code)
            })
            .map(|evt| (evt.read_code, evt.date))
            .collect()
    }

    /// Coronary heart disease
    pub fn test_chd<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.chd126.contains(evt.read_code))
    }

    /// Chronic kidney disease
    pub fn test_ckd<'a>(
        &'a self,
        events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        let mut levels: BTreeMap<NaiveDate, R64> = BTreeMap::new();
        for event in events.filter(|evt| evt.date <= date && self.ckd147.contains(evt.read_code)) {
            if let Some(val) = parse_egfr(event) {
                levels.insert(event.date, val);
            }
        }
        let mut val_iter = levels.values().rev();
        let mut first = match val_iter.next() {
            Some(v) => *v,
            // assume no ckd if no eGFR tests
            None => return false,
        };
        // take the highest of the first 2
        if let Some(second) = val_iter.next() {
            if *second > first {
                first = *second;
            }
        }
        first < 60.
    }

    /// Chronic liver disease and viral hepititis
    pub fn test_cld<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.cld148.contains(evt.read_code))
    }

    /// Constipation
    pub fn test_con<'a>(
        &'a self,
        events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events
            .filter(|evt| {
                evt.date <= date
                    && evt.date > date_y(date, -1)
                    && self.con150.contains(evt.read_code)
            })
            .count()
            >= 4
    }

    /// COPD
    pub fn test_cop<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.cop151.contains(evt.read_code))
    }

    /// Dementia
    pub fn test_dem<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.dem131.contains(evt.read_code))
    }

    /// Diabetes
    pub fn test_dib<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.dib128.contains(evt.read_code))
    }

    /// Diverticular disease of intestine
    pub fn test_div<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.div154.contains(evt.read_code))
    }

    /// Epilepsy (currently treated)
    pub fn test_epi<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        let medcode = events.any(|evt| evt.date <= date && self.epi155.contains(evt.read_code));
        let prodcode = events.any(|evt| {
            evt.date <= date && evt.date > date_y(date, -1) && self.epi156.contains(evt.read_code)
        });
        medcode && prodcode
    }

    /// Heart failure
    pub fn test_hef<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.hef158.contains(evt.read_code))
    }

    /// Hearing loss
    pub fn test_hel<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.hel157.contains(evt.read_code))
    }

    /// Hypertension
    pub fn test_hyp<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.hyp159.contains(evt.read_code))
    }

    /// Inflammatory bowel disease
    pub fn test_ibd<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.ibd160.contains(evt.read_code))
    }

    /// Irritable bowel syndrome
    pub fn test_ibs<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        let medcode = events.any(|evt| evt.date <= date && self.ibs161.contains(evt.read_code));

        let prodcode = events
            .filter(|evt| {
                evt.date <= date
                    && evt.date > date_y(date, -1)
                    && self.ibs162.contains(evt.read_code)
            })
            .count()
            >= 4;

        medcode || prodcode
    }

    /// Learning disability
    pub fn test_lea<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.lea163.contains(evt.read_code))
    }

    /// Migraine
    pub fn test_mig<'a>(
        &'a self,
        events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events
            .filter(|evt| {
                evt.date <= date
                    && evt.date > date_y(date, -1)
                    && self.mig164.contains(evt.read_code)
            })
            .count()
            >= 4
    }

    /// Multiple sclerosis
    pub fn test_msc<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.msc165.contains(evt.read_code))
    }

    /// Peptic ulcer disease
    pub fn test_pep<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.pep135.contains(evt.read_code))
    }

    /// Painful condition
    pub fn test_pnc<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event> + Clone,
        date: NaiveDate,
    ) -> bool {
        let analcode = events
            .clone()
            .filter(|evt| {
                evt.date <= date
                    && evt.date > date_y(date, -1)
                    && self.pnc166.contains(evt.read_code)
            })
            .count()
            >= 4;
        let antiepicode = events
            .clone()
            .filter(|evt| {
                evt.date <= date
                    && evt.date > date_y(date, -1)
                    && self.pnc167.contains(evt.read_code)
            })
            .count()
            >= 4;
        let epicode = events.any(|evt| evt.date <= date && self.epi155.contains(evt.read_code));
        analcode || (antiepicode && !epicode)
    }

    /// Parkinson's disease
    pub fn test_prk<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.prk169.contains(evt.read_code))
    }

    /// Prostate disorders
    pub fn test_pro<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.pro170.contains(evt.read_code))
    }

    /// Psychoactive substance misuse (except alcohol)
    pub fn test_psm<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.psm173.contains(evt.read_code))
    }

    /// Psoriasis or eczema
    pub fn test_pso<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event> + Clone,
        date: NaiveDate,
    ) -> bool {
        let prodcode = events
            .clone()
            .filter(|evt| {
                evt.date <= date
                    && evt.date > date_y(date, -1)
                    && self.pso172.contains(evt.read_code)
            })
            .count()
            >= 4;
        let medcode = events.any(|evt| evt.date <= date && self.pso171.contains(evt.read_code));
        medcode && prodcode
    }

    /// Peripheral vascular disease
    pub fn test_pvd<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.pvd168.contains(evt.read_code))
    }

    /// Rheumatoid arthritis, other inflammatory polyarthropathies & systematic connective tissue
    /// disorders
    pub fn test_rhe<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.rhe174.contains(evt.read_code))
    }

    /// Schizophrenia (and related non-organic psychosis) or bipolar disorder
    pub fn test_scz<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        let medcode = events.any(|evt| evt.date <= date && self.scz175.contains(evt.read_code));
        let prodcode = events.any(|evt| evt.date <= date && self.scz176.contains(evt.read_code));
        medcode || prodcode
    }

    /// Chronic sinusitis
    pub fn test_sin<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.sin149.contains(evt.read_code))
    }

    /// Stroke and transient aschaemic attach
    pub fn test_str<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.str130.contains(evt.read_code))
    }

    /// Thyroid disorders
    pub fn test_thy<'a>(
        &'a self,
        mut events: impl Iterator<Item = &'a Event>,
        date: NaiveDate,
    ) -> bool {
        events.any(|evt| evt.date <= date && self.thy179.contains(evt.read_code))
    }

    pub fn report(
        &self,
        patients: &Patients,
        events: &Events,
        diagnosis_dates: &HashMap<PatientId, NaiveDate>,
    ) -> ConditionsReport {
        // count of people who got their diagnosis more than 5 years ago
        let extract_date = date_of_extract();
        let y5 = date_y(extract_date, -5);
        let total5 = diagnosis_dates.values().filter(|d| **d < y5).count();
        // count of people who got their diagnosis more than 10 years ago
        let y10 = date_y(extract_date, -10);
        let total10 = diagnosis_dates.values().filter(|d| **d < y10).count();
        let mut report = ConditionsReport::new([patients.len(), total5, total10]);

        for pat in patients.iter() {
            let evts = events.events_for_patient(pat.patient_id);
            let date = match diagnosis_dates.get(&pat.patient_id) {
                Some(date) => *date,
                None => continue,
            };
            let date5 = date_y(date, 5);
            let date10 = date_y(date, 10);

            macro_rules! ltc_test {
                ($field:ident, $test:ident) => {
                    let row = &mut report.$field;
                    if self.$test(evts.clone(), date) {
                        row.y0 += 1;
                    }
                    if date5 <= extract_date && self.$test(evts.clone(), date5) {
                        row.y5 += 1;
                    }
                    if date10 <= extract_date && self.$test(evts.clone(), date10) {
                        row.y10 += 1;
                    }
                };
            }

            ltc_test!(alc, test_alc);
            ltc_test!(ano, test_ano);
            ltc_test!(anx_dep, test_anx_dep);
            ltc_test!(ast, test_ast);
            ltc_test!(atr, test_atr);
            ltc_test!(bli, test_bli);
            ltc_test!(bro, test_bro);
            ltc_test!(can, test_can);
            ltc_test!(chd, test_chd);
            ltc_test!(ckd, test_ckd);
            ltc_test!(cld, test_cld);
            ltc_test!(con, test_con);
            ltc_test!(cop, test_cop);
            ltc_test!(dem, test_dem);
            ltc_test!(dib, test_dib);
            ltc_test!(div, test_div);
            ltc_test!(epi, test_epi);
            ltc_test!(hef, test_hef);
            ltc_test!(hel, test_hel);
            ltc_test!(hyp, test_hyp);
            ltc_test!(ibd, test_ibd);
            ltc_test!(ibs, test_ibs);
            ltc_test!(lea, test_lea);
            ltc_test!(mig, test_mig);
            ltc_test!(msc, test_msc);
            ltc_test!(pep, test_pep);
            ltc_test!(pnc, test_pnc);
            ltc_test!(prk, test_prk);
            ltc_test!(pro, test_pro);
            ltc_test!(psm, test_psm);
            ltc_test!(pso, test_pso);
            ltc_test!(pvd, test_pvd);
            ltc_test!(rhe, test_rhe);
            ltc_test!(scz, test_scz);
            ltc_test!(sin, test_sin);
            ltc_test!(str_, test_str);
            ltc_test!(thy, test_thy);
        }
        report
    }

    /// Load codesets from disk
    pub fn load() -> Result<Self> {
        let data_path = Path::new("../data");
        let termset_path = data_path.join("termsets");
        let camb_codeset_path = data_path.join("camb_codesets");

        macro_rules! camb {
            ($path:expr) => {
                read2::CodeSet::load_camb(camb_codeset_path.join($path))?.into_matcher()
            };
        }

        macro_rules! term {
            ($path:expr) => {
                read2::CodeSet::load(termset_path.join($path).join("codes.txt"))?.into_matcher()
            };
        }

        let alc138 = camb!("alc138_mc.csv");
        let ano139 = camb!("ano139_mc.csv");
        let anx140 = camb!("anx140_mc.csv");
        let anx141 = term!("anxiety_meds");
        let ast127 = term!("asthma_meds");
        let ast142 = camb!("ast142_mc.csv");
        let atr143 = camb!("atr143_mc.csv");
        let bli144 = camb!("bli144_mc.csv");
        let bro145 = camb!("bro145_mc.csv");
        let can146 = camb!("can146_mc.csv");
        let chd126 = camb!("chd126_mc.csv");
        let ckd147 = camb!("ckd147_mc.csv");
        let cld148 = camb!("cld148_mc.csv");
        let con150 = term!("constipation_meds");
        let cop151 = camb!("cop151_mc.csv");
        let dem131 = camb!("dem131_mc.csv");
        let dep152 = camb!("dep152_mc.csv");
        let dep153 = term!("depression_meds");
        let dib128 = camb!("dib128_mc.csv");
        let div154 = camb!("div154_mc.csv");
        let epi155 = camb!("epi155_mc.csv");
        let epi156 = term!("epilepsy_meds");
        let hef158 = camb!("hef158_mc.csv");
        let hel157 = camb!("hel157_mc.csv");
        let hyp159 = camb!("hyp159_mc.csv");
        let ibd160 = camb!("ibd160_mc.csv");
        let ibs161 = camb!("ibs161_mc.csv");
        let ibs162 = term!("ibs_meds");
        let lea163 = camb!("lea163_mc.csv");
        let mig164 = term!("migraine_meds");
        let msc165 = camb!("msc165_mc.csv");
        let pep135 = camb!("pep135_mc.csv");
        let pnc166 = term!("analgesics_ex_migraine_meds");
        let pnc167 = term!("epilepsy_ex_benzos_meds");
        let prk169 = camb!("prk169_mc.csv");
        let pro170 = camb!("pro170_mc.csv");
        let psm173 = camb!("psm173_mc.csv");
        let pso171 = camb!("pso171_mc.csv");
        let pso172 = term!("psoriasis_eczema_meds");
        let pvd168 = camb!("pvd168_mc.csv");
        let rhe174 = camb!("rhe174_mc.csv");
        let scz175 = camb!("scz175_mc.csv");
        let scz176 = term!("schizophrenia_meds");
        let sin149 = camb!("sin149_mc.csv");
        let str130 = camb!("str130_mc.csv");
        let thy179 = camb!("thy179_mc.csv");

        let lymphoma_leukaemia = term!("lymphoma_leukaemia");

        Ok(Conditions {
            alc138,
            ano139,
            anx140,
            anx141,
            ast127,
            ast142,
            atr143,
            bli144,
            bro145,
            can146,
            chd126,
            ckd147,
            cld148,
            con150,
            cop151,
            dem131,
            dep152,
            dep153,
            dib128,
            div154,
            epi155,
            epi156,
            hef158,
            hel157,
            hyp159,
            ibd160,
            ibs161,
            ibs162,
            lea163,
            mig164,
            msc165,
            pep135,
            pnc166,
            pnc167,
            prk169,
            pro170,
            psm173,
            pso171,
            pso172,
            pvd168,
            rhe174,
            scz175,
            scz176,
            sin149,
            str130,
            thy179,
            lymphoma_leukaemia,
        })
    }
}

#[derive(Default, Debug)]
pub struct ConditionsReport {
    totals: [usize; 3],

    alc: ReportRow,
    ano: ReportRow,
    anx_dep: ReportRow,
    ast: ReportRow,
    atr: ReportRow,
    bli: ReportRow,
    bro: ReportRow,
    can: ReportRow,
    chd: ReportRow,
    ckd: ReportRow,
    cld: ReportRow,
    con: ReportRow,
    cop: ReportRow,
    dem: ReportRow,
    dib: ReportRow,
    div: ReportRow,
    epi: ReportRow,
    hef: ReportRow,
    hel: ReportRow,
    hyp: ReportRow,
    ibd: ReportRow,
    ibs: ReportRow,
    lea: ReportRow,
    mig: ReportRow,
    msc: ReportRow,
    pep: ReportRow,
    pnc: ReportRow,
    prk: ReportRow,
    pro: ReportRow,
    psm: ReportRow,
    pso: ReportRow,
    pvd: ReportRow,
    rhe: ReportRow,
    scz: ReportRow,
    sin: ReportRow,
    str_: ReportRow,
    thy: ReportRow,
}

impl ConditionsReport {
    // Prevalence rates come from CPRD@Cambridge.
    const PRE_ALC: f64 = 0.018;
    const PRE_ANO: f64 = 0.005;
    const PRE_ANX: f64 = 0.17;
    const PRE_AST: f64 = 0.042;
    const PRE_ATR: f64 = 0.03;
    const PRE_BLI: f64 = 0.01;
    const PRE_BRO: f64 = 0.004;
    const PRE_CAN: f64 = 0.012;
    const PRE_CKD: f64 = 0.035;
    const PRE_CLD: f64 = 0.006;
    const PRE_SIN: f64 = 0.029;
    const PRE_CON: f64 = 0.022;
    const PRE_COP: f64 = 0.031;
    const PRE_CHD: f64 = 0.055;
    const PRE_DEM: f64 = 0.013;
    const PRE_DEP: f64 = 0.103;
    const PRE_DIB: f64 = 0.059;
    const PRE_DIV: f64 = 0.067;
    const PRE_EPI: f64 = 0.005;
    const PRE_HEL: f64 = 0.111;
    const PRE_HEF: f64 = 0.014;
    const PRE_HYP: f64 = 0.189;
    const PRE_IBD: f64 = 0.01;
    const PRE_IBS: f64 = 0.079;
    const PRE_LEA: f64 = 0.004;
    const PRE_MIG: f64 = 0.004;
    const PRE_MSC: f64 = 0.003;
    const PRE_PNC: f64 = 0.101;
    const PRE_PRK: f64 = 0.003;
    const PRE_PEP: f64 = 0.021;
    const PRE_PVD: f64 = 0.013;
    const PRE_PRO: f64 = 0.057;
    const PRE_PSO: f64 = 0.007;
    const PRE_PSM: f64 = 0.015;
    const PRE_RHE: f64 = 0.025;
    const PRE_SCZ: f64 = 0.003;
    const PRE_STR: f64 = 0.029;
    const PRE_THY: f64 = 0.051;

    fn new(totals: [usize; 3]) -> Self {
        Self {
            totals,
            ..Default::default()
        }
    }

    pub fn term_table(&self) -> tdt::Table {
        use tdt::{Cell, Row, Table};
        let mut table = Table::new()
            .with_row(
                Row::new()
                    .with_cell(Cell::from("Condition"))
                    .with_cell(Cell::from("0 years"))
                    .with_cell(Cell::from("5 years"))
                    .with_cell(Cell::from("10 years")),
            )
            .with_row(
                Row::new()
                    .with_cell(Cell::from("Totals"))
                    .with_cell(Cell::from(self.totals[0].to_string()))
                    .with_cell(Cell::from(self.totals[1].to_string()))
                    .with_cell(Cell::from(self.totals[2].to_string())),
            );
        for (name, data, _) in self.iter() {
            table = table.with_row(data.term_table(name, self.totals));
        }
        table
    }

    /// Perform significance testing
    ///
    /// Params
    ///  - `error` The probability that we would see a 'significant' result at random.
    ///  - `min_count` Exclude conditions that have fewer than this number at baseline
    ///  - `use_bonferroni` Whether to report the 'family-wise error rate'. In practice this means
    ///  that each individual test has a much smaller error rate.
    pub fn test_significance(
        &self,
        mut error: f64,
        min_count: usize,
        use_bonferroni: bool,
    ) -> SignificanceTable {
        // We are doing a 2-sided test so we need to halve the error
        error = error * 0.5;
        if use_bonferroni {
            let total_tests = self
                .iter()
                .filter(|(_, data, _)| data.y0 >= min_count)
                .count()
                * 3;
            println!(
                "Count of conditions meeting minimum threshold: {}",
                total_tests / 3
            );
            println!("Bonferroni factor 1 / {total_tests}");
            error = error / total_tests as f64;
        }

        let low = error;
        let high = 1. - error;

        let rows = self
            .iter()
            .filter(|(_, data, _)| data.y0 >= min_count)
            .map(|(label, data, prevalence)| {
                let total_0y = self.totals[0].try_into().unwrap();
                let binom_0y = Binomial::new(prevalence, total_0y).unwrap();
                println!("binom({prevalence}, {total_0y}).inverse_cdf({low})");
                let low_count_0y = binom_0y.inverse_cdf(low);
                println!("binom({prevalence}, {total_0y}).inverse_cdf({high})");
                let high_count_0y = binom_0y.inverse_cdf(high);

                let total_5y = self.totals[1].try_into().unwrap();
                let binom_5y = Binomial::new(prevalence, total_5y).unwrap();
                println!("binom({prevalence}, {total_5y}).inverse_cdf({low})");
                let low_count_5y = binom_5y.inverse_cdf(low);
                println!("binom({prevalence}, {total_5y}).inverse_cdf({high})");
                let high_count_5y = binom_5y.inverse_cdf(high);

                let total_10y = self.totals[2].try_into().unwrap();
                let binom_10y = Binomial::new(prevalence, total_10y).unwrap();
                println!("binom({prevalence}, {total_10y}).inverse_cdf({low})");
                let low_count_10y = binom_10y.inverse_cdf(low);
                println!("binom({prevalence}, {total_10y}).inverse_cdf({high})");
                let high_count_10y = binom_10y.inverse_cdf(high);

                let y0 = data.y0 as u64;
                let y5 = data.y5 as u64;
                let y10 = data.y10 as u64;
                SignificanceRow {
                    label,
                    null_range_0y: (low_count_0y, high_count_0y),
                    significant_0y: y0 < low_count_0y || y0 > high_count_0y,
                    null_range_5y: (low_count_5y, high_count_5y),
                    significant_5y: y5 < low_count_5y || y5 > high_count_5y,
                    null_range_10y: (low_count_10y, high_count_10y),
                    significant_10y: y10 < low_count_10y || y10 > high_count_10y,
                }
            })
            .collect();

        SignificanceTable { rows }
    }

    // Make it easier to iterate through conditions
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &ReportRow, f64)> {
        macro_rules! iter_impl {
            ($name:expr => $field:ident, $pre:ident) => {
                iter::once(($name, &self.$field, Self::$pre))
            };
        }

        chain![
            iter_impl!("Alcohol problems" => alc, PRE_ALC),
            iter_impl!("Anorexia & Bulemia" => ano, PRE_ANO),
            iter_impl!("Anxiety & Depression" => anx_dep, PRE_DEP),
            iter_impl!("Asthma (currently treated)" => ast, PRE_AST),
            iter_impl!("Atrial fibrillation" => atr, PRE_ATR),
            iter_impl!("Blindness and low vision" => bli, PRE_BLI),
            iter_impl!("Bronchiectasis" => bro, PRE_BRO),
            iter_impl!("Cancer (not lymphoma) within 5 years" => can, PRE_CAN),
            iter_impl!("Coronary heart disease" => chd, PRE_CHD),
            iter_impl!("Chronic kidney failure" => ckd, PRE_CKD),
            iter_impl!("Chronic liver disease & viral hepititis" => cld, PRE_CLD),
            iter_impl!("Constipation (treated)" => con, PRE_CON),
            iter_impl!("COPD" => cop, PRE_COP),
            iter_impl!("Dementia" => dem, PRE_DEM),
            iter_impl!("Diabetes" => dib, PRE_DIB),
            iter_impl!("Diverticular disease of intestine" => div, PRE_DIV),
            iter_impl!("Epilepsy" => epi, PRE_EPI),
            iter_impl!("Heart failure" => hef, PRE_HEF),
            iter_impl!("Hearing loss" => hel, PRE_HEL),
            iter_impl!("Hypertension" => hyp, PRE_HYP),
            iter_impl!("Inflammatory bowel disease" => ibd, PRE_IBD),
            iter_impl!("Irritable bowel syndrome" => ibs, PRE_IBS),
            iter_impl!("Learning disability" => lea, PRE_LEA),
            iter_impl!("Migraine" => mig, PRE_MIG),
            iter_impl!("Multiple sclerosis" => msc, PRE_MSC),
            iter_impl!("Peptic uncer disease" => pep, PRE_PEP),
            iter_impl!("Painful condition" => pnc, PRE_PNC),
            iter_impl!("Parkinson's disease" => prk, PRE_PRK),
            iter_impl!("Prostate disorders" => pro, PRE_PRO),
            iter_impl!("Psychoactive substance misuse (not alcohol)" => psm, PRE_PSM),
            iter_impl!("Psoriasis or eczema" => pso, PRE_PSO),
            iter_impl!("Peripheral vascular disease" => pvd, PRE_PVD),
            iter_impl!(
                "Rheumatoid arthritis, other inflammatory polyarthropathies & systematic \
                    connective tissue disorders" =>
                rhe, PRE_RHE
            ),
            iter_impl!(
                "Schizophrenia (and related non-organic psychosis) or bipolar disorder" =>
                scz, PRE_SCZ
            ),
            iter_impl!("Chronic sinusitis" => sin, PRE_SIN),
            iter_impl!("Stroke and TIA" => str_, PRE_STR),
            iter_impl!("Thyroid disorders" => thy, PRE_THY),
        ]
    }
}

#[derive(Debug, Default)]
pub struct ReportRow {
    /// 0 years after diagnosis
    y0: usize,
    /// 5 years after diagnosis
    y5: usize,
    /// 10 years after diagnosis
    y10: usize,
}

impl ReportRow {
    fn prevalence(&self, totals: [usize; 3]) -> [f64; 3] {
        [
            self.y0 as f64 / totals[0] as f64,
            self.y5 as f64 / totals[1] as f64,
            self.y10 as f64 / totals[2] as f64,
        ]
    }

    fn term_table<'a>(&'a self, title: &'a str, totals: [usize; 3]) -> tdt::Row<'a> {
        use tdt::{Cell, Row};
        let [py0, py5, py10] = self.prevalence(totals);
        Row::new()
            .with_cell(Cell::from(title))
            .with_cell(Cell::from(format!("{} ({:.1}%)", self.y0, py0 * 100.)))
            .with_cell(Cell::from(format!("{} ({:.1}%)", self.y5, py5 * 100.)))
            .with_cell(Cell::from(format!("{} ({:.1}%)", self.y10, py10 * 100.)))
    }
}

pub struct SignificanceTable {
    rows: Vec<SignificanceRow>,
}

impl SignificanceTable {
    pub fn term_table(&self) -> tdt::Table {
        use tdt::Table;
        let mut tbl = Table::new();
        for row in self.rows.iter() {
            tbl.add_row(row.term_table());
        }
        tbl
    }
}

struct SignificanceRow {
    label: &'static str,
    null_range_0y: (u64, u64),
    significant_0y: bool,
    null_range_5y: (u64, u64),
    significant_5y: bool,
    null_range_10y: (u64, u64),
    significant_10y: bool,
}

impl SignificanceRow {
    fn term_table(&self) -> tdt::Row {
        use tdt::{Cell, Row};
        Row::new()
            .with_cell(Cell::from(self.label))
            .with_cell(format!(
                "[{}, {}]{}",
                self.null_range_0y.0,
                self.null_range_0y.1,
                if self.significant_0y {
                    " significant"
                } else {
                    ""
                }
            ))
            .with_cell(format!(
                "[{}, {}]{}",
                self.null_range_5y.0,
                self.null_range_5y.1,
                if self.significant_5y {
                    " significant"
                } else {
                    ""
                }
            ))
            .with_cell(format!(
                "[{}, {}]{}",
                self.null_range_10y.0,
                self.null_range_10y.1,
                if self.significant_10y {
                    " significant"
                } else {
                    ""
                }
            ))
    }
}

/// add years from a date
fn date_y(date: NaiveDate, years: i32) -> NaiveDate {
    date.with_year(date.year() + years).unwrap()
}

fn parse_egfr(evt: &Event) -> Option<R64> {
    let val = evt.code_value.as_ref()?;
    let val = val.parse::<f64>().ok()?;
    R64::try_new(val)
}
