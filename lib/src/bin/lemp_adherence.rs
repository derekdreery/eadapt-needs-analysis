#![feature(array_windows)]
use chrono::{Duration, Months, NaiveDate};
use eadapt_needs_analysis::{
    date_of_extract,
    read2::{CodeSet, Thesaurus},
    subtypes::CodeSubtypeMap,
    Adapt, Adapts, Event, Events, Patient, Patients,
};
use qu::ick_use::*;
use serde::Serialize;
use std::{cmp::Ordering, fmt, iter};
use term_data_table::{Row, Table};

// Tests that we can check using Read code EHR. Start looking when person was 'ADAPTed'.
// Report mean/sd of frequency (measurements per year) and mean/sd of longest gap (years)
//
//  - Annual BP test (doxorubicin, cisplatin/carboplatin, radiation (heart), radiation (abdomen,
//    kidney))
//    - use Richard Williams' termset
//  - 'regular' lipid tests (doxorubicin, radiation (heart))
//    - use Richard Williams' termset
//  - annual flu vaccination (radiation (lungs), bleomycin)
//  - annual breast cancer screening (radiation (chest) + female + <36 years old)
//  - annual TSH test (radiation (thyroid))
//  - annual kidney function test (cisplatin/carboplatin, radiation (abdomen/kidney))
//  - use irradiated blood products
//    - we could check if there is anything on the EHR indicating this, or if there are any Read v2
//    codes for it.

#[qu::ick]
pub fn main() -> Result {
    let patients = Patients::load("patients_clean.bin")?;
    let events = Events::load("events_clean.bin")?;
    let adapt = Adapts::load("adapt.bin")?;

    println!("{}", Table::from_serde(patients.iter_ref().take(10))?);

    let lemp_data = LempData::new(patients, adapt, events);

    let bp_stats = lemp_data.bp_measurement_stats();
    println!("\nBP Stats");
    println!("{}", bp_stats.data_table());

    let cholesterol_stats = lemp_data.cholesterol_measurement_stats();
    println!("\nCholesterol Stats");
    println!("{}", cholesterol_stats.data_table());

    let flu_stats = lemp_data.influenza_vaccination_stats();
    println!("\nFlu Stats");
    println!("{}", flu_stats.data_table());

    let breast_screening_stats = lemp_data.breast_cancer_screening_stats();
    println!("\nBreast screening Stats");
    println!("{}", breast_screening_stats.data_table());

    let thyroid_function_stats = lemp_data.thyroid_function_measurement_stats();
    println!("\nThyroid function Stats");
    println!("{}", thyroid_function_stats.data_table());

    let renal_function_stats = lemp_data.renal_function_measurement_stats();
    println!("\nRenal function Stats");
    println!("{}", renal_function_stats.data_table());

    Ok(())
}

#[derive(Debug, Clone)]
struct PatientAdapt {
    patient: Patient,
    adapt: Adapt,
}

impl PatientAdapt {
    fn from_patients_adapts(patients: Patients, adapts: Adapts) -> Vec<Self> {
        patients
            .iter()
            .filter_map(|patient| {
                adapts
                    .find_by_id(patient.patient_id)
                    .map(|adapt| PatientAdapt {
                        patient,
                        adapt: (*adapt).clone(),
                    })
            })
            .collect()
    }

    fn adapt_date(&self) -> NaiveDate {
        self.adapt.last_review_date
    }
}

struct LempData {
    adapt_patients: Vec<PatientAdapt>,
    events: Events,
}

impl LempData {
    fn new(patients: Patients, adapts: Adapts, events: Events) -> Self {
        let adapt_patients = PatientAdapt::from_patients_adapts(patients, adapts);
        Self {
            adapt_patients,
            events,
        }
    }

    // People should have this test if they have had any of
    //   - doxorubicin
    //   - radiation (heart)
    //   - cisplatin/carboplatin
    //   - radiation (abdomen/kidney)
    fn bp_measurement_stats(&self) -> Stats {
        fn include_test(ap: &&PatientAdapt) -> bool {
            ap.adapt.chemo_doxorubicin
                || ap.adapt.radiation_heart
                || ap.adapt.female_sub_50_chemo_doxorubicin_radiation_heart
                || ap.adapt.chemo_doxorubicin_radiation_heart
                || ap.adapt.chemo_cisplatin_carboplatin
                || ap.adapt.radiation_abdomen_kidney
        }

        // provenance: Richard Williams
        let bp_test_codeset =
            CodeSet::load("../data/termsets/blood_pressure_measurement/codes.txt").unwrap();
        self.codeset_freq_stats(
            &bp_test_codeset,
            self.adapt_patients.iter().filter(include_test),
        )
    }

    // People should have this test if they have had any of
    //   - doxorubicin
    //   - radiation (heart)
    fn cholesterol_measurement_stats(&self) -> Stats {
        fn include_test(ap: &&PatientAdapt) -> bool {
            ap.adapt.chemo_doxorubicin
                || ap.adapt.radiation_heart
                || ap.adapt.female_sub_50_chemo_doxorubicin_radiation_heart
                || ap.adapt.chemo_doxorubicin_radiation_heart
        }

        // provenance: Richard Williams
        let cholesterol_test_codeset =
            CodeSet::load("../data/termsets/cholesterol_measurement/codes.txt").unwrap();
        self.codeset_freq_stats(
            &cholesterol_test_codeset,
            self.adapt_patients.iter().filter(include_test),
        )
    }

    // People should have this test if they have had any of
    //   - bleomycin
    //   - radiation (lungs)
    fn influenza_vaccination_stats(&self) -> Stats {
        fn include_test(ap: &&PatientAdapt) -> bool {
            ap.adapt.chemo_bleomycin || ap.adapt.radiation_lungs
        }

        // provenance: Me using getset
        let influenza_vaccination_codeset =
            CodeSet::load("../data/termsets/influenza_vaccination/codes.txt").unwrap();
        self.codeset_freq_stats(
            &influenza_vaccination_codeset,
            self.adapt_patients.iter().filter(include_test),
        )
    }

    // People should have this test if they have had
    //   - radiation (chest) + female + <36 years old
    fn breast_cancer_screening_stats(&self) -> Stats {
        fn include_test(ap: &&PatientAdapt) -> bool {
            ap.adapt.female_sub_36_radiation_chest
        }

        // provenance: Me using getset
        let breast_cancer_screening_codeset =
            CodeSet::load("../data/termsets/breast_cancer_screening/codes.txt").unwrap();
        self.codeset_freq_stats(
            &breast_cancer_screening_codeset,
            self.adapt_patients.iter().filter(include_test),
        )
    }

    // People should have this test if they have had any of
    //   - radiation (thyroid)
    fn thyroid_function_measurement_stats(&self) -> Stats {
        fn include_test(ap: &&PatientAdapt) -> bool {
            ap.adapt.radiation_thyroid
        }

        // provenance: Richard Williams
        let thyroid_function_test_codeset =
            CodeSet::load("../data/termsets/thyroid_function_measurement/codes.txt").unwrap();
        self.codeset_freq_stats(
            &thyroid_function_test_codeset,
            self.adapt_patients.iter().filter(include_test),
        )
    }

    // People should have this test if they have had any of
    //   - cisplatin/carboplatin
    //   - radiation (abdomen/kidney)
    fn renal_function_measurement_stats(&self) -> Stats {
        fn include_test(ap: &&PatientAdapt) -> bool {
            ap.adapt.chemo_cisplatin_carboplatin || ap.adapt.radiation_abdomen_kidney
        }

        // provenance: Me (getset)
        let renal_function_test_codeset =
            CodeSet::load("../data/termsets/renal_function_measurement/codes.txt").unwrap();
        self.codeset_freq_stats(
            &renal_function_test_codeset,
            self.adapt_patients.iter().filter(include_test),
        )
    }

    /// Reports stats
    fn codeset_freq_stats<'a>(
        &self,
        code_set: &CodeSet,
        patients: impl Iterator<Item = &'a PatientAdapt>,
    ) -> Stats {
        // Collect stuff to work out stats. We work in days here
        let end_date = date_of_extract();
        let mut n: usize = 0;
        let mut rate_sum = 0f64;
        let mut rate_sum_squared = 0f64;
        let mut longest_sum = 0f64;
        let mut longest_sum_squared = 0f64;
        let mut count_no_data = 0;

        let mut patient_rates = vec![];
        let mut patient_longest_gaps = vec![];

        for pa in patients {
            let adapt_date = pa.adapt_date();
            let events = self
                .events
                .events_for_patient(pa.patient.patient_id)
                .filter(|&evt| code_set.contains(evt.read_code) && evt.date >= adapt_date)
                .collect::<Vec<_>>();

            // We increment the denominator.
            n += 1;

            // The timespan between when this patient was ADAPTed, and the date of data extraction,
            // in years.
            let span = (end_date - adapt_date).num_seconds() as f64 / (60. * 60. * 24. * 365.25);
            // The rate of measurement, in years.
            let rate = events.len() as f64 / span;

            // Keep track of the number of people who never had a test
            if events.is_empty() {
                count_no_data += 1;
            }

            // Stats
            patient_rates.push(rate);
            rate_sum += rate;
            rate_sum_squared += rate * rate;

            // The longest time without a test, in years.
            let longest = biggest_gap(adapt_date, end_date, events.iter().copied()).num_days()
                as f64
                / 365.25;
            assert!(longest >= 0.);
            patient_longest_gaps.push(longest);
            longest_sum += longest;
            longest_sum_squared += longest * longest;
        }

        if n == 0 {
            return Stats {
                num_people: 0,
                count_no_data: 0,
                rate_mean: f64::NAN,
                rate_sd: f64::NAN,
                rate_25_percentile: f64::NAN,
                rate_50_percentile: f64::NAN,
                rate_75_percentile: f64::NAN,
                longest_mean: f64::NAN,
                longest_sd: f64::NAN,
                longest_median: f64::NAN,
            };
        }

        let denom = n as f64;
        let rate_mean = rate_sum / denom;
        let rate_square_mean = rate_sum_squared / denom;
        let rate_sd = (rate_square_mean - rate_mean * rate_mean).sqrt();

        patient_rates.sort_by(sort_f64);
        patient_longest_gaps.sort_by(sort_f64);

        let rate_25_percentile = patient_rates[percentile_to_rank(0.25, n)];
        let rate_50_percentile = patient_rates[percentile_to_rank(0.5, n)];
        let rate_75_percentile = patient_rates[percentile_to_rank(0.75, n)];

        let longest_mean = longest_sum / denom;
        let longest_square_mean = longest_sum_squared / denom;
        let longest_sd = (longest_square_mean - longest_mean * longest_mean).sqrt();
        let longest_50_percentile = patient_longest_gaps[percentile_to_rank(0.5, n)];

        Stats {
            num_people: n,
            rate_mean,
            rate_sd,
            rate_25_percentile,
            rate_50_percentile,
            rate_75_percentile,
            longest_mean,
            longest_sd,
            longest_median: longest_50_percentile,
            count_no_data,
        }
    }
}

/// Gives the biggest gap between events, a start date, and an end date.
fn biggest_gap<'a>(
    start_date: NaiveDate,
    end_date: NaiveDate,
    events: impl Iterator<Item = &'a Event> + 'a,
) -> Duration {
    let dates = events
        .map(|evt| evt.date)
        .filter(|date| start_date <= *date && *date <= end_date);
    let mut dates = iter::once(start_date)
        .chain(dates)
        .chain(iter::once(end_date))
        .collect::<Vec<_>>();
    dates.sort();
    if dates.is_empty() {
        return end_date - start_date;
    }
    // Cannot panic as `dates` has at least 2 elements.
    dates
        .array_windows()
        .map(|[prev, next]| *next - *prev)
        .max()
        .unwrap()
}

#[derive(Debug, Serialize)]
struct Stats {
    /// Total people in the denominator
    num_people: usize,
    /// The average number of coded events per year
    rate_mean: f64,
    /// Standard deviation for `rate_mean`
    rate_sd: f64,
    /// The 25th percentile rate
    rate_25_percentile: f64,
    /// The 50th percentile rate
    rate_50_percentile: f64,
    /// The 75th percentile rate
    rate_75_percentile: f64,
    /// The average longest gap between coded events, in years
    longest_mean: f64,
    /// The standard deviation for `longest_mean`
    longest_sd: f64,
    /// The average (median) longest gap between coded events, in years
    longest_median: f64,
    /// How many people had no events.
    count_no_data: usize,
}

impl Stats {
    fn data_table(&self) -> Table<'_> {
        Table::new()
            .with_row(self.row("Total people with prerequisite treatment", self.num_people))
            .with_row(self.row(
                "Total people with prerequisite treatment who have at least 1 test",
                self.num_people - self.count_no_data,
            ))
            .with_row(self.row(
                "Mean test rate",
                format_args!("{:.1} per year", &self.rate_mean),
            ))
            .with_row(self.row(
                "SD test rate",
                format_args!("{:.1} per year", &self.rate_sd),
            ))
            .with_row(self.row(
                "25th percentile test rate",
                format_args!("{:.1} per year", &self.rate_25_percentile),
            ))
            .with_row(self.row(
                "50th percentile test rate",
                format_args!("{:.1} per year", &self.rate_50_percentile),
            ))
            .with_row(self.row(
                "75th percentile test rate",
                format_args!("{:.1} per year", &self.rate_75_percentile),
            ))
            .with_row(self.row(
                "Mean longest gap between tests",
                format_args!("{:.1} years", &self.longest_mean),
            ))
            .with_row(self.row(
                "SD longest gap between tests",
                format_args!("{:.1} years", &self.longest_sd),
            ))
            .with_row(self.row(
                "Median longest gap between tests",
                format_args!("{:.1} years", &self.longest_median),
            ))
    }

    fn row<'any>(&self, label: &'static str, value: impl fmt::Display + 'any) -> Row<'_> {
        Row::new().with_cell(label).with_cell(value.to_string())
    }
}

fn percentile_to_rank(proportion: f64, n: usize) -> usize {
    assert!(0. <= proportion && proportion <= 1.);
    let rank = (proportion * (n as f64 + 1.)) as usize;
    assert!(rank >= 1 && rank <= n);
    rank - 1
}

fn sort_f64(left: &f64, right: &f64) -> Ordering {
    if !(left.is_finite() && right.is_finite()) {
        panic!("only finite numbers expected");
    }
    if left < right {
        Ordering::Less
    } else if left == right {
        Ordering::Equal
    } else if left > right {
        Ordering::Greater
    } else {
        unreachable!()
    }
}
