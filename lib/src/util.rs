use crate::{ArcStr, Imd, ReadCode};
use chrono::{NaiveDate, NaiveDateTime, Timelike};
use serde::{de, Deserialize, Deserializer};
use std::{collections::BTreeSet, fs, io, path::Path};
//use parking_lot::Mutex;
use once_cell::sync::Lazy;
use std::{
    borrow::Cow,
    cell::{Cell, RefCell, RefMut},
    fmt,
    fmt::Write,
};

/// The default maximum number of rows displayed.
pub const DEFAULT_MAX_ROWS: usize = 100;

/// Converts a not found error to Ok(false)
pub fn path_exists(path: &Path) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if matches!(e.kind(), io::ErrorKind::NotFound) => Ok(false),
        Err(e) => Err(e),
    }
}

// Helpers for serde to parse fields with quirks.

/// parse the index of multiple deprivation score, mapping 'null' to `None`.
pub fn imd<'de, D>(d: D) -> Result<Imd, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(d)?;
    if s.eq_ignore_ascii_case("null") || s.is_empty() {
        Ok(Imd::Missing)
    } else if let Ok(v) = s.parse::<f32>() {
        if v != v.floor() || v < 0. || v > 10. || !v.is_finite() {
            Err(de::Error::custom("invalid value"))
        } else {
            Ok(match v.floor() as u8 {
                1 => Imd::_1,
                2 => Imd::_2,
                3 => Imd::_3,
                4 => Imd::_4,
                5 => Imd::_5,
                6 => Imd::_6,
                7 => Imd::_7,
                8 => Imd::_8,
                9 => Imd::_9,
                10 => Imd::_10,
                _ => return Err(de::Error::custom("invalid value")),
            })
        }
    } else {
        Err(de::Error::custom("invalid value"))
    }
}

pub fn maybe_read<'de, D>(d: D) -> Result<Option<ReadCode>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &[u8] = Deserialize::deserialize(d)?;
    if let Ok(code) = ReadCode::from_bytes(s) {
        Ok(Some(code))
    } else {
        Ok(None)
    }
}

/// Parse a string, but map "null" to `None` (in addition to the default "" -> None mapping)
pub fn optional_string<'de, D>(d: D) -> Result<Option<ArcStr>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(d)?;
    if s.eq_ignore_ascii_case("null") || s.is_empty() {
        Ok(None)
    } else {
        Ok(Some(s.into()))
    }
}

/// parse a '1' to `true` and a '0' to `false`
pub fn bool_01<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let s: u8 = Deserialize::deserialize(d)?;
    match s {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::custom("expected '0' or '1'")),
    }
}

/// Parse a date with the format used in the adapt dataset (dd/mm/yyyy hh:mm:ss).
///
/// The time part is always 0. This is checked and an error is returned if it is not the case.
pub fn adapt_date<'de, D>(d: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let s: &str = Deserialize::deserialize(d)?;
    let datetime = NaiveDateTime::parse_from_str(s, "%d/%m/%Y %H:%M:%S")
        .map_err(|e| Error::custom(format!("{}", e)))?;
    if datetime.hour() != 0 || datetime.minute() != 0 || datetime.second() != 0 {
        return Err(Error::custom(format!(
            "non-zero time: {}:{}:{}",
            datetime.hour(),
            datetime.minute(),
            datetime.second()
        )));
    }
    Ok(datetime.date())
}

/// Like `adapt_date`, but maps the empty string to `None`.
pub fn opt_adapt_date<'de, D>(d: D) -> Result<Option<NaiveDate>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let s: &str = Deserialize::deserialize(d)?;
    if s.is_empty() {
        return Ok(None);
    }
    let datetime = NaiveDateTime::parse_from_str(s, "%d/%m/%Y %H:%M:%S")
        .map_err(|e| Error::custom(format!("{}", e)))?;
    if datetime.hour() != 0 || datetime.minute() != 0 || datetime.second() != 0 {
        return Err(Error::custom(format!(
            "non-zero time: {}:{}:{}",
            datetime.hour(),
            datetime.minute(),
            datetime.second()
        )));
    }
    Ok(Some(datetime.date()))
}

// Something to make it easier to get custom data on screen

/* TODO this doesn't seem to work with evcxr.
static DEFAULT_MAX_ROWS: Mutex<usize> = Mutex::new(100);

pub fn set_default_table_max_rows(new_max_rows: usize) {
    *DEFAULT_MAX_ROWS.lock() = constrain_max_rows(new_max_rows);
}
*/

pub struct RowDrawer<'a> {
    output: &'a mut String,
    scratch: &'a mut String,
}

impl<'a> RowDrawer<'a> {
    fn cell(&mut self, content: impl fmt::Display) {
        self.output.push_str("<td>");
        self.scratch.clear();
        let _ = write!(self.scratch, "{}", content);
        html_escape::encode_text_to_string(&mut self.scratch, self.output);
        self.output.push_str("</td>");
    }
}

pub trait RowForDisplay {
    fn draw(&self, drawer: RowDrawer<'_>);
}

macro_rules! row_for_display_tuple {
    () => {};

    ($first_ty:ident $($rest_ty:ident)*) => {
        impl<$first_ty, $($rest_ty,)*> RowForDisplay for ($first_ty, $($rest_ty),*)
            where $first_ty: ::std::fmt::Display,
                  $(
                      $rest_ty: ::std::fmt::Display,
                  )*
        {
            fn draw(&self, mut drawer: RowDrawer<'_>) {
                #[allow(non_snake_case)]
                let (
                    ref $first_ty,
                    $(
                        ref $rest_ty
                    ),*
                ) = &self;
                drawer.cell($first_ty);
                $(
                    drawer.cell($rest_ty);
                )*
            }
        }

        row_for_display_tuple!($($rest_ty)*);
    };
}

row_for_display_tuple!(D1 D2 D3 D4 D5 D6 D7 D8 D9 D10);

impl<D: fmt::Display, const N: usize> RowForDisplay for [D; N] {
    fn draw(&self, mut drawer: RowDrawer<'_>) {
        for cell in self.iter() {
            drawer.cell(cell);
        }
    }
}

/// An object that can display itself nicely as a table in evcxr.
pub struct Table<Row, I, DR> {
    headers: Option<Vec<Cow<'static, str>>>,
    title: Option<Cow<'static, str>>,
    row_fn: Box<dyn Fn(&Row, usize) -> DR>,
    data: RefCell<I>,
    /// must be even - enforced by setter and `new`.
    max_rows: Option<usize>,
    col_count: Cell<Option<usize>>,
    completed: Cell<bool>,
}

impl<Row, I, DR> Table<Row, I, DR>
where
    I: ExactSizeIterator + Iterator<Item = Row>,
    DR: RowForDisplay,
{
    /// Create a new headerless table from a slice of row data and a function showing how to map
    /// that data to cells.
    pub fn new(
        data: impl IntoIterator<IntoIter = I>,
        row_fn: impl Fn(&Row, usize) -> DR + 'static,
    ) -> Self {
        Table {
            headers: None,
            title: None,
            row_fn: Box::new(row_fn),
            data: RefCell::new(data.into_iter()),
            max_rows: None,
            col_count: Cell::new(None),
            completed: Cell::new(false),
        }
    }

    pub fn with_headers(
        mut self,
        headers: impl IntoIterator<Item = impl Into<Cow<'static, str>>>,
    ) -> Self {
        self.headers = Some(headers.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_title(mut self, title: impl Into<Cow<'static, str>>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set the maximum number of rows to show
    pub fn set_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = Some(constrain_max_rows(max_rows));
        self
    }

    /// Display this table as HTML in the evcxr window.
    pub fn evcxr_display(&self) {
        let iter = self.data.borrow_mut();
        if self.completed.replace(true) {
            panic!(
                "Tables are used once. Please recreate the table for each display \
                   (they are cheap to create)"
            );
        }

        // buffer our output so we only draw something when there's no error
        let mut output = if let Some(title) = &self.title {
            let mut output =
                String::from(r#"<p style="font-weight:bold;font-variant:small-caps;">"#);
            html_escape::encode_text_to_string(title, &mut output);
            output.push_str("</p>");
            output
        } else {
            String::from("")
        };

        output.push_str("<table>");
        if let Some(headers) = &self.headers {
            self.col_count.set(Some(headers.len()));
            output.push_str("<thead><tr><th></th>");
            for header in headers {
                output.push_str("<th>");
                html_escape::encode_text_to_string(header, &mut output);
                output.push_str("</th>");
            }
            output.push_str("</tr></thead>");
        } else {
            self.col_count.set(None);
        }

        output.push_str("<tbody>");
        self.write_body(iter, &mut output);
        output.push_str("</tbody></table>");

        println!(
            "EVCXR_BEGIN_CONTENT text/html\n{}\nEVCXR_END_CONTENT",
            output
        );
    }

    fn write_body(&self, iter: RefMut<'_, I>, output: &mut String) {
        if iter.len() == 0 {
            return;
        }
        let max_rows = self.max_rows.unwrap_or_else(|| DEFAULT_MAX_ROWS);
        self.write_some_rows(iter, max_rows, output);
    }

    fn write_some_rows(&self, mut iter: RefMut<'_, I>, max_rows: usize, output: &mut String) {
        let len = iter.len();
        if max_rows == 0 || max_rows >= len {
            return self.write_rows(&mut *iter, 0, len, output);
        }

        let window_len = max_rows / 2;
        self.write_rows(&mut *iter, 0, window_len, output);
        output.push_str("<tr><th>...</th>");
        if let Some(headers) = &self.headers {
            for _ in 0..headers.len() {
                output.push_str("<td>...</td>");
            }
        }
        output.push_str("</tr>");

        // skip middle records
        let skip_len = len - 2 * window_len;
        // TODO use advance_by when stable.
        for _ in 0..skip_len {
            let _ = iter.next();
        }
        self.write_rows(&mut *iter, skip_len + window_len, len, output);
    }

    fn write_rows(
        &self,
        mut rows: impl Iterator<Item = Row>,
        start: usize,
        count: usize,
        output: &mut String,
    ) {
        let mut scratch = String::new();
        for idx in start..count {
            let row = rows.next().expect("internal inconsistency in Table");
            let _ = write!(output, "<tr><th>{}</th>", idx);
            let drawer = RowDrawer {
                output,
                scratch: &mut scratch,
            };
            let to_draw = (self.row_fn)(&row, idx);
            to_draw.draw(drawer);
            output.push_str("</tr>");
        }
    }
}

/*
#[test]
fn test_table() {
    let table = Table::new(
        &[["one", "two"], ["three", "four"]],
        |row: &[&'static str; 2], _| row.iter(),
    )
    .headers(&["some", "headers"]);
    table.evcxr_display();
}
*/

// error printing helper.
//
pub trait ResultExt {
    fn print_error(self) -> Self;
}

impl<T> ResultExt for Result<T, anyhow::Error> {
    fn print_error(self) -> Self {
        match self {
            Ok(v) => Ok(v),
            Err(error) => {
                println!("error decoding events: {}", error);
                let mut err: &dyn std::error::Error = error.as_ref();
                while let Some(cause) = err.source() {
                    println!("caused by: {}", cause);
                    err = cause;
                }
                Err(error)
            }
        }
    }
}

fn constrain_max_rows(mut max_rows: usize) -> usize {
    // make sure 0 -> 0, true since we only touch odd numbers
    if max_rows % 2 == 1 {
        if max_rows == 1 {
            max_rows = 2;
        } else {
            max_rows -= 1;
        }
    }
    max_rows
}

pub fn header(header: &str) {
    let len = header.len();
    print!("\n{}\n", header);
    for _ in 0..len {
        print!("=");
    }
    println!("\n")
}

pub(crate) static EMPTY_DESC: Lazy<BTreeSet<ArcStr>> = Lazy::new(|| BTreeSet::new());
