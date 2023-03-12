//! A small query language.
use chrono::NaiveDate;
use qu::ick_use::*;
use regex::Regex;

pub enum Query {
    Expr(Expr),
    And(Box<Query>, Box<Query>),
    Or(Box<Query>, Box<Query>),
}

impl Query {
    pub fn parse(input: &str) -> Self {
        todo!()
    }
}

pub enum Expr {
    /// ==
    Eq,
    /// !=
    Neq,
    /// >
    Gt,
    /// >=
    Geq,
    /// <
    Lt,
    /// <=
    Leq,
    Like,
    RLike,
}

pub struct Field(String);

// Lexer

enum Tok {
    Field(String),
    Value(Value),
    Operator(Operator),
    /// (
    LRound,
    /// )
    RRound,
}

enum Operator {
    Eq,
    Neq,
    Gt,
    Geq,
    Lt,
    Leq,
    Like,
    RLike,
    And,
    Or,
}

pub enum Value {
    String(String),
    Number(f64),
    Date(NaiveDate),
    Regex(Regex),
}

struct Lexer<'a> {
    input: &'a str,
    input_start: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            input_start: 0,
        }
    }

    fn next(&mut self) -> Result<Option<Tok>> {
        todo!()
    }

    /// Discard 1 char from front
    fn advance(&mut self) {
        if let Some(ch) = self.input.chars().next() {
            let first_len = ch.len_utf8();
            self.input = &self.input[first_len..];
            self.input_start += first_len;
        }
    }
}
