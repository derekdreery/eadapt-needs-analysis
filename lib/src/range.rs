use itertools::{EitherOrBoth, Itertools};
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, fmt};

/// Range where lower bound is inclusive, upper bound is exclusive or unbounded.
#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct Range<T>(T, Option<T>);

impl<T> Range<T>
where
    T: Ord,
{
    pub fn new(from: T, to: Option<T>) -> Self {
        if let Some(ref to) = to {
            if from >= *to {
                panic!("ranges must go from low to high")
            }
        }
        Range(from, to)
    }
    pub fn contains(&self, val: &T) -> bool {
        if let Some(end) = &self.1 {
            val >= &self.0 && val < end
        } else {
            val >= &self.0
        }
    }
}

impl<T> Range<T> {
    pub fn as_ref(&self) -> Range<&T> {
        Range(&self.0, self.1.as_ref())
    }
}

impl<T> fmt::Display for Range<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(end) = &self.1 {
            write!(f, "{} - {}", self.0, end)
        } else {
            write!(f, "{}+", self.0)
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RangeSet<T> {
    ranges: Vec<Range<T>>,
}

impl<T> RangeSet<T> {
    pub fn new(ranges: Vec<Range<T>>) -> Self {
        Self { ranges }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Range<T>> + '_ {
        self.ranges.iter()
    }

    pub fn push(&mut self, range: Range<T>) {
        self.ranges.push(range);
    }
}

impl<T> RangeSet<T>
where
    T: Ord,
{
    pub fn bucket_values<I, B>(self, values: I) -> RangeSetCounts<T>
    where
        I: Iterator<Item = B>,
        B: Borrow<T>,
    {
        let mut buckets = vec![0usize; self.ranges.len()];
        for value in values {
            for (idx, bucket) in self.ranges.iter().enumerate() {
                if bucket.contains(value.borrow()) {
                    buckets[idx] += 1;
                }
            }
        }
        RangeSetCounts {
            set: self,
            counts: buckets,
        }
    }

    pub fn bucket_values_with_missing<I, B>(self, values: I) -> RangeSetCountsWithMissing<T>
    where
        I: Iterator<Item = Option<B>>,
        B: Borrow<T>,
    {
        let mut buckets = vec![0usize; self.ranges.len() + 1];
        let last = self.ranges.len();
        for value in values {
            if let Some(value) = value {
                for (idx, bucket) in self.ranges.iter().enumerate() {
                    if bucket.contains(value.borrow()) {
                        buckets[idx] += 1;
                    }
                }
            } else {
                buckets[last] += 1;
            }
        }
        RangeSetCountsWithMissing {
            set: self,
            counts: buckets,
        }
    }
}

/// A range set with values bucketed, and bucket sizes recorded.
pub struct RangeSetCounts<T> {
    set: RangeSet<T>,
    counts: Vec<usize>,
}

impl<T> RangeSetCounts<T> {
    pub fn iter(&self) -> impl Iterator<Item = (&Range<T>, usize)> {
        self.set.iter().zip_eq(self.counts.iter().copied())
    }
}

/// A range set with values bucketed, and bucket sizes recorded.
pub struct RangeSetCountsWithMissing<T> {
    set: RangeSet<T>,
    counts: Vec<usize>,
}

impl<T> RangeSetCountsWithMissing<T> {
    pub fn iter(&self) -> impl Iterator<Item = (Option<&Range<T>>, usize)> {
        self.set
            .iter()
            .zip_longest(self.counts.iter().copied())
            .map(|el| match el {
                EitherOrBoth::Left(_) => unreachable!(),
                EitherOrBoth::Right(count) => (None, count),
                EitherOrBoth::Both(range, count) => (Some(range), count),
            })
    }
}

impl<T> RangeSetCountsWithMissing<T>
where
    T: fmt::Display,
{
    pub fn for_display(&self) -> impl Iterator<Item = (&dyn fmt::Display, usize)> {
        self.iter().map(|(range, count)| {
            let range = match range {
                Some(range) => range,
                None => &"missing data" as &dyn fmt::Display,
            };
            (range, count)
        })
    }
}
