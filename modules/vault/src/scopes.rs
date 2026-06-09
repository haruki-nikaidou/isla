//! Scope-based permission system for vault secrets.
//!
//! This module provides a glob-like pattern matching system for controlling
//! access to secrets. Scopes are hierarchical, dot-separated identifiers
//! (e.g., `@isla.memory_repository.object_storage`), and scope ranges are
//! patterns that can match one or more scopes using wildcards.
//!
//! System scopes (prefixed with `@isla`) are reserved for internal Isla
//! services.

use std::fmt::Display;
use compact_str::CompactString;
use smallvec::{SmallVec, smallvec};
use std::str::FromStr;
use std::sync::LazyLock;

/// A pattern that matches zero or more [`Scope`]s.
///
/// Scope ranges support glob-like syntax for flexible permission grants:
/// - Literal segments match exactly (e.g., `foo` matches only `foo`)
/// - `*` matches exactly one segment
/// - `**` matches zero or more segments
/// - `+` matches one or more segments
///
/// Patterns are parsed from dot-separated strings (e.g., `@isla.**.read`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeRange(pub SmallVec<[ScopeRangeSection; 3]>);

impl ScopeRange {
    /// O(m·n) DP match, where m = pattern sections, n = scope segments.
    ///
    /// `dp[j]` = "can pattern\[0..i] match scope\[0..j]" after processing i sections.
    /// Each section type is a simple in-place linear scan — no recursion, no branching
    /// explosion from consecutive wildcards.
    pub fn matches(&self, scope: Scope) -> bool {
        let segs = scope.0;
        let n = segs.len();

        // SmallVec avoids heap allocation for typical short scopes.
        let mut dp: SmallVec<[bool; 8]> = smallvec![false; n + 1];
        dp[0] = true;

        for section in self.0.iter() {
            match section {
                ScopeRangeSection::Wildcard => {
                    // `**` matches zero-or-more: dp[j] |= dp[j-1]  (left-to-right)
                    // dp[0] is unchanged (zero-match of previous state).
                    for j in 1..=n {
                        dp[j] |= dp[j - 1];
                    }
                }
                ScopeRangeSection::OneOrMore => {
                    // `+` matches one-or-more: dp_new[j] = dp_old[j-1] | dp_new[j-1]
                    // dp_new[0] = false (need at least one segment).
                    // Carry `prev_old` to track dp_old[j-1] while updating in-place.
                    let mut prev_old = dp[0];
                    dp[0] = false;
                    for j in 1..=n {
                        let old_j = dp[j];
                        dp[j] = prev_old | dp[j - 1];
                        prev_old = old_j;
                    }
                }
                ScopeRangeSection::AnyOne => {
                    // `*` matches exactly one segment: dp_new[j] = dp_old[j-1]
                    // Right-to-left shift; dp[0] = false.
                    for j in (1..=n).rev() {
                        dp[j] = dp[j - 1];
                    }
                    dp[0] = false;
                }
                ScopeRangeSection::Lit(l) => {
                    // Literal: dp_new[j] = dp_old[j-1] & (segs[j-1] == l)
                    // Right-to-left shift; dp[0] = false.
                    for j in (1..=n).rev() {
                        dp[j] = dp[j - 1] && l == segs[j - 1];
                    }
                    dp[0] = false;
                }
            }
        }

        dp[n]
    }
}

/// Error returned when parsing a [`ScopeRange`] from a string fails.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ScopeParseError {
    /// A segment in the pattern was empty (e.g., `foo..bar` has an empty segment at index 1).
    #[error("Expect a non-empty string at {0}, get an empty string")]
    Empty(usize),
}

impl FromStr for ScopeRange {
    type Err = ScopeParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sections: SmallVec<[ScopeRangeSection; 3]> = s
            .split('.')
            .enumerate()
            .map(|(i, part)| {
                if part.is_empty() {
                    return Err(ScopeParseError::Empty(i));
                }
                Ok(match part {
                    "**" => ScopeRangeSection::Wildcard,
                    "*" => ScopeRangeSection::AnyOne,
                    "+" => ScopeRangeSection::OneOrMore,
                    lit => ScopeRangeSection::Lit(CompactString::from(lit)),
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(ScopeRange(sections))
    }
}

/// A single segment within a [`ScopeRange`] pattern.
///
/// Each section represents one dot-separated component of a scope range
/// and defines how that component matches against scope segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeRangeSection {
    /// A literal string that must match exactly.
    Lit(CompactString),

    /// `*` matches exactly one segment (any value).
    AnyOne,

    /// `**` matches zero or more segments.
    Wildcard,

    /// `+` matches one or more segments.
    OneOrMore,
}

/// Matches all system-reserved scopes (those starting with `@isla`).
pub static SYSTEM_SCOPE_RANGE: LazyLock<ScopeRange> = LazyLock::new(|| {
    ScopeRange(smallvec![
        ScopeRangeSection::Lit(CompactString::const_new("@isla")),
        ScopeRangeSection::Wildcard,
    ])
});

/// A concrete permission scope represented as a static slice of segments.
///
/// Scopes are hierarchical identifiers used to control access to secrets.
/// They are matched against [`ScopeRange`] patterns to determine whether
/// a particular operation is permitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scope(pub &'static [&'static str]);

impl Scope {
    /// Returns `true` if this scope is a system scope (starts with `@isla`).
    pub fn is_system_scope(self) -> bool {
        SYSTEM_SCOPE_RANGE.matches(self)
    }
}

impl Display for Scope {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

/// Scope for accessing the object storage subsystem in the memory repository.
pub static OBJECT_STORAGE_SCOPE: Scope = Scope(&["@isla", "memory_repository", "object_storage"]);

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a ScopeRange from a dot-separated pattern string.
    fn range(s: &str) -> ScopeRange {
        s.parse().expect("valid pattern")
    }

    // Helper: build a Scope from a dot-separated scope string.
    // Uses leaked statics so we can call matches in tests without hard-coding consts.
    fn scope(s: &'static str) -> Scope {
        let parts: &'static [&'static str] =
            Box::leak(s.split('.').collect::<Vec<_>>().into_boxed_slice());
        Scope(parts)
    }

    // ── Lit ──────────────────────────────────────────────────────────────────

    #[test]
    fn lit_exact_match() {
        assert!(range("foo").matches(scope("foo")));
    }

    #[test]
    fn lit_mismatch() {
        assert!(!range("foo").matches(scope("bar")));
    }

    #[test]
    fn lit_multi_segment_match() {
        assert!(range("foo.bar.baz").matches(scope("foo.bar.baz")));
    }

    #[test]
    fn lit_multi_segment_mismatch() {
        assert!(!range("foo.bar.baz").matches(scope("foo.bar.qux")));
    }

    #[test]
    fn lit_too_short_scope() {
        assert!(!range("foo.bar").matches(scope("foo")));
    }

    #[test]
    fn lit_too_long_scope() {
        assert!(!range("foo").matches(scope("foo.bar")));
    }

    // ── AnyOne (*) ───────────────────────────────────────────────────────────

    #[test]
    fn any_one_matches_single_segment() {
        assert!(range("*").matches(scope("anything")));
    }

    #[test]
    fn any_one_does_not_match_empty() {
        assert!(!range("*").matches(Scope(&[])));
    }

    #[test]
    fn any_one_does_not_match_two_segments() {
        assert!(!range("*").matches(scope("a.b")));
    }

    #[test]
    fn any_one_in_prefix() {
        assert!(range("*.bar").matches(scope("foo.bar")));
        assert!(!range("*.bar").matches(scope("foo.baz")));
    }

    #[test]
    fn any_one_in_suffix() {
        assert!(range("foo.*").matches(scope("foo.anything")));
        assert!(!range("foo.*").matches(scope("bar.anything")));
    }

    // ── Wildcard (**) ────────────────────────────────────────────────────────

    #[test]
    fn wildcard_matches_empty() {
        assert!(range("**").matches(Scope(&[])));
    }

    #[test]
    fn wildcard_matches_one() {
        assert!(range("**").matches(scope("foo")));
    }

    #[test]
    fn wildcard_matches_many() {
        assert!(range("**").matches(scope("a.b.c.d.e")));
    }

    #[test]
    fn wildcard_as_prefix() {
        assert!(range("**.baz").matches(scope("baz")));
        assert!(range("**.baz").matches(scope("foo.baz")));
        assert!(range("**.baz").matches(scope("foo.bar.baz")));
        assert!(!range("**.baz").matches(scope("foo.bar")));
    }

    #[test]
    fn wildcard_as_suffix() {
        assert!(range("foo.**").matches(scope("foo")));
        assert!(range("foo.**").matches(scope("foo.bar")));
        assert!(range("foo.**").matches(scope("foo.bar.baz")));
        assert!(!range("foo.**").matches(scope("bar")));
    }

    #[test]
    fn wildcard_in_middle() {
        assert!(range("foo.**.baz").matches(scope("foo.baz")));
        assert!(range("foo.**.baz").matches(scope("foo.x.baz")));
        assert!(range("foo.**.baz").matches(scope("foo.x.y.baz")));
        assert!(!range("foo.**.baz").matches(scope("foo.x.y")));
    }

    #[test]
    fn consecutive_wildcards() {
        // Two back-to-back wildcards should still match anything.
        assert!(range("**.**").matches(Scope(&[])));
        assert!(range("**.**").matches(scope("a.b.c")));
    }

    // ── OneOrMore (+) ────────────────────────────────────────────────────────

    #[test]
    fn one_or_more_rejects_empty() {
        assert!(!range("+").matches(Scope(&[])));
    }

    #[test]
    fn one_or_more_matches_one() {
        assert!(range("+").matches(scope("foo")));
    }

    #[test]
    fn one_or_more_matches_many() {
        assert!(range("+").matches(scope("a.b.c")));
    }

    #[test]
    fn one_or_more_as_suffix() {
        assert!(!range("foo.+").matches(scope("foo")));
        assert!(range("foo.+").matches(scope("foo.bar")));
        assert!(range("foo.+").matches(scope("foo.bar.baz")));
    }

    #[test]
    fn one_or_more_in_middle() {
        assert!(!range("foo.+.baz").matches(scope("foo.baz")));
        assert!(range("foo.+.baz").matches(scope("foo.x.baz")));
        assert!(range("foo.+.baz").matches(scope("foo.x.y.baz")));
    }

    // ── Mixed patterns ───────────────────────────────────────────────────────

    #[test]
    fn lit_then_wildcard_then_lit() {
        let r = range("@isla.**.read");
        assert!(r.matches(scope("@isla.read")));
        assert!(r.matches(scope("@isla.docs.read")));
        assert!(r.matches(scope("@isla.a.b.c.read")));
        assert!(!r.matches(scope("@isla.a.b.c.write")));
        assert!(!r.matches(scope("other.read")));
    }

    #[test]
    fn any_one_and_wildcard_combined() {
        // *.** — one fixed segment followed by zero-or-more
        let r = ScopeRange(smallvec![
            ScopeRangeSection::AnyOne,
            ScopeRangeSection::Wildcard,
        ]);
        assert!(r.matches(scope("ns")));
        assert!(r.matches(scope("ns.a")));
        assert!(r.matches(scope("ns.a.b.c")));
        assert!(!r.matches(Scope(&[])));
    }

    // ── Edge cases ───────────────────────────────────────────────────────────

    #[test]
    fn empty_pattern_matches_empty_scope() {
        let r = ScopeRange(smallvec![]);
        assert!(r.matches(Scope(&[])));
    }

    #[test]
    fn empty_pattern_rejects_nonempty_scope() {
        let r = ScopeRange(smallvec![]);
        assert!(!r.matches(scope("foo")));
    }

    #[test]
    fn wildcard_only_matches_everything() {
        let r = range("**");
        for s in &["", "a", "a.b", "a.b.c.d.e.f"] {
            let sc = if s.is_empty() {
                Scope(&[])
            } else {
                scope(Box::leak(s.to_string().into_boxed_str()))
            };
            assert!(r.matches(sc), "should match {s:?}");
        }
    }

    // ── FromStr parsing ──────────────────────────────────────────────────────

    #[test]
    fn parse_all_token_types() {
        let r = range("foo.*.**.+.bar");
        assert_eq!(
            r.0.as_slice(),
            &[
                ScopeRangeSection::Lit(CompactString::const_new("foo")),
                ScopeRangeSection::AnyOne,
                ScopeRangeSection::Wildcard,
                ScopeRangeSection::OneOrMore,
                ScopeRangeSection::Lit(CompactString::const_new("bar")),
            ]
        );
    }

    #[test]
    fn parse_empty_segment_returns_error() {
        assert!(matches!(
            "foo..bar".parse::<ScopeRange>(),
            Err(ScopeParseError::Empty(1))
        ));
    }

    // ── Built-in constants ───────────────────────────────────────────────────

    #[test]
    fn system_scope_range_matches_isla_scopes() {
        assert!(OBJECT_STORAGE_SCOPE.is_system_scope());
        assert!(Scope(&["@isla", "anything"]).is_system_scope());
        assert!(Scope(&["@isla"]).is_system_scope()); // ** matches zero trailing
    }

    #[test]
    fn system_scope_range_rejects_non_isla() {
        assert!(!Scope(&["user", "read"]).is_system_scope());
        assert!(!Scope(&[]).is_system_scope());
    }
}
