//! Reading a value back out of a Prometheus scrape.
//!
//! Tests assert on the rendered text rather than on an in-memory registry, and
//! that is deliberate: the rendered text is what Prometheus consumes, so it is the
//! only artefact where a wrong metric name, a missing label or a value that never
//! made it out of the exporter can actually be caught. A registry lookup would pass
//! for a metric no dashboard could ever find.
//!
//! Only as much of the exposition format as the tests need. A line is
//! `name{label="value",...} number`, with `#` for comments.

/// The value of one sample, or `None` if no line matches.
///
/// `labels` is a subset match: a sample carrying extra labels still matches, which
/// is what makes a test survive a label being added for an unrelated reason.
pub fn sample(rendered: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    for line in rendered.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // The value is last and never contains a space; the label set may contain
        // several, so splitting from the right is the only safe direction.
        let Some((key, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let (metric, label_text) = match key.split_once('{') {
            Some((metric, rest)) => (metric, rest.trim_end_matches('}')),
            None => (key, ""),
        };
        if metric != name {
            continue;
        }
        if labels
            .iter()
            .all(|(k, v)| label_text.contains(&format!("{k}=\"{v}\"")))
        {
            return value.parse().ok();
        }
    }
    None
}

/// The value of a sample, treating "not present yet" as zero.
///
/// Right for counters and wrong for gauges: a counter that has never been
/// incremented genuinely is zero, whereas an absent gauge means nobody has reported
/// it and zero would be an invention.
pub fn counter(rendered: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    sample(rendered, name, labels).unwrap_or(0.0)
}

/// Whether the scrape declares this metric at all, description included.
///
/// A metric that has been described appears with its `# HELP` line before anything
/// has reported into it, which is what lets a dashboard distinguish "zero" from
/// "this does not exist".
pub fn is_described(rendered: &str, name: &str) -> bool {
    rendered
        .lines()
        .any(|l| l.starts_with(&format!("# HELP {name} ")))
}
