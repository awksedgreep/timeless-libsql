use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::{BTreeMap, HashMap};

use serde::Deserialize;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Series {
    name: String,
    labels: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug)]
struct Point {
    series: u32,
    timestamp_seconds: i64,
    value_bits: u64,
}

#[derive(Debug, Default)]
pub(crate) struct VictoriaBatch {
    series: Vec<Series>,
    points: Vec<Point>,
    pub(crate) errors: usize,
}

#[derive(Deserialize)]
struct VictoriaLine {
    metric: BTreeMap<String, String>,
    values: Vec<f64>,
    timestamps: Vec<i64>,
}

impl VictoriaBatch {
    pub(crate) fn point_count(&self) -> usize {
        self.points.len()
    }

    pub(crate) fn encode(self) -> Result<Vec<u8>, String> {
        let series_count = u32::try_from(self.series.len())
            .map_err(|_| "VictoriaMetrics request exceeds u32::MAX series")?;
        let point_count = u32::try_from(self.points.len())
            .map_err(|_| "VictoriaMetrics request exceeds u32::MAX points")?;
        let point_bytes =
            self.points.len().checked_mul(20).ok_or_else(|| {
                "VictoriaMetrics batch allocation overflows this host".to_string()
            })?;
        let mut encoded_series = Vec::with_capacity(self.series.len());
        let mut series_bytes = 0_usize;
        for series in &self.series {
            let name = series.name.as_bytes();
            let labels = if series.labels.is_empty() {
                Vec::new()
            } else {
                let labels: BTreeMap<&str, &str> = series
                    .labels
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect();
                serde_json::to_vec(&labels)
                    .map_err(|error| format!("encode VictoriaMetrics labels: {error}"))?
            };
            u32::try_from(name.len())
                .map_err(|_| "VictoriaMetrics metric name exceeds u32::MAX bytes")?;
            u32::try_from(labels.len())
                .map_err(|_| "VictoriaMetrics labels exceed u32::MAX bytes")?;
            series_bytes = series_bytes
                .checked_add(8 + name.len() + labels.len())
                .ok_or_else(|| "VictoriaMetrics series table overflows this host".to_string())?;
            encoded_series.push((name, labels));
        }
        let capacity = 12_usize
            .checked_add(series_bytes)
            .and_then(|size| size.checked_add(point_bytes))
            .ok_or_else(|| "VictoriaMetrics batch allocation overflows this host".to_string())?;
        let mut blob = Vec::with_capacity(capacity);
        blob.push(0x01);
        blob.push(0);
        blob.extend_from_slice(&0_u16.to_le_bytes());
        blob.extend_from_slice(&series_count.to_le_bytes());
        blob.extend_from_slice(&point_count.to_le_bytes());
        for (name, labels) in encoded_series {
            blob.extend_from_slice(&(name.len() as u32).to_le_bytes());
            blob.extend_from_slice(name);
            blob.extend_from_slice(&(labels.len() as u32).to_le_bytes());
            blob.extend_from_slice(&labels);
        }
        for point in &self.points {
            blob.extend_from_slice(&point.series.to_le_bytes());
        }
        for point in &self.points {
            blob.extend_from_slice(&point.timestamp_seconds.to_le_bytes());
        }
        for point in &self.points {
            blob.extend_from_slice(&point.value_bits.to_le_bytes());
        }
        debug_assert_eq!(blob.len(), capacity);
        Ok(blob)
    }
}

pub(crate) fn parse(body: &[u8]) -> VictoriaBatch {
    let mut batch = VictoriaBatch::default();
    let mut series_index: HashMap<Series, u32> = HashMap::new();
    for line in body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let Ok(mut line) = serde_json::from_slice::<VictoriaLine>(line) else {
            batch.errors = batch.errors.saturating_add(1);
            continue;
        };
        if line.values.len() != line.timestamps.len() {
            batch.errors = batch.errors.saturating_add(1);
            continue;
        }
        if line.values.is_empty() {
            continue;
        }
        let name = match line.metric.entry("__name__".to_string()) {
            BTreeEntry::Occupied(entry) => entry.remove(),
            BTreeEntry::Vacant(_) => "unknown".to_string(),
        };
        let key = Series {
            name,
            labels: line.metric.into_iter().collect(),
        };
        let series = match series_index.get(&key) {
            Some(series) => *series,
            None => {
                let Ok(series) = u32::try_from(batch.series.len()) else {
                    batch.errors = batch.errors.saturating_add(1);
                    continue;
                };
                series_index.insert(key.clone(), series);
                batch.series.push(key);
                series
            }
        };
        batch
            .points
            .extend(
                line.timestamps
                    .into_iter()
                    .zip(line.values)
                    .map(|(timestamp, value)| Point {
                        series,
                        timestamp_seconds: timestamp / 1_000,
                        value_bits: value.to_bits(),
                    }),
            );
    }
    batch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_body_normalizes_units_defaults_names_and_counts_lines() {
        let body = br#"{"metric":{"__name__":"cpu","host":"a"},"values":[1,2.5],"timestamps":[1700000000123,-1999]}
{"metric":{"host":"b"},"values":[3],"timestamps":[1700000002000]}
{"metric":
{"metric":{"__name__":"bad"},"values":[1],"timestamps":[]}
"#;
        let batch = parse(body);
        assert_eq!(batch.errors, 2);
        assert_eq!(batch.series.len(), 2);
        assert_eq!(batch.point_count(), 3);
        assert_eq!(batch.series[0].name, "cpu");
        assert_eq!(batch.series[0].labels, vec![("host".into(), "a".into())]);
        assert_eq!(batch.series[1].name, "unknown");
        assert_eq!(batch.points[0].timestamp_seconds, 1_700_000_000);
        assert_eq!(batch.points[1].timestamp_seconds, -1);
        assert_eq!(f64::from_bits(batch.points[2].value_bits), 3.0);
    }

    #[test]
    fn encoder_is_the_public_named_columnar_batch() {
        let batch = parse(
            br#"{"metric":{"__name__":"cpu","env":"prod"},"values":[1.5,2.5],"timestamps":[1000,2000]}
{"metric":{"__name__":"cpu","env":"prod"},"values":[3.5],"timestamps":[3000]}"#,
        );
        assert_eq!(batch.errors, 0);
        let blob = batch.encode().unwrap();
        assert_eq!(&blob[..4], &[1, 0, 0, 0]);
        assert_eq!(u32::from_le_bytes(blob[4..8].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(blob[8..12].try_into().unwrap()), 3);

        let name_len = u32::from_le_bytes(blob[12..16].try_into().unwrap()) as usize;
        assert_eq!(&blob[16..16 + name_len], b"cpu");
        let labels_len_at = 16 + name_len;
        let labels_len =
            u32::from_le_bytes(blob[labels_len_at..labels_len_at + 4].try_into().unwrap()) as usize;
        let columns_at = labels_len_at + 4 + labels_len;
        assert_eq!(&blob[labels_len_at + 4..columns_at], br#"{"env":"prod"}"#);
        assert_eq!(
            &blob[columns_at..columns_at + 12],
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            i64::from_le_bytes(blob[columns_at + 12..columns_at + 20].try_into().unwrap()),
            1
        );
    }

    #[test]
    fn empty_and_empty_array_bodies_encode_as_a_valid_zero_point_batch() {
        for body in [
            b"".as_slice(),
            br#"{"metric":{"__name__":"empty"},"values":[],"timestamps":[]}"#,
        ] {
            let batch = parse(body);
            assert_eq!(batch.point_count(), 0);
            assert_eq!(batch.errors, 0);
            assert_eq!(
                batch.encode().unwrap(),
                vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            );
        }
    }
}
