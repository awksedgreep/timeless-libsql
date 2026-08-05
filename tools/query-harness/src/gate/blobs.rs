use std::fs;
use std::path::PathBuf;

use anyhow::Result;

use super::{require_outputs, FixtureKind};

fn u16_le(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn u32_le(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn i64_le(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn f64_le(out: &mut Vec<u8>, value: f64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn framed(out: &mut Vec<u8>, value: &[u8]) {
    u32_le(out, value.len() as u32);
    out.extend_from_slice(value);
}

fn metrics_v0() -> Vec<u8> {
    let mut out = vec![1, 0];
    u16_le(&mut out, 0);
    u32_le(&mut out, 2);
    u32_le(&mut out, 3);
    framed(&mut out, b"cpu");
    framed(&mut out, br#"{"host":"a"}"#);
    framed(&mut out, b"mem");
    framed(&mut out, b"");
    for index in [0_u32, 0, 1] {
        u32_le(&mut out, index);
    }
    for timestamp in [100_i64, 200, 150] {
        i64_le(&mut out, timestamp);
    }
    for value in [1.5_f64, 2.5, 3.25] {
        f64_le(&mut out, value);
    }
    out
}

fn logs_v0() -> Vec<u8> {
    let mut out = vec![1, 0];
    u16_le(&mut out, 0);
    u32_le(&mut out, 3);
    for timestamp in [1000_i64, 1050, 2000] {
        i64_le(&mut out, timestamp);
    }
    out.extend_from_slice(&[1, 3, 1]);
    for message in [b"hello".as_slice(), b"boom", b"world"] {
        framed(&mut out, message);
    }
    for metadata in [
        br#"{"service":"api"}"#.as_slice(),
        b"",
        br#"{"service":"web"}"#,
    ] {
        framed(&mut out, metadata);
    }
    out
}

fn traces_v0() -> Vec<u8> {
    let mut out = vec![1, 0];
    u16_le(&mut out, 0);
    u32_le(&mut out, 2);
    out.extend_from_slice(&[1; 16]);
    out.extend_from_slice(&[2; 16]);
    out.extend_from_slice(&[3; 8]);
    out.extend_from_slice(&[4; 8]);
    out.extend_from_slice(&[0; 8]);
    out.extend_from_slice(&[5; 8]);
    for name in [b"op1".as_slice(), b"op2"] {
        framed(&mut out, name);
    }
    for service in [b"api".as_slice(), b"web"] {
        framed(&mut out, service);
    }
    out.extend_from_slice(&[1, 2, 1, 2]);
    for timestamp in [5000_i64, 6000] {
        i64_le(&mut out, timestamp);
    }
    for duration in [100_i64, 200] {
        i64_le(&mut out, duration);
    }
    for attributes in [br#"{"k":"v"}"#.as_slice(), b""] {
        framed(&mut out, attributes);
    }
    out
}

fn resolved_metrics_v1() -> Vec<u8> {
    let mut out = vec![2, 0];
    u16_le(&mut out, 0);
    u32_le(&mut out, 3);
    for series_id in [1_i64, 2, 1] {
        i64_le(&mut out, series_id);
    }
    for timestamp in [10_i64, 10, 20] {
        i64_le(&mut out, timestamp);
    }
    for value in [1.5_f64, 2.5, 3.5] {
        f64_le(&mut out, value);
    }
    out
}

fn metrics_nan_v0() -> Vec<u8> {
    let mut out = vec![1, 0];
    u16_le(&mut out, 0);
    u32_le(&mut out, 2);
    u32_le(&mut out, 5);
    for (name, labels) in [
        (b"nan_metric".as_slice(), br#"{"host":"mixed"}"#.as_slice()),
        (
            b"nan_metric".as_slice(),
            br#"{"host":"all-nan"}"#.as_slice(),
        ),
    ] {
        framed(&mut out, name);
        framed(&mut out, labels);
    }
    for index in [0_u32, 0, 0, 1, 1] {
        u32_le(&mut out, index);
    }
    for timestamp in [0_i64, 1, 2, 0, 1] {
        i64_le(&mut out, timestamp);
    }
    for value in [f64::NAN, 2.0, 4.0, f64::NAN, f64::NAN] {
        f64_le(&mut out, value);
    }
    out
}

pub(super) fn write(kind: FixtureKind, outputs: &[PathBuf]) -> Result<()> {
    match kind {
        FixtureKind::MetricsV0 => {
            require_outputs(outputs, 1, kind)?;
            fs::write(&outputs[0], metrics_v0())?;
        }
        FixtureKind::MetricsV0Truncated => {
            require_outputs(outputs, 1, kind)?;
            let mut value = metrics_v0();
            value.truncate(value.len() - 4);
            fs::write(&outputs[0], value)?;
        }
        FixtureKind::MetricsV0OutOfRange => {
            require_outputs(outputs, 1, kind)?;
            let mut out = vec![1, 0];
            u16_le(&mut out, 0);
            u32_le(&mut out, 1);
            u32_le(&mut out, 1);
            framed(&mut out, b"cpu");
            framed(&mut out, b"");
            u32_le(&mut out, 5);
            i64_le(&mut out, 1);
            f64_le(&mut out, 1.0);
            fs::write(&outputs[0], out)?;
        }
        FixtureKind::LogsTracesV0 => {
            require_outputs(outputs, 2, kind)?;
            fs::write(&outputs[0], logs_v0())?;
            fs::write(&outputs[1], traces_v0())?;
        }
        FixtureKind::LogsV0Malformed => {
            require_outputs(outputs, 7, kind)?;
            let blob = logs_v0();
            for (output, cut) in outputs[..6]
                .iter()
                .zip([3_usize, 7, 20, 33, 40, blob.len() - 1])
            {
                fs::write(output, &blob[..cut])?;
            }
            let mut bad = blob;
            bad[8 + 24] = 9;
            fs::write(&outputs[6], bad)?;
        }
        FixtureKind::ResolvedMetricsV1 => {
            require_outputs(outputs, 1, kind)?;
            fs::write(&outputs[0], resolved_metrics_v1())?;
        }
        FixtureKind::MetricsNanV0 => {
            require_outputs(outputs, 1, kind)?;
            fs::write(&outputs[0], metrics_nan_v0())?;
        }
    }
    Ok(())
}

pub(super) fn log_batch(start: i64, count: usize, step: i64) -> Vec<u8> {
    let mut out = vec![1, 0, 0, 0];
    u32_le(&mut out, count as u32);
    for index in 0..count {
        i64_le(&mut out, start + index as i64 * step);
    }
    out.extend(std::iter::repeat_n(1, count));
    for index in 0..count {
        framed(
            &mut out,
            format!("request {}", start + index as i64 * step).as_bytes(),
        );
    }
    for _ in 0..count {
        framed(&mut out, br#"{"service":"api"}"#);
    }
    out
}
