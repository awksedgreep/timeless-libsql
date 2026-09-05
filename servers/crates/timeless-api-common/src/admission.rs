//! Bytes-based admission control for the ingest writer queues (#47 M5).
//!
//! The writer command channels bound queued *batches*; a 10 MiB body cap
//! per batch means worst-case queued memory is `queue_batches × 10 MiB`.
//! This gate bounds queued *bytes* instead: an admission acquires permits
//! proportional to its payload before entering the queue and releases
//! them when the writer has applied the batch.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// One permit unit, in bytes. Permits are integral, so bytes are rounded
/// up to whole units; 64 KiB keeps permit counts small while making the
/// granularity irrelevant next to real batch sizes.
const UNIT_BYTES: u64 = 64 * 1024;

/// Bounds the total payload bytes queued ahead of the writer.
#[derive(Clone)]
pub struct BytesGate {
    semaphore: Arc<Semaphore>,
    max_bytes: u64,
}

/// A held reservation. Drop releases the bytes back to the gate.
pub struct GatePermit {
    _permit: OwnedSemaphorePermit,
}

impl BytesGate {
    /// A gate allowing at most `max_bytes` of queued payload. Values
    /// below one unit still yield a workable gate of one unit.
    pub fn new(max_bytes: u64) -> Self {
        let units = u32::try_from(max_bytes.div_ceil(UNIT_BYTES))
            .unwrap_or(u32::MAX)
            .max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(units as usize)),
            max_bytes: units as u64 * UNIT_BYTES,
        }
    }

    /// Reserve `bytes` of queue space, waiting until they fit. A single
    /// batch larger than the whole gate is clamped to the full gate: it
    /// is admitted alone rather than deadlocking the producer.
    pub async fn acquire(&self, bytes: usize) -> GatePermit {
        // Zero-byte admissions (empty batches) need no queue space and
        // must never block, even on a full gate.
        if bytes == 0 {
            return GatePermit {
                _permit: self
                    .semaphore
                    .clone()
                    .acquire_many_owned(0)
                    .await
                    .expect("gate is never closed"),
            };
        }
        let requested = u64::try_from(bytes)
            .unwrap_or(u64::MAX)
            .div_ceil(UNIT_BYTES);
        let total = self.semaphore.available_permits() as u64;
        let units = u32::try_from(requested.min(total))
            .unwrap_or(u32::MAX)
            .max(1);
        GatePermit {
            _permit: self
                .semaphore
                .clone()
                .acquire_many_owned(units)
                .await
                .expect("gate is never closed"),
        }
    }

    /// The effective capacity in bytes (rounded up to whole units).
    pub fn capacity_bytes(&self) -> u64 {
        self.max_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    #[tokio::test]
    async fn oversized_batch_clamps_to_full_capacity_and_still_admits() {
        // M5 deadlock rule: a batch bigger than the gate must be
        // admitted alone, never rejected or stuck.
        let gate = BytesGate::new(64 * 1024); // one unit
        let permit = gate.acquire(10 * 1024 * 1024).await;
        drop(permit);
        let again = tokio::time::timeout(Duration::from_secs(1), gate.acquire(64 * 1024));
        assert!(again.await.is_ok(), "gate must refill after release");
    }

    #[tokio::test]
    async fn full_gate_blocks_until_a_holder_releases() {
        let gate = BytesGate::new(64 * 1024);
        let first = gate.acquire(64 * 1024).await;
        let gate = StdArc::new(gate);
        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.acquire(64 * 1024).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "waiter must block while full");
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("release must wake the waiter")
            .unwrap();
    }

    #[tokio::test]
    async fn zero_byte_admission_is_free() {
        let gate = BytesGate::new(64 * 1024);
        let _held = gate.acquire(64 * 1024).await;
        // Zero bytes needs no units even while the gate is full.
        let free = tokio::time::timeout(Duration::from_secs(1), gate.acquire(0));
        assert!(free.await.is_ok());
    }

    #[tokio::test]
    async fn concurrent_holders_share_the_capacity() {
        let gate = BytesGate::new(3 * 64 * 1024); // three units
        let a = gate.acquire(64 * 1024).await;
        let b = gate.acquire(64 * 1024).await;
        let c = gate.acquire(64 * 1024).await;
        let gate = StdArc::new(gate);
        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.acquire(64 * 1024).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());
        drop(a);
        drop(b);
        drop(c);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("full release must wake the waiter")
            .unwrap();
    }
}
