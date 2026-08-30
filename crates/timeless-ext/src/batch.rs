//! Shared bounds-checked reader for the batch blob wire formats
//! (metrics v0 since the POC; logs/traces v0 added by F5). Every read
//! names what it was reading, so truncation errors point at the exact
//! field — these are public wire formats and their error messages are
//! part of the API.

use rusqlite::{Error, Result};

fn module_err(msg: String) -> Error {
    Error::ModuleError(msg)
}

pub(crate) struct BatchReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BatchReader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        BatchReader { buf, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Take exactly `n` bytes or fail with a message naming `what`.
    /// checked_add guards against a hostile length that would overflow
    /// usize arithmetic (u32 lengths can't overflow on 64-bit, but the
    /// habit is free and the compiler removes it when provably safe).
    pub(crate) fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| module_err(format!("batch blob: length overflow reading {what}")))?;
        if end > self.buf.len() {
            return Err(module_err(format!(
                "batch blob truncated: need {n} byte(s) for {what} at offset {}, \
                 but only {} remain",
                self.pos,
                self.remaining()
            )));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    /// Take a fixed-width column without allowing hostile element counts to
    /// wrap the byte length on narrower hosts.
    pub(crate) fn take_array(
        &mut self,
        count: usize,
        width: usize,
        what: &str,
    ) -> Result<&'a [u8]> {
        let len = count.checked_mul(width).ok_or_else(|| {
            module_err(format!("batch blob: element count overflows {what} length"))
        })?;
        self.take(len, what)
    }

    pub(crate) fn skip(&mut self, n: usize, what: &str) -> Result<()> {
        self.take(n, what).map(|_| ())
    }

    pub(crate) fn u8(&mut self, what: &str) -> Result<u8> {
        Ok(self.take(1, what)?[0])
    }

    pub(crate) fn u32(&mut self, what: &str) -> Result<u32> {
        let b = self.take(4, what)?;
        Ok(u32::from_le_bytes(b.try_into().unwrap()))
    }

    /// A u32-length-prefixed UTF-8 string.
    pub(crate) fn str(&mut self, what: &str) -> Result<&'a str> {
        let len = self.u32(what)? as usize;
        let bytes = self.take(len, what)?;
        std::str::from_utf8(bytes)
            .map_err(|_| module_err(format!("batch blob: {what} is not valid UTF-8")))
    }
}

#[cfg(test)]
mod tests {
    use super::BatchReader;
    use rusqlite::Error;

    #[test]
    fn fixed_width_columns_reject_length_overflow() {
        let error = BatchReader::new(&[])
            .take_array(usize::MAX, 2, "timestamp column")
            .unwrap_err();
        let Error::ModuleError(message) = error else {
            panic!("expected module error");
        };
        assert_eq!(
            message,
            "batch blob: element count overflows timestamp column length"
        );
    }
}
