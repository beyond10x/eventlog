//! Bytes this process read and wrote through system calls, from Linux `/proc/self/io`.
//!
//! The file sums every thread of the process, so the blocking pool's reads count. Reading the
//! file is itself one read of known length; [`Snapshot::since`] takes that read off again.
use std::io::Read;

#[derive(Clone, Copy, Debug)]
pub struct Snapshot {
    rchar: u64,
    wchar: u64,
    own_read: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Io {
    pub read: u64,
    pub written: u64,
}

impl Snapshot {
    /// `None` where `/proc/self/io` does not exist (not Linux).
    pub fn take() -> Option<Self> {
        let mut file = std::fs::File::open("/proc/self/io").ok()?;
        let mut buffer = [0_u8; 1024];
        let length = file.read(&mut buffer).ok()?;
        let text = std::str::from_utf8(&buffer[..length]).ok()?;
        let field = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name))
                .and_then(|value| value.trim().parse::<u64>().ok())
        };
        Some(Self {
            rchar: field("rchar:")?,
            wchar: field("wchar:")?,
            own_read: u64::try_from(length).ok()?,
        })
    }

    /// What the process read and wrote since this snapshot, less the snapshot's own read.
    pub fn since(&self) -> Option<Io> {
        let now = Self::take()?;
        Some(Io {
            read: now.rchar.saturating_sub(self.rchar + self.own_read),
            written: now.wchar.saturating_sub(self.wchar),
        })
    }
}
