use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::KvFormat;
use std::fmt::Display;

pub enum WriteFormat {
    ArrowLog,
    IndexedLog,
    CompactedLog,
    IndexedKv,
    CompactedKv,
}

impl WriteFormat {
    pub fn is_log(&self) -> bool {
        match self {
            WriteFormat::ArrowLog => true,
            WriteFormat::IndexedLog => true,
            WriteFormat::CompactedLog => true,
            WriteFormat::IndexedKv => false,
            WriteFormat::CompactedKv => false,
        }
    }

    pub fn is_kv(&self) -> bool {
        !self.is_log()
    }

    pub fn to_kv_format(&self) -> Result<KvFormat> {
        match self {
            WriteFormat::IndexedKv => Ok(KvFormat::INDEXED),
            WriteFormat::CompactedKv => Ok(KvFormat::COMPACTED),
            other => Err(IllegalArgument {
                message: format!("WriteFormat `{}` is not a KvFormat", other),
            }),
        }
    }

    pub fn from_kv_format(kv_format: &KvFormat) -> Result<Self> {
        match kv_format {
            KvFormat::INDEXED => Ok(WriteFormat::IndexedKv),
            KvFormat::COMPACTED => Ok(WriteFormat::CompactedKv),
        }
    }
}

impl Display for WriteFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteFormat::ArrowLog => f.write_str("ArrowLog"),
            WriteFormat::IndexedLog => f.write_str("IndexedLog"),
            WriteFormat::CompactedLog => f.write_str("CompactedLog"),
            WriteFormat::IndexedKv => f.write_str("IndexedKv"),
            WriteFormat::CompactedKv => f.write_str("CompactedKv"),
        }
    }
}
