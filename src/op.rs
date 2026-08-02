use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum FsOpKind {
    Read = 0,
    Write = 1,
}

impl FsOpKind {
    pub fn try_from_raw(opcode: u8) -> Option<Self> {
        match opcode {
            0 => Some(Self::Read),
            1 => Some(Self::Write),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Immutable, IntoBytes, FromBytes)]
#[repr(C)]
pub struct FsOpSqe {
    pub opcode: u8,
    pub pad: [u8; 3],
    pub file_idx: u32,
    pub off: u64,
    pub buf_offset: u32,
    pub buf_len: u32,
    pub user_data: u64,
}

impl FsOpSqe {
    pub fn opcode(&self) -> Option<FsOpKind> {
        FsOpKind::try_from_raw(self.opcode)
    }
}

#[derive(Debug, Clone, Immutable, IntoBytes, FromBytes)]
#[repr(C)]
pub struct FsOpCqe {
    pub user_data: u64,
    pub res: i32,
    pub pad: u32,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum DiskOpKind {
    Read = 0,
    Write = 1,
}

impl DiskOpKind {
    pub fn try_from_raw(raw: u8) -> Option<Self> {
        Some(match raw {
            0 => Self::Read,
            1 => Self::Write,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Immutable, IntoBytes, FromBytes)]
#[repr(C)]
pub struct DiskOpSqe {
    pub opcode: u8,
    pub pad: [u8; 3],
    pub file_idx: u32,
    pub block: u64,
    pub buf_offset: u32,
    pub buf_len: u32,
    pub user_data: u64,
}

impl DiskOpSqe {
    pub fn opcode(&self) -> Option<DiskOpKind> {
        DiskOpKind::try_from_raw(self.opcode)
    }
}

#[derive(Debug, Clone, Immutable, IntoBytes, FromBytes)]
#[repr(C)]
pub struct DiskOpCqe {
    pub user_data: u64,
    pub count: u32,
    pub status: u16,
    pub pad: u16,
}

pub const RING_MAX_SQ_ENTRIES: u32 = 1 << 15;
pub const RING_MAX_CQ_ENTRIES: u32 = 2 * RING_MAX_SQ_ENTRIES;

bitflags::bitflags! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    #[repr(transparent)]
    pub struct RingSetupFlags: u32 {
        const CQSIZE = 1 << 0;
    }
}

#[derive(Debug, Clone, Immutable, IntoBytes, FromBytes, KnownLayout)]
#[repr(C)]
pub struct RingSetupParams {
    /// Number of submission queue entries.
    pub nr_sq_entries: u32,
    /// Number of completion queue entries.
    pub nr_cq_entries: u32,
    /// See [`RingSetupFlags`].
    pub flags: u32,
    pub pool_size: u32, // (in bytes)
}

impl RingSetupParams {
    pub fn flags(&self) -> Option<RingSetupFlags> {
        RingSetupFlags::from_bits(self.flags)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum RingCallVerb {
    Setup = 0,
    SetFileTable = 1,
}

impl RingCallVerb {
    pub fn try_from_raw(verb: u8) -> Option<Self> {
        match verb {
            0 => Some(Self::Setup),
            1 => Some(Self::SetFileTable),
            _ => None,
        }
    }
}
