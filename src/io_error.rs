//! A compact, transport-agnostic I/O error type.
//!
//! [`IoError`] is a small `Copy` enum covering the common failure modes seen
//! across the I/O primitives in this crate.  It is deliberately independent of
//! [`std::io::Error`] so that it can be used in `no_std` builds, but when the
//! `std` feature is enabled it converts to and from [`std::io::Error`] and
//! [`std::io::ErrorKind`].  Each variant also maps to the closest HTTP status
//! code via [`IoError::to_http_status`], and round-trips through its variant
//! name via [`IoError::as_str`] / [`FromStr`].

use core::convert::Infallible;
use core::fmt::Display;
use core::ops::Deref;
use core::str::FromStr;

/// Simple common types of I/O error.
///
/// This enum is `#[non_exhaustive]`: new variants may be added in future
/// releases, so callers should always include a wildcard arm when matching.
#[derive(Debug, Copy, Clone, PartialEq)]
#[non_exhaustive]
pub enum IoError {
    /// An unknown or uncategorised error.
    Unknown,
    /// An internal failure that is not attributable to the caller's request.
    InternalFailure,
    /// The resource is temporarily offline or unavailable; retrying later may
    /// succeed.
    TemporarilyOffline,
    /// The operation is not valid in the target's current state.
    InvalidState,
    /// A remote peer failed for an unspecified reason.
    PeerFailure,
    /// The connection was closed by the peer (a broken pipe).
    BrokenPipe,
    /// The requested item could not be found.
    NotFound,
    /// The requested operation is not supported.
    Unsupported,
    /// The caller does not have permission to perform the operation.
    PermissionDenied,
    /// Authentication is required before the operation can proceed.
    AuthenticationRequired,
    /// Authentication credentials could not be verified.
    AuthenticationFailed,
    /// The peer actively refused the connection.
    ConnectionRefused,
    /// The peer could not be reached.
    Unreachable,
    /// The operation was aborted at the caller's request.
    AbortRequested,
    /// A resource allocation conflicts with an existing one (e.g. an address
    /// already in use).
    AllocationConflict,
    /// The operation conflicts with the current state of the target.
    OperationConflict,
    /// Optimistic concurrency control detected a conflicting update; the
    /// operation can be retried.
    OptimisticConcurrencyFailure,
    /// The operation would deadlock.
    Deadlock,
    /// The object to be created already exists.
    AlreadyExists,
    /// The input or message does not conform to the expected protocol or
    /// format.
    MalformedInput,
    /// The referenced data is not valid.
    InvalidData,
    /// A reference could not be resolved to its target.
    InvalidReference,
    /// The operation timed out.
    TimedOut,
    /// The request is too large to process with the available resources.
    TooBig,
    /// The resource is busy; the operation can be retried later.
    Busy,
    /// A rate limit has been exceeded.
    RateLimited,
    /// A quota has been exceeded.
    QuotaLimited,
    /// The operation was interrupted before it could complete.
    Interrupted,
    /// The end of the data was reached unexpectedly.
    UnexpectedEof,
    /// The operation ran out of memory.
    OutOfMemory,
    /// A storage limit was exceeded.
    OutOfStorage,
}

impl core::error::Error for IoError {}

impl Display for IoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IoError::Unknown => {
                write!(f, "Unknown error")
            }
            IoError::InternalFailure => {
                write!(f, "Internal failure")
            }
            IoError::TemporarilyOffline => {
                write!(f, "Temporarily offline")
            }
            IoError::InvalidState => {
                write!(f, "Invalid state for operation")
            }
            IoError::PeerFailure => {
                write!(f, "Unknown peer failed")
            }
            IoError::BrokenPipe => {
                write!(f, "Peer closed")
            }
            IoError::NotFound => {
                write!(f, "Not found")
            }
            IoError::Unsupported => {
                write!(f, "Unsupported operation")
            }
            IoError::PermissionDenied => {
                write!(f, "Permission denied")
            }
            IoError::AuthenticationRequired => {
                write!(f, "Authentication required")
            }
            IoError::AuthenticationFailed => {
                write!(f, "Authentication could not be verified")
            }
            IoError::ConnectionRefused => {
                write!(f, "Connection refused")
            }
            IoError::Unreachable => {
                write!(f, "Peer unreachable")
            }
            IoError::AbortRequested => {
                write!(f, "Abort requested")
            }
            IoError::AllocationConflict => {
                write!(f, "Conflicting allocations")
            }
            IoError::OperationConflict => {
                write!(f, "Operation conflicts with the current state")
            }
            IoError::OptimisticConcurrencyFailure => {
                write!(f, "Optimistic concurrency control failure (can retry)")
            }
            IoError::Deadlock => {
                write!(f, "Deadlock")
            }
            IoError::AlreadyExists => {
                write!(f, "Object to create already exists")
            }
            IoError::MalformedInput => {
                write!(f, "Input or message does not conform to protocol")
            }
            IoError::InvalidData => {
                write!(f, "Referenced data is not valid")
            }
            IoError::InvalidReference => {
                write!(f, "Could not find reference target")
            }
            IoError::TimedOut => {
                write!(f, "Operation timed out")
            }
            IoError::TooBig => {
                write!(f, "Request is too big to process with available resources")
            }
            IoError::Busy => {
                write!(f, "Busy. Try again later")
            }
            IoError::RateLimited => {
                write!(f, "Rate limit exceeded")
            }
            IoError::QuotaLimited => {
                write!(f, "Quota exceeded")
            }
            IoError::Interrupted => {
                write!(f, "Interrupted")
            }
            IoError::UnexpectedEof => {
                write!(f, "Unexpected end of data")
            }
            IoError::OutOfMemory => {
                write!(f, "Out of memory")
            }
            IoError::OutOfStorage => {
                write!(f, "Storage limit exceeded")
            }
        }
    }
}

impl IoError {
    /// Map to the most equivalent HTTP status code.
    pub fn to_http_status(self) -> u16 {
        match self {
            IoError::Unknown => 500,
            IoError::InternalFailure => 500,
            IoError::TemporarilyOffline => 503,
            IoError::InvalidState => 409,
            IoError::PeerFailure => 502,
            IoError::BrokenPipe => 502,
            IoError::NotFound => 404,
            IoError::Unsupported => 501,
            IoError::PermissionDenied => 403,
            IoError::AuthenticationRequired => 401,
            IoError::AuthenticationFailed => 401,
            IoError::ConnectionRefused => 502,
            IoError::Unreachable => 502,
            IoError::AbortRequested => 499,
            IoError::AllocationConflict => 409,
            IoError::OperationConflict => 409,
            IoError::OptimisticConcurrencyFailure => 409,
            IoError::Deadlock => 409,
            IoError::AlreadyExists => 409,
            IoError::MalformedInput => 400,
            IoError::InvalidData => 422,
            IoError::InvalidReference => 422,
            IoError::TimedOut => 504,
            IoError::TooBig => 413,
            IoError::Busy => 503,
            IoError::RateLimited => 429,
            IoError::QuotaLimited => 429,
            IoError::Interrupted => 503,
            IoError::UnexpectedEof => 400,
            IoError::OutOfMemory => 503,
            IoError::OutOfStorage => 507,
        }
    }
}

#[cfg(feature = "std")]
impl From<IoError> for std::io::ErrorKind {
    fn from(err: IoError) -> Self {
        match err {
            IoError::Unknown => std::io::ErrorKind::Other,
            IoError::InternalFailure => std::io::ErrorKind::Other,
            IoError::TemporarilyOffline => std::io::ErrorKind::Other,
            IoError::InvalidState => std::io::ErrorKind::Other,
            IoError::PeerFailure => std::io::ErrorKind::Other,
            IoError::BrokenPipe => std::io::ErrorKind::BrokenPipe,
            IoError::NotFound => std::io::ErrorKind::NotFound,
            IoError::Unsupported => std::io::ErrorKind::Unsupported,
            IoError::PermissionDenied => std::io::ErrorKind::PermissionDenied,
            IoError::AuthenticationRequired => std::io::ErrorKind::PermissionDenied,
            IoError::AuthenticationFailed => std::io::ErrorKind::PermissionDenied,
            IoError::ConnectionRefused => std::io::ErrorKind::ConnectionRefused,
            IoError::Unreachable => std::io::ErrorKind::HostUnreachable,
            IoError::AbortRequested => std::io::ErrorKind::ConnectionAborted,
            IoError::AllocationConflict => std::io::ErrorKind::AddrInUse,
            IoError::OperationConflict => std::io::ErrorKind::Other,
            IoError::OptimisticConcurrencyFailure => std::io::ErrorKind::Deadlock,
            IoError::Deadlock => std::io::ErrorKind::Deadlock,
            IoError::AlreadyExists => std::io::ErrorKind::AlreadyExists,
            IoError::MalformedInput => std::io::ErrorKind::InvalidInput,
            IoError::InvalidData => std::io::ErrorKind::InvalidData,
            IoError::InvalidReference => std::io::ErrorKind::InvalidData,
            IoError::TimedOut => std::io::ErrorKind::TimedOut,
            IoError::TooBig => std::io::ErrorKind::ArgumentListTooLong,
            IoError::Busy => std::io::ErrorKind::ResourceBusy,
            IoError::RateLimited => std::io::ErrorKind::QuotaExceeded,
            IoError::QuotaLimited => std::io::ErrorKind::QuotaExceeded,
            IoError::Interrupted => std::io::ErrorKind::Interrupted,
            IoError::UnexpectedEof => std::io::ErrorKind::UnexpectedEof,
            IoError::OutOfMemory => std::io::ErrorKind::OutOfMemory,
            IoError::OutOfStorage => std::io::ErrorKind::StorageFull,
        }
    }
}

#[cfg(feature = "std")]
impl From<IoError> for std::io::Error {
    fn from(err: IoError) -> Self {
        std::io::Error::new(std::io::ErrorKind::from(err), err)
    }
}

#[cfg(feature = "std")]
impl From<std::io::ErrorKind> for IoError {
    fn from(kind: std::io::ErrorKind) -> Self {
        match kind {
            std::io::ErrorKind::NotFound => IoError::NotFound,
            std::io::ErrorKind::PermissionDenied => IoError::PermissionDenied,
            std::io::ErrorKind::ConnectionRefused => IoError::ConnectionRefused,
            std::io::ErrorKind::ConnectionReset => IoError::BrokenPipe,
            std::io::ErrorKind::HostUnreachable => IoError::Unreachable,
            std::io::ErrorKind::NetworkUnreachable => IoError::Unreachable,
            std::io::ErrorKind::ConnectionAborted => IoError::AbortRequested,
            std::io::ErrorKind::NotConnected => IoError::BrokenPipe,
            std::io::ErrorKind::AddrInUse => IoError::AllocationConflict,
            std::io::ErrorKind::AddrNotAvailable => IoError::AllocationConflict,
            std::io::ErrorKind::NetworkDown => IoError::TemporarilyOffline,
            std::io::ErrorKind::BrokenPipe => IoError::BrokenPipe,
            std::io::ErrorKind::AlreadyExists => IoError::AlreadyExists,
            std::io::ErrorKind::WouldBlock => IoError::Busy,
            std::io::ErrorKind::NotADirectory => IoError::InvalidReference,
            std::io::ErrorKind::IsADirectory => IoError::InvalidReference,
            std::io::ErrorKind::DirectoryNotEmpty => IoError::OperationConflict,
            std::io::ErrorKind::ReadOnlyFilesystem => IoError::Unsupported,
            std::io::ErrorKind::StaleNetworkFileHandle => IoError::InvalidReference,
            std::io::ErrorKind::InvalidInput => IoError::MalformedInput,
            std::io::ErrorKind::InvalidData => IoError::InvalidData,
            std::io::ErrorKind::TimedOut => IoError::TimedOut,
            std::io::ErrorKind::WriteZero => IoError::BrokenPipe,
            std::io::ErrorKind::StorageFull => IoError::OutOfStorage,
            std::io::ErrorKind::NotSeekable => IoError::Unsupported,
            std::io::ErrorKind::QuotaExceeded => IoError::QuotaLimited,
            std::io::ErrorKind::FileTooLarge => IoError::TooBig,
            std::io::ErrorKind::ResourceBusy => IoError::Busy,
            std::io::ErrorKind::ExecutableFileBusy => IoError::Busy,
            std::io::ErrorKind::Deadlock => IoError::Deadlock,
            std::io::ErrorKind::CrossesDevices => IoError::Unsupported,
            std::io::ErrorKind::TooManyLinks => IoError::Unknown,
            std::io::ErrorKind::InvalidFilename => IoError::MalformedInput,
            std::io::ErrorKind::ArgumentListTooLong => IoError::TooBig,
            std::io::ErrorKind::Interrupted => IoError::Interrupted,
            std::io::ErrorKind::Unsupported => IoError::Unsupported,
            std::io::ErrorKind::UnexpectedEof => IoError::UnexpectedEof,
            std::io::ErrorKind::OutOfMemory => IoError::OutOfMemory,
            std::io::ErrorKind::Other => IoError::Unknown,
            _ => IoError::Unknown,
        }
    }
}

#[cfg(feature = "std")]
impl From<std::io::Error> for IoError {
    fn from(err: std::io::Error) -> Self {
        if let Some(inner) = err.get_ref().and_then(|e| e.downcast_ref::<IoError>()) {
            *inner
        } else {
            IoError::from(err.kind())
        }
    }
}

impl IoError {
    /// The enum constant name for this variant.
    pub const fn as_str(self) -> &'static str {
        match self {
            IoError::Unknown => "Unknown",
            IoError::InternalFailure => "InternalFailure",
            IoError::TemporarilyOffline => "TemporarilyOffline",
            IoError::InvalidState => "InvalidState",
            IoError::PeerFailure => "PeerFailure",
            IoError::BrokenPipe => "BrokenPipe",
            IoError::NotFound => "NotFound",
            IoError::Unsupported => "Unsupported",
            IoError::PermissionDenied => "PermissionDenied",
            IoError::AuthenticationRequired => "AuthenticationRequired",
            IoError::AuthenticationFailed => "AuthenticationFailed",
            IoError::ConnectionRefused => "ConnectionRefused",
            IoError::Unreachable => "Unreachable",
            IoError::AbortRequested => "AbortRequested",
            IoError::AllocationConflict => "AllocationConflict",
            IoError::OperationConflict => "OperationConflict",
            IoError::OptimisticConcurrencyFailure => "OptimisticConcurrencyFailure",
            IoError::Deadlock => "Deadlock",
            IoError::AlreadyExists => "AlreadyExists",
            IoError::MalformedInput => "MalformedInput",
            IoError::InvalidData => "InvalidData",
            IoError::InvalidReference => "InvalidReference",
            IoError::TimedOut => "TimedOut",
            IoError::TooBig => "TooBig",
            IoError::Busy => "Busy",
            IoError::RateLimited => "RateLimited",
            IoError::QuotaLimited => "QuotaLimited",
            IoError::Interrupted => "Interrupted",
            IoError::UnexpectedEof => "UnexpectedEof",
            IoError::OutOfMemory => "OutOfMemory",
            IoError::OutOfStorage => "OutOfStorage",
        }
    }
}

impl Deref for IoError {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for IoError {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Infallible> {
        Ok(match s {
            "Unknown" => IoError::Unknown,
            "InternalFailure" => IoError::InternalFailure,
            "TemporarilyOffline" => IoError::TemporarilyOffline,
            "InvalidState" => IoError::InvalidState,
            "PeerFailure" => IoError::PeerFailure,
            "BrokenPipe" => IoError::BrokenPipe,
            "NotFound" => IoError::NotFound,
            "Unsupported" => IoError::Unsupported,
            "PermissionDenied" => IoError::PermissionDenied,
            "AuthenticationRequired" => IoError::AuthenticationRequired,
            "AuthenticationFailed" => IoError::AuthenticationFailed,
            "ConnectionRefused" => IoError::ConnectionRefused,
            "Unreachable" => IoError::Unreachable,
            "AbortRequested" => IoError::AbortRequested,
            "AllocationConflict" => IoError::AllocationConflict,
            "OperationConflict" => IoError::OperationConflict,
            "OptimisticConcurrencyFailure" => IoError::OptimisticConcurrencyFailure,
            "Deadlock" => IoError::Deadlock,
            "AlreadyExists" => IoError::AlreadyExists,
            "MalformedInput" => IoError::MalformedInput,
            "InvalidData" => IoError::InvalidData,
            "InvalidReference" => IoError::InvalidReference,
            "TimedOut" => IoError::TimedOut,
            "TooBig" => IoError::TooBig,
            "Busy" => IoError::Busy,
            "RateLimited" => IoError::RateLimited,
            "QuotaLimited" => IoError::QuotaLimited,
            "Interrupted" => IoError::Interrupted,
            "UnexpectedEof" => IoError::UnexpectedEof,
            "OutOfMemory" => IoError::OutOfMemory,
            "OutOfStorage" => IoError::OutOfStorage,
            _ => IoError::Unknown,
        })
    }
}
