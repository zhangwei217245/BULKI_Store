use anyhow::Error;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum StatusCode {
    Ok = 0,
    InvalidRequest = 1,
    InvalidResponse = 2,
    ConnectionFailure = 3,
    HandlerNotFound = 4,
    RequestTimeout = 5,
    RequestHandlerError = 6,
    ResponseHandlerError = 7,
    Cancelled = 8,
    Aborted = 9,
    OutOfMemory = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unknown = 14,
    NotFound = 15,
    Unauthenticated = 16,
}

impl Default for StatusCode {
    fn default() -> Self {
        StatusCode::Ok
    }
}

impl From<StatusCode> for u8 {
    fn from(code: StatusCode) -> Self {
        code as u8
    }
}

impl From<u8> for StatusCode {
    fn from(value: u8) -> Self {
        match value {
            0 => StatusCode::Ok,
            1 => StatusCode::InvalidRequest,
            2 => StatusCode::InvalidResponse,
            3 => StatusCode::ConnectionFailure,
            4 => StatusCode::HandlerNotFound,
            5 => StatusCode::RequestTimeout,
            6 => StatusCode::RequestHandlerError,
            7 => StatusCode::ResponseHandlerError,
            8 => StatusCode::Cancelled,
            9 => StatusCode::Aborted,
            10 => StatusCode::OutOfMemory,
            11 => StatusCode::OutOfRange,
            12 => StatusCode::Unimplemented,
            13 => StatusCode::Internal,
            14 => StatusCode::Unknown,
            15 => StatusCode::NotFound,
            16 => StatusCode::Unauthenticated,
            _ => StatusCode::Unknown,
        }
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatusCode::Ok => write!(f, "OK"),
            StatusCode::InvalidRequest => write!(f, "Invalid Request"),
            StatusCode::InvalidResponse => write!(f, "Invalid Response"),
            StatusCode::ConnectionFailure => write!(f, "Connection Failure"),
            StatusCode::HandlerNotFound => write!(f, "Handler Not Found"),
            StatusCode::RequestTimeout => write!(f, "Request Timeout"),
            StatusCode::RequestHandlerError => write!(f, "Request Handler Error"),
            StatusCode::ResponseHandlerError => write!(f, "Response Handler Error"),
            StatusCode::Cancelled => write!(f, "Cancelled"),
            StatusCode::Aborted => write!(f, "Aborted"),
            StatusCode::OutOfMemory => write!(f, "Out of Memory"),
            StatusCode::OutOfRange => write!(f, "Out of Range"),
            StatusCode::Unimplemented => write!(f, "Unimplemented"),
            StatusCode::Internal => write!(f, "Internal Error"),
            StatusCode::Unknown => write!(f, "Unknown"),
            StatusCode::NotFound => write!(f, "Not Found"),
            StatusCode::Unauthenticated => write!(f, "Unauthenticated"),
        }
    }
}

impl StatusCode {
    /// Returns true if this status code represents success
    pub fn is_ok(&self) -> bool {
        matches!(self, StatusCode::Ok)
    }

    /// Returns true if this status code represents an error
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }

    /// Returns a canonical string representation of this status code
    pub fn as_str(&self) -> &'static str {
        match self {
            StatusCode::Ok => "OK",
            StatusCode::InvalidRequest => "INVALID_REQUEST",
            StatusCode::InvalidResponse => "INVALID_RESPONSE",
            StatusCode::ConnectionFailure => "CONNECTION_FAILURE",
            StatusCode::HandlerNotFound => "HANDLER_NOT_FOUND",
            StatusCode::RequestTimeout => "REQUEST_TIMEOUT",
            StatusCode::RequestHandlerError => "REQUEST_HANDLER_ERROR",
            StatusCode::ResponseHandlerError => "RESPONSE_HANDLER_ERROR",
            StatusCode::Cancelled => "CANCELLED",
            StatusCode::Aborted => "ABORTED",
            StatusCode::OutOfMemory => "OUT_OF_MEMORY",
            StatusCode::OutOfRange => "OUT_OF_RANGE",
            StatusCode::Unimplemented => "UNIMPLEMENTED",
            StatusCode::Internal => "INTERNAL",
            StatusCode::Unknown => "UNKNOWN",
            StatusCode::NotFound => "NOT_FOUND",
            StatusCode::Unauthenticated => "UNAUTHENTICATED",
        }
    }

    /// Convert from a string representation to StatusCode
    pub fn from_str(s: &str) -> Result<Self, Error> {
        match s.to_uppercase().as_str() {
            "OK" => Ok(StatusCode::Ok),
            "INVALID_REQUEST" => Ok(StatusCode::InvalidRequest),
            "INVALID_RESPONSE" => Ok(StatusCode::InvalidResponse),
            "CONNECTION_FAILURE" => Ok(StatusCode::ConnectionFailure),
            "HANDLER_NOT_FOUND" => Ok(StatusCode::HandlerNotFound),
            "REQUEST_TIMEOUT" => Ok(StatusCode::RequestTimeout),
            "REQUEST_HANDLER_ERROR" => Ok(StatusCode::RequestHandlerError),
            "RESPONSE_HANDLER_ERROR" => Ok(StatusCode::ResponseHandlerError),
            "CANCELLED" => Ok(StatusCode::Cancelled),
            "ABORTED" => Ok(StatusCode::Aborted),
            "OUT_OF_MEMORY" => Ok(StatusCode::OutOfMemory),
            "OUT_OF_RANGE" => Ok(StatusCode::OutOfRange),
            "UNIMPLEMENTED" => Ok(StatusCode::Unimplemented),
            "INTERNAL" => Ok(StatusCode::Internal),
            "UNKNOWN" => Ok(StatusCode::Unknown),
            "NOT_FOUND" => Ok(StatusCode::NotFound),
            "UNAUTHENTICATED" => Ok(StatusCode::Unauthenticated),
            _ => Err(Error::msg(format!("Invalid status code string: {}", s))),
        }
    }
}

// Implement FromStr trait to allow string parsing
impl std::str::FromStr for StatusCode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        StatusCode::from_str(s)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcErr {
    pub status_code: StatusCode,
    pub message: String,
}

pub type RPCResult<T> = Result<T, RpcErr>;

impl RpcErr {
    pub fn new(status_code: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status_code,
            message: message.into(),
        }
    }

    pub fn with_status(status: StatusCode) -> Self {
        let default_message = status.as_str().to_string();
        Self {
            status_code: status,
            message: default_message,
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status_code.clone()
    }

    pub fn from_tuple(status: StatusCode, message: impl Into<String>) -> Self {
        Self::new(status, message)
    }
}

impl From<StatusCode> for RpcErr {
    fn from(status: StatusCode) -> Self {
        Self::with_status(status)
    }
}

impl From<String> for RpcErr {
    fn from(message: String) -> Self {
        Self::new(StatusCode::Unknown, message)
    }
}

impl From<&str> for RpcErr {
    fn from(message: &str) -> Self {
        Self::new(StatusCode::Unknown, message)
    }
}

impl std::error::Error for RpcErr {}

impl fmt::Display for RpcErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status_code, self.message)
    }
}
