use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Copy)]
pub enum TestEventSource {
    Gateway,
    Ingestor,
    Node,
    #[default]
    Unspecified,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Copy)]
pub enum TestEvent {
    Startup,
    SearchRequest,
    SearchResponse,
    SearchFailure,
    BatchFlushStart,
    BatchFlushNodeSuccess,
    BatchFlushNodeFailure,
    BatchFlushFailed,
    BatchFlushSuccess,
    CdcRowReceived,
    CdcRowRouted,
    BatchEnqueueFailed,
    IndexBatchRequest,
    IndexBatchResponse,
    IndexBatchFailure,
    #[default]
    Unspecified,
}

impl fmt::Display for TestEventSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Gateway => write!(f, "Gateway"),
            Self::Ingestor => write!(f, "Ingestor"),
            Self::Node => write!(f, "Node"),
            Self::Unspecified => write!(f, "Unspecified"),
        }
    }
}

impl fmt::Display for TestEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Startup => write!(f, "Startup"),
            Self::SearchRequest => write!(f, "SearchRequest"),
            Self::SearchResponse => write!(f, "SearchResponse"),
            Self::SearchFailure => write!(f, "SearchFailure"),
            Self::BatchFlushStart => write!(f, "BatchFlushStart"),
            Self::BatchFlushNodeSuccess => write!(f, "BatchFlushNodeSuccess"),
            Self::BatchFlushNodeFailure => write!(f, "BatchFlushNodeFailure"),
            Self::BatchFlushFailed => write!(f, "BatchFlushFailed"),
            Self::BatchFlushSuccess => write!(f, "BatchFlushSuccess"),
            Self::CdcRowReceived => write!(f, "CdcRowReceived"),
            Self::CdcRowRouted => write!(f, "CdcRowRouted"),
            Self::BatchEnqueueFailed => write!(f, "BatchEnqueueFailed"),
            Self::IndexBatchRequest => write!(f, "IndexBatchRequest"),
            Self::IndexBatchResponse => write!(f, "IndexBatchResponse"),
            Self::IndexBatchFailure => write!(f, "IndexBatchFailure"),
            Self::Unspecified => write!(f, "Unspecified"),
        }
    }
}

impl From<TestEventSource> for &'static str {
    fn from(value: TestEventSource) -> Self {
        match value {
            TestEventSource::Gateway => "Gateway",
            TestEventSource::Ingestor => "Ingestor",
            TestEventSource::Node => "Node",
            TestEventSource::Unspecified => "Unspecified",
        }
    }
}

impl From<TestEvent> for &'static str {
    fn from(value: TestEvent) -> Self {
        match value {
            TestEvent::Startup => "Startup",
            TestEvent::SearchRequest => "SearchRequest",
            TestEvent::SearchResponse => "SearchResponse",
            TestEvent::SearchFailure => "SearchFailure",
            TestEvent::BatchFlushStart => "BatchFlushStart",
            TestEvent::BatchFlushNodeSuccess => "BatchFlushNodeSuccess",
            TestEvent::BatchFlushNodeFailure => "BatchFlushNodeFailure",
            TestEvent::BatchFlushFailed => "BatchFlushFailed",
            TestEvent::BatchFlushSuccess => "BatchFlushSuccess",
            TestEvent::CdcRowReceived => "CdcRowReceived",
            TestEvent::CdcRowRouted => "CdcRowRouted",
            TestEvent::BatchEnqueueFailed => "BatchEnqueueFailed",
            TestEvent::IndexBatchRequest => "IndexBatchRequest",
            TestEvent::IndexBatchResponse => "IndexBatchResponse",
            TestEvent::IndexBatchFailure => "IndexBatchFailure",
            TestEvent::Unspecified => "Unspecified",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseEnumError(String);

impl fmt::Display for ParseEnumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown variant: '{}'", self.0)
    }
}

impl std::error::Error for ParseEnumError {}

impl FromStr for TestEventSource {
    type Err = ParseEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Gateway" => Ok(Self::Gateway),
            "Ingestor" => Ok(Self::Ingestor),
            "Node" => Ok(Self::Node),
            "Unspecified" => Ok(Self::Unspecified),
            other => Err(ParseEnumError(other.to_string())),
        }
    }
}

impl FromStr for TestEvent {
    type Err = ParseEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Startup" => Ok(Self::Startup),
            "SearchRequest" => Ok(Self::SearchRequest),
            "SearchResponse" => Ok(Self::SearchResponse),
            "SearchFailure" => Ok(Self::SearchFailure),
            "BatchFlushStart" => Ok(Self::BatchFlushStart),
            "BatchFlushNodeSuccess" => Ok(Self::BatchFlushNodeSuccess),
            "BatchFlushNodeFailure" => Ok(Self::BatchFlushNodeFailure),
            "BatchFlushFailed" => Ok(Self::BatchFlushFailed),
            "BatchFlushSuccess" => Ok(Self::BatchFlushSuccess),
            "CdcRowReceived" => Ok(Self::CdcRowReceived),
            "CdcRowRouted" => Ok(Self::CdcRowRouted),
            "BatchEnqueueFailed" => Ok(Self::BatchEnqueueFailed),
            "IndexBatchRequest" => Ok(Self::IndexBatchRequest),
            "IndexBatchResponse" => Ok(Self::IndexBatchResponse),
            "IndexBatchFailure" => Ok(Self::IndexBatchFailure),
            "Unspecified" => Ok(Self::Unspecified),
            other => Err(ParseEnumError(other.to_string())),
        }
    }
}

impl From<&str> for TestEventSource {
    fn from(s: &str) -> Self {
        s.parse().unwrap_or_default()
    }
}

impl From<String> for TestEventSource {
    fn from(s: String) -> Self {
        s.as_str().into()
    }
}

impl From<&str> for TestEvent {
    fn from(s: &str) -> Self {
        s.parse().unwrap_or_default()
    }
}

impl From<String> for TestEvent {
    fn from(s: String) -> Self {
        s.as_str().into()
    }
}
