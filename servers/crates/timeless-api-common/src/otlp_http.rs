//! OTLP/HTTP response encoding, including errors raised by outer middleware.

use axum::body::Bytes;
use axum::extract::Request;
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use prost::Message;

#[derive(Clone, Copy, Debug)]
pub enum Encoding {
    Json,
    Protobuf,
}

#[derive(Clone)]
struct Encoded;

// google.rpc.Status: code and details are optional for OTLP/HTTP. Keeping
// only message emits the standard field 2 in binary and JSON encodings.
#[derive(prost::Message, serde::Serialize)]
struct Status {
    #[prost(string, tag = "2")]
    message: String,
}

impl Encoding {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        if headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/x-protobuf"))
        {
            Self::Protobuf
        } else {
            // Preserve the existing JSON fallback for missing or legacy
            // content types. Only the two OTLP encodings are emitted.
            Self::Json
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Protobuf => "application/x-protobuf",
        }
    }

    pub fn success(self) -> Response {
        // An ExportTraceServiceResponse with no partial_success field.
        self.response(
            StatusCode::OK,
            match self {
                Self::Json => Bytes::from_static(b"{}"),
                Self::Protobuf => Bytes::new(),
            },
        )
    }

    pub fn error(self, code: StatusCode, message: impl Into<String>) -> Response {
        let mut message = message.into();
        // Parser diagnostics can contain user-selected field names. Bound
        // the response independently of the rejected request's size.
        let mut end = message.len().min(4096);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        let status = Status { message };
        let bytes = match self {
            Self::Json => serde_json::to_vec(&status).expect("status contains only a string"),
            Self::Protobuf => status.encode_to_vec(),
        };
        self.response(code, bytes.into())
    }

    fn response(self, code: StatusCode, body: Bytes) -> Response {
        let mut response =
            (code, [(header::CONTENT_TYPE, self.content_type())], body).into_response();
        response.extensions_mut().insert(Encoded);
        response
    }
}

/// Wrap the OTLP route and authentication so even pre-handler rejections
/// and method errors have a standard Status body. Preserve Retry-After and
/// other middleware headers without copying an internal error body.
pub async fn response_boundary(request: Request, next: Next) -> Response {
    let encoding = (request.uri().path() == "/insert/opentelemetry/v1/traces")
        .then(|| Encoding::from_headers(request.headers()));
    let response = next.run(request).await;
    let Some(encoding) = encoding else {
        return response;
    };
    if !(response.status().is_client_error() || response.status().is_server_error())
        || response.extensions().get::<Encoded>().is_some()
    {
        return response;
    }
    let mut encoded = encoding.error(
        response.status(),
        response
            .status()
            .canonical_reason()
            .unwrap_or("request failed"),
    );
    for (name, value) in response.headers() {
        if !matches!(
            *name,
            header::CONTENT_TYPE | header::CONTENT_LENGTH | header::CONTENT_ENCODING
        ) {
            encoded.headers_mut().append(name.clone(), value.clone());
        }
    }
    encoded
}
