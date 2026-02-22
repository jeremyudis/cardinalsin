//! API-level telemetry helpers for HTTP and gRPC surfaces.

use axum::extract::MatchedPath;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::KeyValue;
use std::sync::OnceLock;
use std::time::Instant;
use tonic::Code;
use tracing::{info_span, Instrument};

struct HttpInstruments {
    request_count: Counter<u64>,
    request_duration_seconds: Histogram<f64>,
    request_errors: Counter<u64>,
}

struct GrpcInstruments {
    request_count: Counter<u64>,
    request_duration_seconds: Histogram<f64>,
    request_errors: Counter<u64>,
}

fn http_instruments() -> &'static HttpInstruments {
    static INSTRUMENTS: OnceLock<HttpInstruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("cardinalsin.api.http");
        HttpInstruments {
            request_count: meter
                .u64_counter("http.server.request.count")
                .with_description("Total number of HTTP requests handled by CardinalSin API")
                .init(),
            request_duration_seconds: meter
                .f64_histogram("http.server.request.duration")
                .with_description("HTTP request duration")
                .with_unit("s")
                .init(),
            request_errors: meter
                .u64_counter("http.server.request.errors")
                .with_description("HTTP requests with 4xx/5xx status codes")
                .init(),
        }
    })
}

fn grpc_instruments() -> &'static GrpcInstruments {
    static INSTRUMENTS: OnceLock<GrpcInstruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("cardinalsin.api.grpc");
        GrpcInstruments {
            request_count: meter
                .u64_counter("rpc.server.request.count")
                .with_description("Total number of gRPC requests handled by CardinalSin APIs")
                .init(),
            request_duration_seconds: meter
                .f64_histogram("rpc.server.duration")
                .with_description("gRPC request duration")
                .with_unit("s")
                .init(),
            request_errors: meter
                .u64_counter("rpc.server.request.errors")
                .with_description("gRPC requests with non-OK status")
                .init(),
        }
    })
}

fn http_attributes(method: &str, route: &str, status: u16) -> Vec<KeyValue> {
    vec![
        KeyValue::new("http.request.method", method.to_string()),
        KeyValue::new("http.route", route.to_string()),
        KeyValue::new("http.response.status_code", status as i64),
    ]
}

fn grpc_attributes(service: &str, method: &str, code: Code) -> Vec<KeyValue> {
    vec![
        KeyValue::new("rpc.system", "grpc"),
        KeyValue::new("rpc.service", service.to_string()),
        KeyValue::new("rpc.method", method.to_string()),
        KeyValue::new("rpc.grpc.status_code", code as i64),
    ]
}

/// HTTP middleware that records request count, duration, and status-class errors.
pub async fn http_observability_middleware(req: Request<axum::body::Body>, next: Next) -> Response {
    let start = Instant::now();
    let method = req.method().as_str().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());

    let span = info_span!(
        "http.request",
        otel.kind = "server",
        http.request.method = %method,
        http.route = %route
    );
    let response = next.run(req).instrument(span).await;
    let status = response.status().as_u16();
    let elapsed = start.elapsed().as_secs_f64();
    let attrs = http_attributes(&method, &route, status);
    let instruments = http_instruments();

    instruments.request_count.add(1, &attrs);
    instruments.request_duration_seconds.record(elapsed, &attrs);
    if status >= 400 {
        instruments.request_errors.add(1, &attrs);
    }

    response
}

/// Record gRPC request metrics using OTel semantic fields.
pub fn record_grpc_request(service: &str, method: &str, code: Code, duration_seconds: f64) {
    let attrs = grpc_attributes(service, method, code);
    let instruments = grpc_instruments();
    instruments.request_count.add(1, &attrs);
    instruments
        .request_duration_seconds
        .record(duration_seconds, &attrs);
    if code != Code::Ok {
        instruments.request_errors.add(1, &attrs);
    }
}
