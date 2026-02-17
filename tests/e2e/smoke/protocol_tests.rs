//! Protocol-level integration tests for OTLP gRPC and Arrow Flight paths.
//!
//! These tests require the docker-compose stack and validate that the advertised
//! protocol surface is reachable and functionally wired.

use crate::e2e::E2EHarness;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{Command, CommandStatementQuery};
use arrow_flight::{FlightDescriptor, Ticket};
use arrow_schema::{DataType, Field, Schema};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{metric::Data, number_data_point, Gauge, Metric};
use opentelemetry_proto::tonic::metrics::v1::{NumberDataPoint, ResourceMetrics, ScopeMetrics};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

fn grpc_endpoint(http_url: &str, grpc_port: u16) -> String {
    let parsed = Url::parse(http_url).expect("valid service URL");
    let host = parsed.host_str().expect("host in service URL");
    format!("http://{}:{}", host, grpc_port)
}

fn build_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
        Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0])),
    ];

    RecordBatch::try_new(schema, columns).expect("test batch")
}

fn otlp_test_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "otlp_grpc_integration_metric".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![KeyValue {
                                key: "host".to_string(),
                                value: Some(AnyValue {
                                    value: Some(
                                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                            "e2e-host".to_string(),
                                        ),
                                    ),
                                }),
                            }],
                            start_time_unix_nano: 0,
                            time_unix_nano: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                                as u64,
                            exemplars: vec![],
                            flags: 0,
                            value: Some(number_data_point::Value::AsDouble(42.0)),
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_otlp_grpc_export_endpoint() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("services healthy");

    let endpoint = grpc_endpoint(&harness.ingester_url, 4317);
    let mut client = MetricsServiceClient::connect(endpoint)
        .await
        .expect("connect otlp grpc");

    client
        .export(otlp_test_request())
        .await
        .expect("otlp export should succeed");
}

#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_flight_ingest_doput_endpoint() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("services healthy");

    let endpoint = grpc_endpoint(&harness.ingester_url, 4317);
    let mut client = FlightServiceClient::connect(endpoint)
        .await
        .expect("connect flight ingest");

    let batch = build_test_batch();
    let flight_stream =
        arrow_flight::utils::batches_to_flight_data(batch.schema().as_ref(), vec![batch])
            .expect("encode flight stream");
    let request_stream = tokio_stream::iter(flight_stream);

    let mut response = client
        .do_put(request_stream)
        .await
        .expect("flight doput should succeed")
        .into_inner();

    let first = response
        .message()
        .await
        .expect("putresult stream")
        .expect("first putresult");
    let rows = std::str::from_utf8(&first.app_metadata)
        .expect("metadata utf8")
        .parse::<u64>()
        .expect("metadata row count");
    assert_eq!(rows, 3);
}

#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_flight_sql_get_flight_info_and_do_get() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("services healthy");

    let endpoint = grpc_endpoint(&harness.query_url, 8815);
    let mut client = FlightServiceClient::connect(endpoint)
        .await
        .expect("connect flight sql");

    let command = Command::CommandStatementQuery(CommandStatementQuery {
        query: "SELECT 1 AS one".to_string(),
        transaction_id: None,
    });
    let descriptor = FlightDescriptor {
        r#type: DescriptorType::Cmd as i32,
        cmd: command.into_any().encode_to_vec().into(),
        path: vec![],
    };

    let info = client
        .get_flight_info(descriptor)
        .await
        .expect("get_flight_info should succeed")
        .into_inner();
    assert!(
        !info.endpoint.is_empty(),
        "expected at least one flight endpoint"
    );
    let ticket = info
        .endpoint
        .first()
        .and_then(|e| e.ticket.clone())
        .expect("flight endpoint ticket");

    let mut stream = client
        .do_get(Ticket {
            ticket: ticket.ticket.clone(),
        })
        .await
        .expect("do_get should succeed")
        .into_inner();
    let mut msg_count = 0;
    while stream.message().await.expect("stream read").is_some() {
        msg_count += 1;
    }
    assert!(msg_count > 0, "expected non-empty flight result stream");
}
