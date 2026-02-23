//! Protocol-level integration tests for OTLP gRPC and Arrow Flight paths.
//!
//! These tests require the docker-compose stack and validate that the advertised
//! protocol surface is reachable and functionally wired.

use crate::e2e::E2EHarness;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, Any, Command,
    CommandGetSqlInfo, CommandPreparedStatementQuery, CommandStatementQuery, ProstMessageExt,
};
use arrow_flight::{Action, Empty, FlightDescriptor, HandshakeRequest, Ticket};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{metric::Data, number_data_point, Gauge, Metric};
use opentelemetry_proto::tonic::metrics::v1::{NumberDataPoint, ResourceMetrics, ScopeMetrics};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tonic::Code;
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

#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_flight_ingest_handshake_schema_and_actions() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("services healthy");

    let endpoint = grpc_endpoint(&harness.ingester_url, 4317);
    let mut client = FlightServiceClient::connect(endpoint)
        .await
        .expect("connect flight ingest");

    let handshake_stream = tokio_stream::iter(vec![HandshakeRequest {
        protocol_version: 1,
        payload: Bytes::from_static(b"hello"),
    }]);
    let mut handshake = client
        .handshake(handshake_stream)
        .await
        .expect("handshake should succeed")
        .into_inner();
    let response = handshake
        .message()
        .await
        .expect("handshake stream read")
        .expect("handshake response");
    assert_eq!(response.payload, Bytes::from_static(b"hello"));

    let schema = client
        .get_schema(FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: Bytes::new(),
            path: vec!["ingest".to_string(), "doput".to_string()],
        })
        .await
        .expect("get_schema should succeed")
        .into_inner();
    assert!(
        !schema.schema.is_empty(),
        "schema IPC payload should not be empty"
    );

    let mut actions = client
        .list_actions(Empty {})
        .await
        .expect("list_actions should succeed")
        .into_inner();
    let action = actions
        .message()
        .await
        .expect("actions stream read")
        .expect("first action");
    assert_eq!(action.r#type, "flight.ingest.doput");

    let err = client
        .do_get(Ticket {
            ticket: Bytes::from_static(b"unsupported"),
        })
        .await
        .expect_err("ingest do_get should be rejected");
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test]
#[ignore = "requires running docker-compose stack"]
async fn test_flight_sql_metadata_and_prepared_actions() {
    let harness = E2EHarness::from_env();
    harness
        .wait_healthy(Duration::from_secs(30))
        .await
        .expect("services healthy");

    let endpoint = grpc_endpoint(&harness.query_url, 8815);
    let mut client = FlightServiceClient::connect(endpoint)
        .await
        .expect("connect flight sql");

    let info_descriptor = FlightDescriptor {
        r#type: DescriptorType::Cmd as i32,
        cmd: Command::CommandGetSqlInfo(CommandGetSqlInfo { info: vec![] })
            .into_any()
            .encode_to_vec()
            .into(),
        path: vec![],
    };
    let info = client
        .get_flight_info(info_descriptor)
        .await
        .expect("get_flight_info sql_info should succeed")
        .into_inner();
    let ticket = info
        .endpoint
        .first()
        .and_then(|e| e.ticket.clone())
        .expect("sql_info endpoint ticket");
    let mut sql_info_stream = client
        .do_get(ticket)
        .await
        .expect("sql_info do_get should succeed")
        .into_inner();
    assert!(
        sql_info_stream
            .message()
            .await
            .expect("sql_info stream read")
            .is_some(),
        "sql_info stream should emit at least one message"
    );

    let create_req = ActionCreatePreparedStatementRequest {
        query: "SELECT 1 AS one".to_string(),
        transaction_id: None,
    };
    let mut create_stream = client
        .do_action(Action {
            r#type: "CreatePreparedStatement".to_string(),
            body: create_req.as_any().encode_to_vec().into(),
        })
        .await
        .expect("create prepared statement action")
        .into_inner();
    let create_body = create_stream
        .message()
        .await
        .expect("create prepared stream read")
        .expect("create prepared response")
        .body;
    let any = Any::decode(&*create_body).expect("decode Any");
    let prepared: arrow_flight::sql::ActionCreatePreparedStatementResult = any
        .unpack()
        .expect("unpack action result")
        .expect("typed action payload");
    assert!(
        !prepared.prepared_statement_handle.is_empty(),
        "prepared handle should be present"
    );

    let prepared_descriptor = FlightDescriptor {
        r#type: DescriptorType::Cmd as i32,
        cmd: Command::CommandPreparedStatementQuery(CommandPreparedStatementQuery {
            prepared_statement_handle: prepared.prepared_statement_handle.clone(),
        })
        .into_any()
        .encode_to_vec()
        .into(),
        path: vec![],
    };
    let prepared_info = client
        .get_flight_info(prepared_descriptor)
        .await
        .expect("prepared get_flight_info should succeed")
        .into_inner();
    let prepared_ticket = prepared_info
        .endpoint
        .first()
        .and_then(|e| e.ticket.clone())
        .expect("prepared endpoint ticket");
    let mut prepared_stream = client
        .do_get(prepared_ticket)
        .await
        .expect("prepared do_get should succeed")
        .into_inner();
    assert!(
        prepared_stream
            .message()
            .await
            .expect("prepared stream read")
            .is_some(),
        "prepared stream should emit data"
    );

    client
        .do_action(Action {
            r#type: "ClosePreparedStatement".to_string(),
            body: ActionClosePreparedStatementRequest {
                prepared_statement_handle: prepared.prepared_statement_handle.clone(),
            }
            .as_any()
            .encode_to_vec()
            .into(),
        })
        .await
        .expect("close prepared statement action");

    let post_close_err = client
        .get_flight_info(FlightDescriptor {
            r#type: DescriptorType::Cmd as i32,
            cmd: Command::CommandPreparedStatementQuery(CommandPreparedStatementQuery {
                prepared_statement_handle: prepared.prepared_statement_handle,
            })
            .into_any()
            .encode_to_vec()
            .into(),
            path: vec![],
        })
        .await
        .expect_err("closed prepared statement should be rejected");
    assert_eq!(post_close_err.code(), Code::InvalidArgument);
}
