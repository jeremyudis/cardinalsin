//! gRPC services for OTLP and Arrow Flight protocols.

use crate::api::ingest::flight_ingest::FlightIngestService;
use crate::api::ingest::otlp::{export_request_to_arrow, OtlpReceiver};
use crate::api::query::flight_sql::FlightSqlQueryService;
use crate::api::telemetry::record_grpc_request;
use crate::ingester::Ingester;
use crate::query::QueryNode;
use crate::{Error, Result};

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{Command, CommandStatementQuery, SqlInfo, TicketStatementQuery};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::info_span;

type GrpcResult<T> = std::result::Result<T, Status>;
type GrpcStream<T> = Pin<Box<dyn Stream<Item = GrpcResult<T>> + Send + 'static>>;

const GRPC_SERVICE_OTLP_METRICS: &str = "opentelemetry.proto.collector.metrics.v1.MetricsService";
const GRPC_SERVICE_FLIGHT: &str = "arrow.flight.protocol.FlightService";
const GRPC_SERVICE_FLIGHT_SQL: &str = "arrow.flight.protocol.sql.FlightSqlService";

fn record_grpc_result<T>(
    service: &'static str,
    method: &'static str,
    start: Instant,
    result: &GrpcResult<Response<T>>,
) {
    let code = match result {
        Ok(_) => Code::Ok,
        Err(status) => status.code(),
    };
    record_grpc_request(service, method, code, start.elapsed().as_secs_f64());
}

/// Run ingester-side gRPC server (OTLP + Flight ingest).
pub async fn run_ingester_grpc_server(
    addr: SocketAddr,
    ingester: Arc<Ingester>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let otlp = OtlpGrpcService::new(ingester.clone());
    let flight = FlightIngestGrpcService::new(ingester);

    Server::builder()
        .add_service(MetricsServiceServer::new(otlp))
        .add_service(FlightServiceServer::new(flight))
        .serve_with_shutdown(addr, wait_for_shutdown(shutdown))
        .await
        .map_err(|e| Error::Internal(format!("Ingester gRPC server error: {e}")))?;

    Ok(())
}

/// Run query-side gRPC server (Flight SQL).
pub async fn run_query_grpc_server(
    addr: SocketAddr,
    query_node: Arc<QueryNode>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let flight_sql = FlightSqlGrpcService::new(query_node);

    Server::builder()
        .add_service(FlightServiceServer::new(flight_sql))
        .serve_with_shutdown(addr, wait_for_shutdown(shutdown))
        .await
        .map_err(|e| Error::Internal(format!("Query gRPC server error: {e}")))?;

    Ok(())
}

fn status_internal<E: std::fmt::Display>(err: E) -> Status {
    Status::internal(err.to_string())
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

/// OTLP MetricsService gRPC implementation.
pub struct OtlpGrpcService {
    receiver: OtlpReceiver,
}

impl OtlpGrpcService {
    pub fn new(ingester: Arc<Ingester>) -> Self {
        Self {
            receiver: OtlpReceiver::new(ingester),
        }
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> GrpcResult<Response<ExportMetricsServiceResponse>> {
        let start = Instant::now();
        let span = info_span!(
            "grpc.request",
            otel.kind = "server",
            rpc.system = "grpc",
            rpc.service = GRPC_SERVICE_OTLP_METRICS,
            rpc.method = "Export"
        );
        let _guard = span.enter();

        let result = async {
            let batch = export_request_to_arrow(request.get_ref()).map_err(|e| {
                Status::invalid_argument(format!("Invalid OTLP metrics payload: {e}"))
            })?;
            self.receiver.ingest(batch).await.map_err(status_internal)?;

            Ok(Response::new(ExportMetricsServiceResponse {
                partial_success: None,
            }))
        }
        .await;

        record_grpc_result(GRPC_SERVICE_OTLP_METRICS, "Export", start, &result);
        result
    }
}

/// Arrow Flight DoPut ingestion service.
pub struct FlightIngestGrpcService {
    ingest: Arc<FlightIngestService>,
}

impl FlightIngestGrpcService {
    pub fn new(ingester: Arc<Ingester>) -> Self {
        Self {
            ingest: Arc::new(FlightIngestService::new(ingester)),
        }
    }
}

#[tonic::async_trait]
impl FlightService for FlightIngestGrpcService {
    type HandshakeStream = GrpcStream<HandshakeResponse>;
    type ListFlightsStream = GrpcStream<FlightInfo>;
    type DoGetStream = GrpcStream<FlightData>;
    type DoPutStream = GrpcStream<PutResult>;
    type DoExchangeStream = GrpcStream<FlightData>;
    type DoActionStream = GrpcStream<arrow_flight::Result>;
    type ListActionsStream = GrpcStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> GrpcResult<Response<Self::HandshakeStream>> {
        let start = Instant::now();
        let result = Err(Status::unimplemented("Handshake is not implemented"));
        record_grpc_result(GRPC_SERVICE_FLIGHT, "Handshake", start, &result);
        result
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> GrpcResult<Response<Self::ListFlightsStream>> {
        let start = Instant::now();
        let empty: Self::ListFlightsStream =
            Box::pin(futures::stream::empty::<GrpcResult<FlightInfo>>());
        let result: GrpcResult<Response<Self::ListFlightsStream>> = Ok(Response::new(empty));
        record_grpc_result(GRPC_SERVICE_FLIGHT, "ListFlights", start, &result);
        result
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let start = Instant::now();
        let result = Err(Status::unimplemented(
            "Flight ingest server supports DoPut only",
        ));
        record_grpc_result(GRPC_SERVICE_FLIGHT, "GetFlightInfo", start, &result);
        result
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<PollInfo>> {
        let start = Instant::now();
        let result = Err(Status::unimplemented("PollFlightInfo is not implemented"));
        record_grpc_result(GRPC_SERVICE_FLIGHT, "PollFlightInfo", start, &result);
        result
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<SchemaResult>> {
        let start = Instant::now();
        let result = Err(Status::unimplemented("GetSchema is not implemented"));
        record_grpc_result(GRPC_SERVICE_FLIGHT, "GetSchema", start, &result);
        result
    }

    async fn do_get(&self, _request: Request<Ticket>) -> GrpcResult<Response<Self::DoGetStream>> {
        let start = Instant::now();
        let result = Err(Status::unimplemented("DoGet is not implemented"));
        record_grpc_result(GRPC_SERVICE_FLIGHT, "DoGet", start, &result);
        result
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> GrpcResult<Response<Self::DoPutStream>> {
        let start = Instant::now();
        let span = info_span!(
            "grpc.request",
            otel.kind = "server",
            rpc.system = "grpc",
            rpc.service = GRPC_SERVICE_FLIGHT,
            rpc.method = "DoPut"
        );
        let _guard = span.enter();

        let result = async {
            let mut stream = request.into_inner();
            let mut payload = Vec::new();
            while let Some(frame) = stream.next().await {
                payload.push(frame?);
            }

            let row_count = self
                .ingest
                .process_stream(payload.into_iter())
                .await
                .map_err(status_internal)?;

            let response = PutResult {
                app_metadata: Bytes::from(row_count.to_string()),
            };
            let out: Self::DoPutStream =
                Box::pin(futures::stream::iter(vec![Ok::<PutResult, Status>(
                    response,
                )]));
            Ok(Response::new(out))
        }
        .await;

        record_grpc_result(GRPC_SERVICE_FLIGHT, "DoPut", start, &result);
        result
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> GrpcResult<Response<Self::DoExchangeStream>> {
        let start = Instant::now();
        let result = Err(Status::unimplemented("DoExchange is not implemented"));
        record_grpc_result(GRPC_SERVICE_FLIGHT, "DoExchange", start, &result);
        result
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> GrpcResult<Response<Self::DoActionStream>> {
        let start = Instant::now();
        let result = Err(Status::unimplemented("DoAction is not implemented"));
        record_grpc_result(GRPC_SERVICE_FLIGHT, "DoAction", start, &result);
        result
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> GrpcResult<Response<Self::ListActionsStream>> {
        let start = Instant::now();
        let empty: Self::ListActionsStream =
            Box::pin(futures::stream::empty::<GrpcResult<ActionType>>());
        let result: GrpcResult<Response<Self::ListActionsStream>> = Ok(Response::new(empty));
        record_grpc_result(GRPC_SERVICE_FLIGHT, "ListActions", start, &result);
        result
    }
}

/// Flight SQL gRPC implementation for query execution.
pub struct FlightSqlGrpcService {
    service: Arc<FlightSqlQueryService>,
}

impl FlightSqlGrpcService {
    pub fn new(query_node: Arc<QueryNode>) -> Self {
        Self {
            service: Arc::new(FlightSqlQueryService::new(query_node)),
        }
    }
}

fn make_statement_ticket(query: &str) -> Ticket {
    let statement = TicketStatementQuery {
        statement_handle: Bytes::from(query.to_string()),
    };
    let any = Command::TicketStatementQuery(statement).into_any();
    Ticket::new(any.encode_to_vec())
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlGrpcService {
    type FlightService = Self;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let start = Instant::now();
        let span = info_span!(
            "grpc.request",
            otel.kind = "server",
            rpc.system = "grpc",
            rpc.service = GRPC_SERVICE_FLIGHT_SQL,
            rpc.method = "GetFlightInfoStatement"
        );
        let _guard = span.enter();

        let result = async {
            let ticket = make_statement_ticket(&query.query);
            let info = self
                .service
                .get_flight_info_with_ticket(&query.query, ticket)
                .await
                .map_err(status_internal)?;
            Ok(Response::new(info))
        }
        .await;

        record_grpc_result(
            GRPC_SERVICE_FLIGHT_SQL,
            "GetFlightInfoStatement",
            start,
            &result,
        );
        result
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let start = Instant::now();
        let span = info_span!(
            "grpc.request",
            otel.kind = "server",
            rpc.system = "grpc",
            rpc.service = GRPC_SERVICE_FLIGHT_SQL,
            rpc.method = "DoGetStatement"
        );
        let _guard = span.enter();

        let result = async {
            let query = String::from_utf8(ticket.statement_handle.to_vec())
                .map_err(|e| Status::invalid_argument(format!("Invalid statement handle: {e}")))?;
            let data = self
                .service
                .do_get(&Ticket::new(query.into_bytes()))
                .await
                .map_err(status_internal)?;
            let stream: <Self as FlightService>::DoGetStream =
                Box::pin(futures::stream::iter(data.into_iter().map(Ok)));
            Ok(Response::new(stream))
        }
        .await;

        record_grpc_result(GRPC_SERVICE_FLIGHT_SQL, "DoGetStatement", start, &result);
        result
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
