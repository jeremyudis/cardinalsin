//! gRPC services for OTLP and Arrow Flight protocols.

use crate::api::ingest::flight_ingest::FlightIngestService;
use crate::api::ingest::otlp::{export_request_to_arrow, OtlpReceiver};
use crate::api::query::flight_sql::FlightSqlQueryService;
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
use tokio::sync::watch;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

type GrpcResult<T> = std::result::Result<T, Status>;
type GrpcStream<T> = Pin<Box<dyn Stream<Item = GrpcResult<T>> + Send + 'static>>;

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
        let batch = export_request_to_arrow(request.get_ref())
            .map_err(|e| Status::invalid_argument(format!("Invalid OTLP metrics payload: {e}")))?;
        self.receiver.ingest(batch).await.map_err(status_internal)?;

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
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
        Err(Status::unimplemented("Handshake is not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> GrpcResult<Response<Self::ListFlightsStream>> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        Err(Status::unimplemented(
            "Flight ingest server supports DoPut only",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<PollInfo>> {
        Err(Status::unimplemented("PollFlightInfo is not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<SchemaResult>> {
        Err(Status::unimplemented("GetSchema is not implemented"))
    }

    async fn do_get(&self, _request: Request<Ticket>) -> GrpcResult<Response<Self::DoGetStream>> {
        Err(Status::unimplemented("DoGet is not implemented"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> GrpcResult<Response<Self::DoPutStream>> {
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
        let out = futures::stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(out)))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> GrpcResult<Response<Self::DoExchangeStream>> {
        Err(Status::unimplemented("DoExchange is not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> GrpcResult<Response<Self::DoActionStream>> {
        Err(Status::unimplemented("DoAction is not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> GrpcResult<Response<Self::ListActionsStream>> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
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
        let ticket = make_statement_ticket(&query.query);
        let info = self
            .service
            .get_flight_info_with_ticket(&query.query, ticket)
            .await
            .map_err(status_internal)?;
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let query = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid statement handle: {e}")))?;
        let data = self
            .service
            .do_get(&Ticket::new(query.into_bytes()))
            .await
            .map_err(status_internal)?;
        let stream = futures::stream::iter(data.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
