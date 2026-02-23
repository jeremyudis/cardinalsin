//! gRPC services for OTLP and Arrow Flight protocols.

use crate::api::ingest::flight_ingest::FlightIngestService;
use crate::api::ingest::otlp::{export_request_to_arrow, OtlpReceiver};
use crate::api::query::flight_sql::{batches_to_flight_data, FlightSqlQueryService};
use crate::ingester::Ingester;
use crate::query::QueryNode;
use crate::{Error, Result};

use arrow_array::{Array, LargeStringArray, StringArray};
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{
    SqlInfoData, SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder,
};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    self, ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, Command, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementIngest,
    CommandStatementQuery, CommandStatementSubstraitPlan, CommandStatementUpdate,
    DoPutPreparedStatementResult, Nullable, Searchable, SqlInfo, SqlSupportedTransaction,
    TicketStatementQuery, XdbcDataType,
};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Schema};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

type GrpcResult<T> = std::result::Result<T, Status>;
type HelperResult<T> = std::result::Result<T, Box<Status>>;
type GrpcStream<T> = Pin<Box<dyn Stream<Item = GrpcResult<T>> + Send + 'static>>;

const DEFAULT_FLIGHT_SQL_CATALOG: &str = "default";
const DEFAULT_FLIGHT_SQL_SCHEMA: &str = "public";

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
    let flight_sql = Arc::new(FlightSqlGrpcService::new(query_node));
    let flight_service = FlightSqlFlightService::new(flight_sql);

    Server::builder()
        .add_service(FlightServiceServer::new(flight_service))
        .serve_with_shutdown(addr, wait_for_shutdown(shutdown))
        .await
        .map_err(|e| Error::Internal(format!("Query gRPC server error: {e}")))?;

    Ok(())
}

fn status_internal<E: std::fmt::Display>(err: E) -> Status {
    Status::internal(err.to_string())
}

fn status_capability(name: &str) -> Status {
    Status::failed_precondition(format!("Capability unavailable: {name}"))
}

fn flight_data_stream_response(data: Vec<FlightData>) -> Response<GrpcStream<FlightData>> {
    let stream = futures::stream::iter(data.into_iter().map(Ok));
    Response::new(Box::pin(stream))
}

fn schema_result(schema: &Schema) -> HelperResult<SchemaResult> {
    let options = arrow_ipc::writer::IpcWriteOptions::default();
    SchemaAsIpc::new(schema, &options)
        .try_into()
        .map_err(|e| Box::new(status_internal(e)))
}

fn schema_bytes(schema: &Schema) -> HelperResult<Bytes> {
    let options = arrow_ipc::writer::IpcWriteOptions::default();
    let ipc: arrow_flight::IpcMessage = SchemaAsIpc::new(schema, &options)
        .try_into()
        .map_err(|e| Box::new(status_internal(e)))?;
    Ok(ipc.0)
}

fn flight_info_for_schema(schema: &Schema, ticket: Ticket) -> HelperResult<FlightInfo> {
    FlightInfo::new()
        .try_with_schema(schema)
        .map(|info| info.with_endpoint(FlightEndpoint::new().with_ticket(ticket)))
        .map_err(|e| Box::new(status_internal(e)))
}

fn command_ticket(command: Command) -> Ticket {
    Ticket::new(command.into_any().encode_to_vec())
}

fn string_value(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|col| col.value(row).to_string()),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|col| col.value(row).to_string()),
        _ => None,
    }
}

fn decode_handle(bytes: &[u8], kind: &str) -> HelperResult<String> {
    String::from_utf8(bytes.to_vec()).map_err(|e| {
        Box::new(Status::invalid_argument(format!(
            "Invalid {kind} handle: {e}"
        )))
    })
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

    fn ingest_descriptor() -> FlightDescriptor {
        FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: Bytes::new(),
            path: vec!["ingest".to_string(), "doput".to_string()],
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
        request: Request<Streaming<HandshakeRequest>>,
    ) -> GrpcResult<Response<Self::HandshakeStream>> {
        let mut stream = request.into_inner();
        let first = stream.next().await.transpose()?;
        let response = HandshakeResponse {
            protocol_version: first.as_ref().map(|v| v.protocol_version).unwrap_or(0),
            payload: first.map(|v| v.payload).unwrap_or_default(),
        };
        let out = futures::stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(out)))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> GrpcResult<Response<Self::ListFlightsStream>> {
        let ticket = Ticket::new(Bytes::from_static(b"ingest:doput"));
        let mut info =
            flight_info_for_schema(&Schema::empty(), ticket).map_err(|status| *status)?;
        info.flight_descriptor = Some(Self::ingest_descriptor());
        let stream = futures::stream::iter(vec![Ok(info)]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let mut info = flight_info_for_schema(
            &Schema::empty(),
            Ticket::new(Bytes::from_static(b"ingest:doput")),
        )
        .map_err(|status| *status)?;
        info.flight_descriptor = Some(request.into_inner());
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<PollInfo>> {
        let mut info = flight_info_for_schema(
            &Schema::empty(),
            Ticket::new(Bytes::from_static(b"ingest:doput")),
        )
        .map_err(|status| *status)?;
        info.flight_descriptor = Some(request.get_ref().clone());
        Ok(Response::new(PollInfo {
            info: Some(info),
            flight_descriptor: None,
            progress: Some(1.0),
            expiration_time: None,
        }))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<SchemaResult>> {
        Ok(Response::new(
            schema_result(&Schema::empty()).map_err(|status| *status)?,
        ))
    }

    async fn do_get(&self, _request: Request<Ticket>) -> GrpcResult<Response<Self::DoGetStream>> {
        Err(status_capability("flight_ingest.do_get"))
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
        Err(status_capability("flight_ingest.do_exchange"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> GrpcResult<Response<Self::DoActionStream>> {
        Err(status_capability("flight_ingest.do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> GrpcResult<Response<Self::ListActionsStream>> {
        let actions = vec![Ok(ActionType {
            r#type: "flight.ingest.doput".to_string(),
            description: "Bulk ingestion via DoPut".to_string(),
        })];
        Ok(Response::new(Box::pin(futures::stream::iter(actions))))
    }
}

/// Flight SQL gRPC implementation for query execution.
pub struct FlightSqlGrpcService {
    service: Arc<FlightSqlQueryService>,
    prepared_statements: Arc<RwLock<HashMap<String, String>>>,
    transactions: Arc<RwLock<HashSet<String>>>,
    savepoints: Arc<RwLock<HashMap<String, String>>>,
    registered_sql_info: Arc<RwLock<HashMap<u32, i32>>>,
    id_seq: AtomicU64,
}

impl FlightSqlGrpcService {
    pub fn new(query_node: Arc<QueryNode>) -> Self {
        Self {
            service: Arc::new(FlightSqlQueryService::new(query_node)),
            prepared_statements: Arc::new(RwLock::new(HashMap::new())),
            transactions: Arc::new(RwLock::new(HashSet::new())),
            savepoints: Arc::new(RwLock::new(HashMap::new())),
            registered_sql_info: Arc::new(RwLock::new(HashMap::new())),
            id_seq: AtomicU64::new(1),
        }
    }

    fn next_handle(&self, prefix: &str) -> String {
        let id = self.id_seq.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{id}")
    }

    async fn resolve_prepared_query(&self, handle: &[u8]) -> GrpcResult<String> {
        let key = decode_handle(handle, "prepared statement").map_err(|status| *status)?;
        self.prepared_statements
            .read()
            .await
            .get(&key)
            .cloned()
            .ok_or_else(|| Status::invalid_argument("Unknown prepared statement handle"))
    }

    async fn query_string_rows(
        &self,
        sql: &str,
        expected_cols: usize,
    ) -> GrpcResult<Vec<Vec<Option<String>>>> {
        let batches = self
            .service
            .execute_batches(sql)
            .await
            .map_err(status_internal)?;
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let mut values = Vec::with_capacity(expected_cols);
                for col in 0..expected_cols {
                    let value = batch
                        .columns()
                        .get(col)
                        .and_then(|array| string_value(array.as_ref(), row));
                    values.push(value);
                }
                rows.push(values);
            }
        }
        Ok(rows)
    }

    async fn build_sql_info_data(&self) -> GrpcResult<SqlInfoData> {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "CardinalSin");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        builder.append(SqlInfo::FlightSqlServerArrowVersion, "53.x");
        builder.append(SqlInfo::FlightSqlServerReadOnly, false);
        builder.append(SqlInfo::FlightSqlServerSql, true);
        builder.append(SqlInfo::FlightSqlServerSubstrait, false);
        builder.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::None as i32,
        );
        builder.append(SqlInfo::FlightSqlServerCancel, false);
        builder.append(SqlInfo::FlightSqlServerBulkIngestion, false);
        builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
        builder.append(
            SqlInfo::SqlKeywords,
            &["SELECT", "FROM", "WHERE"] as &[&str],
        );

        let extras = self.registered_sql_info.read().await;
        for (id, value) in extras.iter() {
            builder.append(*id, *value);
        }

        builder.build().map_err(status_internal)
    }

    fn build_xdbc_type_info_data() -> HelperResult<XdbcTypeInfoData> {
        let mut builder = XdbcTypeInfoDataBuilder::new();
        builder.append(XdbcTypeInfo {
            type_name: "VARCHAR".to_string(),
            data_type: XdbcDataType::XdbcVarchar,
            column_size: Some(i32::MAX),
            literal_prefix: Some("'".to_string()),
            literal_suffix: Some("'".to_string()),
            create_params: Some(vec!["length".to_string()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: true,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("VARCHAR".to_string()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcVarchar,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        });
        builder.append(XdbcTypeInfo {
            type_name: "DOUBLE".to_string(),
            data_type: XdbcDataType::XdbcDouble,
            column_size: Some(53),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("DOUBLE".to_string()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcDouble,
            datetime_subcode: None,
            num_prec_radix: Some(2),
            interval_precision: None,
        });
        builder.append(XdbcTypeInfo {
            type_name: "BIGINT".to_string(),
            data_type: XdbcDataType::XdbcBigint,
            column_size: Some(64),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("BIGINT".to_string()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcBigint,
            datetime_subcode: None,
            num_prec_radix: Some(2),
            interval_precision: None,
        });
        builder.build().map_err(|e| Box::new(status_internal(e)))
    }
}

/// Wrapper around `FlightSqlGrpcService` to provide explicit implementations for
/// Flight service calls that are not customizable through the Flight SQL trait.
pub struct FlightSqlFlightService {
    inner: Arc<FlightSqlGrpcService>,
}

impl FlightSqlFlightService {
    fn new(inner: Arc<FlightSqlGrpcService>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl FlightService for FlightSqlFlightService {
    type HandshakeStream = GrpcStream<HandshakeResponse>;
    type ListFlightsStream = GrpcStream<FlightInfo>;
    type DoGetStream = GrpcStream<FlightData>;
    type DoPutStream = GrpcStream<PutResult>;
    type DoExchangeStream = GrpcStream<FlightData>;
    type DoActionStream = GrpcStream<arrow_flight::Result>;
    type ListActionsStream = GrpcStream<ActionType>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> GrpcResult<Response<Self::HandshakeStream>> {
        <FlightSqlGrpcService as FlightService>::handshake(self.inner.as_ref(), request).await
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> GrpcResult<Response<Self::ListFlightsStream>> {
        let query = "SELECT 1 AS one";
        let info = self
            .inner
            .service
            .get_flight_info_with_ticket(query, make_statement_ticket(query))
            .await
            .map_err(status_internal)?;
        let stream = futures::stream::iter(vec![Ok(info)]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        <FlightSqlGrpcService as FlightService>::get_flight_info(self.inner.as_ref(), request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<PollInfo>> {
        let descriptor = request.into_inner();
        let info = <FlightSqlFlightService as FlightService>::get_flight_info(
            self,
            Request::new(descriptor.clone()),
        )
        .await?
        .into_inner();

        Ok(Response::new(PollInfo {
            info: Some(info),
            flight_descriptor: None,
            progress: Some(1.0),
            expiration_time: None,
        }))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<SchemaResult>> {
        let descriptor = request.into_inner();
        let msg = Any::decode(&*descriptor.cmd)
            .map_err(|e| Status::invalid_argument(format!("Invalid descriptor command: {e}")))?;

        let schema = match Command::try_from(msg).map_err(status_internal)? {
            Command::CommandStatementQuery(query) => self
                .inner
                .service
                .analyze_schema(&query.query)
                .await
                .map_err(status_internal)?,
            Command::CommandPreparedStatementQuery(query) => {
                let sql = self
                    .inner
                    .resolve_prepared_query(&query.prepared_statement_handle)
                    .await?;
                self.inner
                    .service
                    .analyze_schema(&sql)
                    .await
                    .map_err(status_internal)?
            }
            Command::CommandGetCatalogs(query) => query.into_builder().schema().as_ref().clone(),
            Command::CommandGetDbSchemas(query) => query.into_builder().schema().as_ref().clone(),
            Command::CommandGetTables(query) => query.into_builder().schema().as_ref().clone(),
            Command::CommandGetTableTypes(query) => query.into_builder().schema().as_ref().clone(),
            Command::CommandGetSqlInfo(query) => {
                let info_data = self.inner.build_sql_info_data().await?;
                query.into_builder(&info_data).schema().as_ref().clone()
            }
            Command::CommandGetXdbcTypeInfo(_) => FlightSqlGrpcService::build_xdbc_type_info_data()
                .map_err(|status| *status)?
                .schema()
                .as_ref()
                .clone(),
            Command::CommandGetPrimaryKeys(_)
            | Command::CommandGetExportedKeys(_)
            | Command::CommandGetImportedKeys(_)
            | Command::CommandGetCrossReference(_)
            | Command::CommandStatementSubstraitPlan(_)
            | Command::Unknown(_) => return Err(Status::failed_precondition("Schema unavailable")),
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unsupported schema command: {}",
                    other.type_url()
                )))
            }
        };

        Ok(Response::new(
            schema_result(&schema).map_err(|status| *status)?,
        ))
    }

    async fn do_get(&self, request: Request<Ticket>) -> GrpcResult<Response<Self::DoGetStream>> {
        <FlightSqlGrpcService as FlightService>::do_get(self.inner.as_ref(), request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> GrpcResult<Response<Self::DoPutStream>> {
        <FlightSqlGrpcService as FlightService>::do_put(self.inner.as_ref(), request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> GrpcResult<Response<Self::DoExchangeStream>> {
        <FlightSqlGrpcService as FlightService>::do_exchange(self.inner.as_ref(), request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> GrpcResult<Response<Self::DoActionStream>> {
        <FlightSqlGrpcService as FlightService>::do_action(self.inner.as_ref(), request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> GrpcResult<Response<Self::ListActionsStream>> {
        <FlightSqlGrpcService as FlightService>::list_actions(self.inner.as_ref(), request).await
    }
}

fn make_statement_ticket(query: &str) -> Ticket {
    let statement = TicketStatementQuery {
        statement_handle: Bytes::from(query.to_string()),
    };
    command_ticket(Command::TicketStatementQuery(statement))
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlGrpcService {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> GrpcResult<Response<GrpcStream<HandshakeResponse>>> {
        let mut stream = request.into_inner();
        let first = stream.next().await.transpose()?;
        let response = HandshakeResponse {
            protocol_version: first.as_ref().map(|v| v.protocol_version).unwrap_or(0),
            payload: first.map(|v| v.payload).unwrap_or_default(),
        };
        let out = futures::stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(out)))
    }

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

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        Err(status_capability("flightsql.substrait"))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let sql = self
            .resolve_prepared_query(&query.prepared_statement_handle)
            .await?;
        let ticket = command_ticket(Command::CommandPreparedStatementQuery(query));
        let info = self
            .service
            .get_flight_info_with_ticket(&sql, ticket)
            .await
            .map_err(status_internal)?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: sql::CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let schema = query.into_builder().schema();
        let ticket = command_ticket(Command::CommandGetCatalogs(query));
        Ok(Response::new(
            flight_info_for_schema(schema.as_ref(), ticket).map_err(|status| *status)?,
        ))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let schema = query.clone().into_builder().schema();
        let ticket = command_ticket(Command::CommandGetDbSchemas(query));
        Ok(Response::new(
            flight_info_for_schema(schema.as_ref(), ticket).map_err(|status| *status)?,
        ))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let schema = query.clone().into_builder().schema();
        let ticket = command_ticket(Command::CommandGetTables(query));
        Ok(Response::new(
            flight_info_for_schema(schema.as_ref(), ticket).map_err(|status| *status)?,
        ))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let schema = query.into_builder().schema();
        let ticket = command_ticket(Command::CommandGetTableTypes(query));
        Ok(Response::new(
            flight_info_for_schema(schema.as_ref(), ticket).map_err(|status| *status)?,
        ))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let info_data = self.build_sql_info_data().await?;
        let schema = query.clone().into_builder(&info_data).schema();
        let ticket = command_ticket(Command::CommandGetSqlInfo(query));
        Ok(Response::new(
            flight_info_for_schema(schema.as_ref(), ticket).map_err(|status| *status)?,
        ))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        Err(status_capability("flightsql.primary_keys"))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        Err(status_capability("flightsql.exported_keys"))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        Err(status_capability("flightsql.imported_keys"))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        Err(status_capability("flightsql.cross_reference"))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        let data = Self::build_xdbc_type_info_data().map_err(|status| *status)?;
        let schema = data.schema();
        let ticket = command_ticket(Command::CommandGetXdbcTypeInfo(query));
        Ok(Response::new(
            flight_info_for_schema(schema.as_ref(), ticket).map_err(|status| *status)?,
        ))
    }

    async fn get_flight_info_fallback(
        &self,
        cmd: Command,
        _request: Request<FlightDescriptor>,
    ) -> GrpcResult<Response<FlightInfo>> {
        Err(Status::invalid_argument(format!(
            "Unsupported Flight SQL command: {}",
            cmd.type_url()
        )))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let query =
            decode_handle(&ticket.statement_handle, "statement").map_err(|status| *status)?;
        let batches = self
            .service
            .execute_batches(&query)
            .await
            .map_err(status_internal)?;
        let data = batches_to_flight_data(batches).map_err(status_internal)?;
        Ok(flight_data_stream_response(data))
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let sql = self
            .resolve_prepared_query(&query.prepared_statement_handle)
            .await?;
        let batches = self
            .service
            .execute_batches(&sql)
            .await
            .map_err(status_internal)?;
        let data = batches_to_flight_data(batches).map_err(status_internal)?;
        Ok(flight_data_stream_response(data))
    }

    async fn do_get_catalogs(
        &self,
        query: sql::CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let rows = self
            .query_string_rows(
                "SELECT DISTINCT table_catalog FROM information_schema.tables ORDER BY table_catalog",
                1,
            )
            .await?;
        let mut builder = query.into_builder();
        if rows.is_empty() {
            builder.append(DEFAULT_FLIGHT_SQL_CATALOG);
        } else {
            for row in rows {
                builder.append(
                    row.first()
                        .and_then(|v| v.clone())
                        .unwrap_or_else(|| DEFAULT_FLIGHT_SQL_CATALOG.to_string()),
                );
            }
        }
        let batch = builder.build().map_err(status_internal)?;
        let data = batches_to_flight_data(vec![batch]).map_err(status_internal)?;
        Ok(flight_data_stream_response(data))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let rows = self
            .query_string_rows(
                "SELECT DISTINCT table_catalog, table_schema FROM information_schema.tables \
                 ORDER BY table_catalog, table_schema",
                2,
            )
            .await?;
        let mut builder = query.into_builder();
        if rows.is_empty() {
            builder.append(DEFAULT_FLIGHT_SQL_CATALOG, DEFAULT_FLIGHT_SQL_SCHEMA);
        } else {
            for row in rows {
                let catalog = row
                    .first()
                    .and_then(|v| v.clone())
                    .unwrap_or_else(|| DEFAULT_FLIGHT_SQL_CATALOG.to_string());
                let schema = row
                    .get(1)
                    .and_then(|v| v.clone())
                    .unwrap_or_else(|| DEFAULT_FLIGHT_SQL_SCHEMA.to_string());
                builder.append(catalog, schema);
            }
        }
        let batch = builder.build().map_err(status_internal)?;
        let data = batches_to_flight_data(vec![batch]).map_err(status_internal)?;
        Ok(flight_data_stream_response(data))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let rows = self
            .query_string_rows(
                "SELECT table_catalog, table_schema, table_name, table_type \
                 FROM information_schema.tables ORDER BY table_catalog, table_schema, table_name",
                4,
            )
            .await?;
        let mut builder = query.into_builder();
        if rows.is_empty() {
            builder
                .append(
                    DEFAULT_FLIGHT_SQL_CATALOG,
                    DEFAULT_FLIGHT_SQL_SCHEMA,
                    "metrics",
                    "TABLE",
                    &Schema::empty(),
                )
                .map_err(status_internal)?;
        } else {
            for row in rows {
                let catalog = row
                    .first()
                    .and_then(|v| v.clone())
                    .unwrap_or_else(|| DEFAULT_FLIGHT_SQL_CATALOG.to_string());
                let schema_name = row
                    .get(1)
                    .and_then(|v| v.clone())
                    .unwrap_or_else(|| DEFAULT_FLIGHT_SQL_SCHEMA.to_string());
                let table_name = row
                    .get(2)
                    .and_then(|v| v.clone())
                    .unwrap_or_else(|| "metrics".to_string());
                let table_type = row
                    .get(3)
                    .and_then(|v| v.clone())
                    .unwrap_or_else(|| "TABLE".to_string());

                let table_schema = {
                    let escaped_schema = schema_name.replace('"', "\"\"");
                    let escaped_table = table_name.replace('"', "\"\"");
                    let analyze_sql =
                        format!("SELECT * FROM \"{escaped_schema}\".\"{escaped_table}\" LIMIT 0");
                    self.service
                        .analyze_schema(&analyze_sql)
                        .await
                        .unwrap_or_else(|_| Schema::empty())
                };

                builder
                    .append(catalog, schema_name, table_name, table_type, &table_schema)
                    .map_err(status_internal)?;
            }
        }

        let batch = builder.build().map_err(status_internal)?;
        let data = batches_to_flight_data(vec![batch]).map_err(status_internal)?;
        Ok(flight_data_stream_response(data))
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let mut builder = query.into_builder();
        builder.append("TABLE");
        builder.append("VIEW");
        builder.append("SYSTEM TABLE");
        let batch = builder.build().map_err(status_internal)?;
        let data = batches_to_flight_data(vec![batch]).map_err(status_internal)?;
        Ok(flight_data_stream_response(data))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let info_data = self.build_sql_info_data().await?;
        let batch = query
            .into_builder(&info_data)
            .build()
            .map_err(status_internal)?;
        let data = batches_to_flight_data(vec![batch]).map_err(status_internal)?;
        Ok(flight_data_stream_response(data))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        Err(status_capability("flightsql.primary_keys"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        Err(status_capability("flightsql.exported_keys"))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        Err(status_capability("flightsql.imported_keys"))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        Err(status_capability("flightsql.cross_reference"))
    }

    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        let data = Self::build_xdbc_type_info_data().map_err(|status| *status)?;
        let batch = data
            .record_batch(query.data_type)
            .map_err(status_internal)?;
        let data = batches_to_flight_data(vec![batch]).map_err(status_internal)?;
        Ok(flight_data_stream_response(data))
    }

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        message: Any,
    ) -> GrpcResult<Response<<Self as FlightService>::DoGetStream>> {
        Err(Status::invalid_argument(format!(
            "Unsupported do_get ticket type: {}",
            message.type_url
        )))
    }

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> GrpcResult<i64> {
        let batches = self
            .service
            .execute_batches(&ticket.query)
            .await
            .map_err(status_internal)?;
        let rows = batches.iter().map(|b| b.num_rows() as i64).sum();
        Ok(rows)
    }

    async fn do_put_statement_ingest(
        &self,
        _ticket: CommandStatementIngest,
        _request: Request<PeekableFlightDataStream>,
    ) -> GrpcResult<i64> {
        Err(status_capability("flightsql.statement_ingest"))
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> GrpcResult<DoPutPreparedStatementResult> {
        let _ = self
            .resolve_prepared_query(&query.prepared_statement_handle)
            .await?;

        let mut stream = request.into_inner();
        while let Some(frame) = stream.next().await {
            frame?;
        }

        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(query.prepared_statement_handle),
        })
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> GrpcResult<i64> {
        let sql = self
            .resolve_prepared_query(&query.prepared_statement_handle)
            .await?;
        let batches = self
            .service
            .execute_batches(&sql)
            .await
            .map_err(status_internal)?;
        let rows = batches.iter().map(|b| b.num_rows() as i64).sum();
        Ok(rows)
    }

    async fn do_put_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> GrpcResult<i64> {
        Err(status_capability("flightsql.substrait"))
    }

    async fn do_put_fallback(
        &self,
        _request: Request<PeekableFlightDataStream>,
        message: Any,
    ) -> GrpcResult<Response<<Self as FlightService>::DoPutStream>> {
        Err(Status::invalid_argument(format!(
            "Unsupported do_put command type: {}",
            message.type_url
        )))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> GrpcResult<ActionCreatePreparedStatementResult> {
        let prepared = self
            .service
            .create_prepared_statement(&query.query)
            .await
            .map_err(status_internal)?;
        self.prepared_statements
            .write()
            .await
            .insert(prepared.handle.clone(), prepared.query.clone());

        let dataset_schema = self
            .service
            .analyze_schema(&prepared.query)
            .await
            .map_err(status_internal)?;

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: Bytes::from(prepared.handle),
            dataset_schema: schema_bytes(&dataset_schema).map_err(|status| *status)?,
            parameter_schema: schema_bytes(&Schema::empty()).map_err(|status| *status)?,
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> GrpcResult<()> {
        let handle = decode_handle(&query.prepared_statement_handle, "prepared statement")
            .map_err(|status| *status)?;
        self.prepared_statements.write().await.remove(&handle);
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> GrpcResult<ActionCreatePreparedStatementResult> {
        Err(status_capability("flightsql.substrait"))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> GrpcResult<ActionBeginTransactionResult> {
        let tx_id = self.next_handle("tx");
        self.transactions.write().await.insert(tx_id.clone());
        Ok(ActionBeginTransactionResult {
            transaction_id: Bytes::from(tx_id),
        })
    }

    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> GrpcResult<()> {
        let tx_id =
            decode_handle(&query.transaction_id, "transaction").map_err(|status| *status)?;
        let removed = self.transactions.write().await.remove(&tx_id);
        if !removed {
            return Err(Status::invalid_argument("Unknown transaction handle"));
        }
        self.savepoints
            .write()
            .await
            .retain(|_, parent_tx| parent_tx != &tx_id);
        Ok(())
    }

    async fn do_action_begin_savepoint(
        &self,
        query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> GrpcResult<ActionBeginSavepointResult> {
        let tx_id =
            decode_handle(&query.transaction_id, "transaction").map_err(|status| *status)?;
        if !self.transactions.read().await.contains(&tx_id) {
            return Err(Status::invalid_argument("Unknown transaction handle"));
        }

        let savepoint = if query.name.is_empty() {
            self.next_handle("sp")
        } else {
            format!("{}-{}", query.name, self.next_handle("sp"))
        };
        self.savepoints
            .write()
            .await
            .insert(savepoint.clone(), tx_id);
        Ok(ActionBeginSavepointResult {
            savepoint_id: Bytes::from(savepoint),
        })
    }

    async fn do_action_end_savepoint(
        &self,
        query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> GrpcResult<()> {
        let savepoint =
            decode_handle(&query.savepoint_id, "savepoint").map_err(|status| *status)?;
        let removed = self.savepoints.write().await.remove(&savepoint);
        if removed.is_none() {
            return Err(Status::invalid_argument("Unknown savepoint handle"));
        }
        Ok(())
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> GrpcResult<ActionCancelQueryResult> {
        Ok(ActionCancelQueryResult {
            result: 3, // CANCEL_RESULT_NOT_CANCELLABLE
        })
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoActionStream>> {
        Err(Status::invalid_argument(format!(
            "Unsupported action type: {}",
            request.get_ref().r#type
        )))
    }

    async fn do_exchange_fallback(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> GrpcResult<Response<<Self as FlightService>::DoExchangeStream>> {
        Err(status_capability("flightsql.do_exchange"))
    }

    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {
        if id < 0 {
            return;
        }
        self.registered_sql_info
            .write()
            .await
            .insert(id as u32, i32::from(*result));
    }
}
