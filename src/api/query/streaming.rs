//! WebSocket/SSE streaming query support

use crate::api::ApiState;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

/// Streaming query request
#[derive(Debug, Deserialize)]
pub struct StreamRequest {
    pub query: String,
    #[serde(default)]
    pub live: bool,
}

/// Streaming data message
#[derive(Debug, Serialize)]
pub struct StreamMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub data: serde_json::Value,
}

/// WebSocket handler for streaming queries
pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

async fn handle_connection(mut socket: WebSocket, state: ApiState) {
    // Receive query request
    let request: StreamRequest = match socket.recv().await {
        Some(Ok(Message::Text(text))) => {
            match serde_json::from_str(&text) {
                Ok(req) => req,
                Err(e) => {
                    let _ = socket.send(Message::Text(
                        serde_json::to_string(&StreamMessage {
                            msg_type: "error".to_string(),
                            data: serde_json::json!({ "error": e.to_string() }),
                        }).unwrap()
                    )).await;
                    return;
                }
            }
        }
        _ => return,
    };

    // Execute historical query
    match state.query_node.query(&request.query).await {
        Ok(batches) => {
            for batch in batches {
                let json = batch_to_json(&batch);
                let msg = StreamMessage {
                    msg_type: "data".to_string(),
                    data: json,
                };

                if socket.send(Message::Text(serde_json::to_string(&msg).unwrap())).await.is_err() {
                    return;
                }
            }
        }
        Err(e) => {
            let _ = socket.send(Message::Text(
                serde_json::to_string(&StreamMessage {
                    msg_type: "error".to_string(),
                    data: serde_json::json!({ "error": e.to_string() }),
                }).unwrap()
            )).await;
            return;
        }
    }

    // Stream live data if requested
    if request.live {
        let mut rx = state.ingester.subscribe();

        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Ok(batch) => {
                            let json = batch_to_json(&batch);
                            let msg = StreamMessage {
                                msg_type: "data".to_string(),
                                data: json,
                            };

                            if socket.send(Message::Text(
                                serde_json::to_string(&msg).unwrap()
                            )).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // Subscriber lagged, continue
                            continue;
                        }
                        Err(_) => break,
                    }
                }
                msg = socket.recv() => {
                    match msg {
                        Some(Ok(Message::Close(_))) | None => break,
                        _ => continue,
                    }
                }
            }
        }
    }

    // Send end message
    let _ = socket.send(Message::Text(
        serde_json::to_string(&StreamMessage {
            msg_type: "end".to_string(),
            data: serde_json::json!({}),
        }).unwrap()
    )).await;
}

/// Convert RecordBatch to JSON
fn batch_to_json(batch: &arrow_array::RecordBatch) -> serde_json::Value {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_schema::DataType;

    let schema = batch.schema();
    let mut rows = Vec::new();

    for row_idx in 0..batch.num_rows() {
        let mut row = serde_json::Map::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let array = batch.column(col_idx);
            let value = if array.is_null(row_idx) {
                serde_json::Value::Null
            } else {
                match array.data_type() {
                    DataType::Int64 => {
                        serde_json::Value::Number(
                            array.as_primitive::<Int64Type>().value(row_idx).into()
                        )
                    }
                    DataType::Float64 => {
                        serde_json::Number::from_f64(
                            array.as_primitive::<Float64Type>().value(row_idx)
                        )
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                    }
                    DataType::Utf8 => {
                        serde_json::Value::String(
                            array.as_string::<i32>().value(row_idx).to_string()
                        )
                    }
                    DataType::Timestamp(_, _) => {
                        serde_json::Value::Number(
                            array.as_primitive::<TimestampNanosecondType>().value(row_idx).into()
                        )
                    }
                    _ => serde_json::Value::String(format!("{:?}", array.data_type())),
                }
            };

            row.insert(field.name().clone(), value);
        }

        rows.push(serde_json::Value::Object(row));
    }

    serde_json::Value::Array(rows)
}
