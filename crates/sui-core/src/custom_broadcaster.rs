use crate::transaction_outputs::TransactionOutputs;
use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, net::SocketAddr, sync::Arc};
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    transaction::TransactionDataAPI, // Kept if needed for trait bounds, but suppressing warning if unused
};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

// --- Data Structures ---

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(tag = "type")]
pub enum SubscriptionRequest {
    #[serde(rename = "subscribe_pool")]
    SubscribePool { pool_id: ObjectID },
    #[serde(rename = "subscribe_account")]
    SubscribeAccount { account: SuiAddress },
    #[serde(rename = "subscribe_all")]
    SubscribeAll,
    #[serde(rename = "query_field_range")]
    QueryFieldRange {
        table_id: ObjectID,
        current_index: u64,
        range: u64,
        parent_version: Option<u64>,
    },
}

// ... (StreamMessage and AppState remain unchanged, I will skip them in replacement if possible, but I need to target the enum first)
// actually I'll target the whole file content from line 22 to end of handle_socket if easier, or use chunks.
// Chunks are better.

// Chunk 1: Enum update
// Chunk 2: handle_socket rewrite

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type")]
pub enum StreamMessage {
    #[serde(rename = "pool_update")]
    PoolUpdate {
        pool_id: ObjectID,
        digest: String,
        object: Option<Vec<u8>>,
    },
    #[serde(rename = "account_activity")]
    AccountActivity {
        account: SuiAddress,
        digest: String,
        kind: String, // e.g., "Swap", "Transfer"
    },
    #[serde(rename = "balance_change")]
    BalanceChange {
        account: SuiAddress,
        coin_type: String,
        new_balance: u64,
    },
    #[serde(rename = "event")]
    Event {
        package_id: ObjectID,
        transaction_module: String,
        sender: SuiAddress,
        type_: String,
        contents: Vec<u8>,
        digest: String,
    },
    #[serde(rename = "field_data")]
    FieldData {
        table_id: ObjectID,
        index: u64,
        field_id: ObjectID,
        bcs_bytes: Vec<u8>,
        version: u64,
    },
    #[serde(rename = "query_complete")]
    QueryComplete {
        table_id: ObjectID,
        total_fields: usize,
    },
    #[serde(rename = "error")]
    Error { message: String },
    // Raw output for advanced filtering
    #[serde(rename = "raw")]
    Raw(SerializableOutput),
}

#[derive(Clone, Debug, Serialize)]
pub struct SerializableOutput {
    digest: String,
    timestamp_ms: u64,
}

// --- Broadcaster State ---

use crate::authority::AuthorityStore;

struct AppState {
    tx: broadcast::Sender<Arc<TransactionOutputs>>,
    store: Option<Arc<AuthorityStore>>,
}

// --- Main Broadcaster Logic ---

pub struct CustomBroadcaster;

impl CustomBroadcaster {
    pub fn spawn(
        mut rx: mpsc::Receiver<Arc<TransactionOutputs>>,
        port: u16,
        store: Option<Arc<AuthorityStore>>,
    ) {
        // Create a broadcast channel for all connected websocket clients
        // Capacity 1000 to handle bursts
        let (tx, _) = broadcast::channel(1000);
        let tx_clone = tx.clone();

        // 1. Spawn the ingestion loop
        tokio::spawn(async move {
            info!("CustomBroadcaster: Ingestion loop started");
            while let Some(outputs) = rx.recv().await {
                // Determine if this output is "interesting" before broadcasting?
                // Or broadcast everything and let per-client filters handle it?
                // For low latency, we broadcast raw or minimally processed data.

                // We broadcast the Arc directly to avoid cloning the heavy data structure.
                // The serialization happens in the client handling task.
                if let Err(e) = tx_clone.send(outputs) {
                    debug!(
                        "CustomBroadcaster: No active subscribers, dropped message: {}",
                        e
                    );
                }
            }
            info!("CustomBroadcaster: Ingestion loop ended");
        });

        // 2. Spawn the WebServer
        let app_state = Arc::new(AppState { tx, store });

        tokio::spawn(async move {
            let app = Router::new()
                .route("/ws", get(ws_handler))
                .with_state(app_state);

            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            info!("CustomBroadcaster: Listening on {}", addr);

            // Fix for new Axum version: use tokio::net::TcpListener
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    if let Err(e) = axum::serve(listener, app.into_make_service()).await {
                        error!("CustomBroadcaster: Server error: {}", e);
                    }
                }
                Err(e) => {
                    error!("CustomBroadcaster: Failed to bind to address: {}", e);
                }
            }
        });
    }
}

// --- WebSocket Handling ---

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.tx.subscribe();

    let mut subscriptions_pools = HashSet::new();
    let mut subscriptions_accounts = HashSet::new();
    let mut subscribe_all = false;

    loop {
        tokio::select! {
            // Outbound: Send updates to client
            res = rx.recv() => {
                match res {
                    Ok(outputs) => {
                         let digest = outputs.transaction.digest();
                         // We track if we sent anything to avoid noise or filtered logic if needed,
                         // but for now we just process all independent categories.

                         // Debug Logging [Added for Verification]
                         let sender = outputs.transaction.sender_address();
                         info!("CustomBroadcaster: Processing Tx {} from Sender {} (AccSubs: {}, PoolSubs: {})",
                             digest,
                             sender,
                             subscriptions_accounts.len(),
                             subscriptions_pools.len()
                         );

                         // 1. Firehose / SubscribeAll Events (Optional, can be heavy)
                         if subscribe_all {
                             // Account Activity (Sender)
                             let sender = outputs.transaction.sender_address();
                             let msg = StreamMessage::AccountActivity {
                                 account: sender,
                                 digest: digest.to_string(),
                                 kind: "Transaction".to_string(),
                             };
                             if let Err(_) = send_json(&mut socket, &msg).await { break; }
                         }

                         // 2. Events Broadcast
                         // If subscribe_all is true, we send all events.
                         // In the future, we can add filter sets for events.
                         if subscribe_all {
                             for event in &outputs.events.data {
                                 let msg = StreamMessage::Event {
                                     package_id: event.package_id,
                                     transaction_module: event.transaction_module.to_string(),
                                     sender: event.sender,
                                     type_: event.type_.to_string(),
                                     contents: event.contents.clone(),
                                     digest: digest.to_string(),
                                 };
                                 if let Err(_) = send_json(&mut socket, &msg).await { break; }
                             }
                         }

                         // 3. Pool Updates (Written Objects)
                         // We iterate through written objects to see if any match our subscribed pools
                         for (id, object) in &outputs.written {
                             if subscriptions_pools.contains(id) {
                                  let object_bytes = object.data.try_as_move().map(|o| o.contents().to_vec());
                                  let msg = StreamMessage::PoolUpdate {
                                      pool_id: *id,
                                      digest: digest.to_string(),
                                      object: object_bytes,
                                  };
                                  if let Err(_) = send_json(&mut socket, &msg).await { break; }
                             }
                         }

                         // 4. Account Updates (Sender)
                         // Check if the sender is one of our subscribed accounts
                         let sender = outputs.transaction.sender_address();
                         if subscriptions_accounts.contains(&sender) {
                             info!("CustomBroadcaster: Match found for Account {}", sender);
                             let msg = StreamMessage::AccountActivity {
                                 account: sender,
                                 digest: digest.to_string(),
                                 kind: "Transaction".to_string(),
                             };
                             if let Err(_) = send_json(&mut socket, &msg).await { break; }
                         }

                         // Note: Explicit BalanceChange extraction would require parsing the Move objects
                         // in `outputs.written` to see if they are Coin<T> owned by `sender` and what their value is.
                         // This is complex without a resolver. For now, AccountActivity gives the trigger.
                    }
                    Err(_) => break, // Channel closed
                }
            }

            // Inbound: Handle subscriptions
            res = socket.recv() => {
                match res {
                    Some(Ok(msg)) => {
                        if let Message::Text(text) = msg {
                            if let Ok(req) = serde_json::from_str::<SubscriptionRequest>(&text) {
                                info!("Client request: {:?}", req);
                                match req {
                                    SubscriptionRequest::SubscribePool { pool_id } => {
                                        subscriptions_pools.insert(pool_id);
                                    }
                                    SubscriptionRequest::SubscribeAccount { account } => {
                                        info!("CustomBroadcaster: Client subscribed to Account {}", account);
                                        subscriptions_accounts.insert(account);
                                    }
                                    SubscriptionRequest::SubscribeAll => {
                                        subscribe_all = true;
                                    }
                                    SubscriptionRequest::QueryFieldRange {
                                        table_id,
                                        current_index,
                                        range,
                                        parent_version,
                                    } => {
                                        handle_field_range_query(
                                            &mut socket,
                                            &state,
                                            table_id,
                                            current_index,
                                            range,
                                            parent_version,
                                        )
                                        .await;
                                    }
                                }
                            }
                        } else if let Message::Close(_) = msg {
                            break;
                        }
                    }
                    Some(Err(_)) => break,
                    None => break,
                }
            }
        }
    }
}

async fn send_json<T: Serialize>(socket: &mut WebSocket, msg: &T) -> Result<(), ()> {
    let text = serde_json::to_string(msg).map_err(|_| ())?;
    // Fix: Convert String to Utf8Bytes via .into()
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}

async fn handle_field_range_query(
    socket: &mut WebSocket,
    state: &Arc<AppState>,
    table_id: ObjectID,
    current_index: u64,
    range: u64,
    parent_version: Option<u64>,
) {
    use crate::field_data_query::query_field_data_range;
    use sui_types::base_types::SequenceNumber;
    use sui_types::TypeTag;

    let Some(store) = &state.store else {
        let err = StreamMessage::Error {
            message: "Field query not supported: store not available".to_string(),
        };
        let _ = send_json(socket, &err).await;
        return;
    };

    // Use parent_version if provided, otherwise use MAX (latest)
    let version = parent_version
        .map(SequenceNumber::from_u64)
        .unwrap_or(SequenceNumber::MAX);

    info!(
        "Querying field range: table={}, index={}, range=±{}, version={}",
        table_id, current_index, range, version
    );

    // Query the field data range
    match query_field_data_range(
        &store.perpetual_tables,
        table_id,
        current_index,
        range,
        version,
        &TypeTag::U64, // Assuming U64 keys
    ) {
        Ok(field_data) => {
            let total_fields = field_data.len();
            info!("Found {} fields", total_fields);

            // Send each field as a separate message
            for (index, data) in field_data {
                let msg = StreamMessage::FieldData {
                    table_id,
                    index,
                    field_id: data.field_id,
                    bcs_bytes: data.bcs_bytes,
                    version: data.version.value(),
                };

                if send_json(socket, &msg).await.is_err() {
                    error!("Failed to send field data message");
                    return;
                }
            }

            // Send completion message
            let complete = StreamMessage::QueryComplete {
                table_id,
                total_fields,
            };
            let _ = send_json(socket, &complete).await;
        }
        Err(e) => {
            error!("Field range query failed: {}", e);
            let err = StreamMessage::Error {
                message: format!("Query failed: {}", e),
            };
            let _ = send_json(socket, &err).await;
        }
    }
}