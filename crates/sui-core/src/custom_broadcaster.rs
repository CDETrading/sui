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
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::SystemTime};
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    object::Object,
};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

// --- Configuration ---

/// Known CLMM pool package IDs (Cetus, etc.)
/// These are the package addresses that contain pool::Pool types
const KNOWN_POOL_MODULES: &[&str] = &[
    "pool",
    "clmm_pool",
];

const KNOWN_POOL_NAMES: &[&str] = &[
    "Pool",
];

const KNOWN_TICK_MODULES: &[&str] = &[
    "tick",
    "tick_manager",
];

const KNOWN_TICK_NAMES: &[&str] = &[
    "TickInfo",
    "Tick",
];

// --- Subscription Request Types (JSON format matching user spec) ---

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SubscriptionRequest {
    #[serde(rename = "subscribe_pool")]
    SubscribePool { pool_id: String },

    #[serde(rename = "subscribe_account")]
    SubscribeAccount { address: String },

    #[serde(rename = "subscribe_all")]
    SubscribeAll,
}

// --- Stream Message Types ---

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type")]
pub enum StreamMessage {
    /// Pool state update with liquidity, sqrt_price, tick_index, and tick updates
    #[serde(rename = "tx_pool_state")]
    TxPoolState(TxPoolStatePayload),

    /// Legacy pool update (raw bytes)
    #[serde(rename = "pool_update")]
    PoolUpdate {
        pool_id: String,
        digest: String,
        object: Option<Vec<u8>>,
    },

    /// Account activity notification
    #[serde(rename = "account_activity")]
    AccountActivity {
        account: String,
        digest: String,
        kind: String,
    },

    /// Balance change notification
    #[serde(rename = "balance_change")]
    BalanceChange {
        account: String,
        coin_type: String,
        new_balance: String,
    },

    /// Event notification
    #[serde(rename = "event")]
    Event {
        package_id: String,
        transaction_module: String,
        sender: String,
        type_: String,
        contents: Vec<u8>,
        digest: String,
    },

    /// Error message
    #[serde(rename = "error")]
    Error { message: String },
}

/// Payload for pool state updates
#[derive(Clone, Debug, Serialize)]
pub struct TxPoolStatePayload {
    pub ts_ms: u64,
    pub tx_digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_seq: Option<u64>,
    pub updates: Vec<PoolStateUpdate>,
}

/// Individual pool state update
#[derive(Clone, Debug, Serialize)]
pub struct PoolStateUpdate {
    pub pool_id: String,
    /// sqrt_price as string (u128)
    pub sqrt_price: String,
    /// Current tick index
    pub tick_index: i32,
    /// Current liquidity as string (u128)
    pub liquidity: String,
    /// Tick updates associated with this pool change
    pub ticks: Vec<TickUpdate>,
}

/// Tick update information
#[derive(Clone, Debug, Serialize)]
pub struct TickUpdate {
    pub tick_index: i32,
    /// Liquidity gross as string (u128)
    pub liquidity_gross: String,
    /// Liquidity net as string (i128)
    pub liquidity_net: String,
}

// --- Pool/Tick Extraction Logic ---

/// Extracted pool data from BCS-encoded Move object
#[derive(Debug, Clone)]
pub struct ExtractedPoolData {
    pub pool_id: ObjectID,
    pub sqrt_price: u128,
    pub tick_index: i32,
    pub liquidity: u128,
}

/// Extracted tick data from BCS-encoded Move object
#[derive(Debug, Clone)]
pub struct ExtractedTickData {
    pub pool_id: ObjectID,  // Parent pool ID (from dynamic field)
    pub tick_index: i32,
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
}

/// Check if a type looks like a CLMM pool based on module/name patterns
fn is_pool_type(module: &str, name: &str) -> bool {
    KNOWN_POOL_MODULES.iter().any(|m| module.contains(m))
        && KNOWN_POOL_NAMES.contains(&name)
}

/// Check if a type looks like a tick info based on module/name patterns
fn is_tick_type(module: &str, name: &str) -> bool {
    KNOWN_TICK_MODULES.iter().any(|m| module.contains(m))
        && KNOWN_TICK_NAMES.contains(&name)
}

/// Try to extract pool data from a Move object
///
/// CLMM Pool struct typically has this layout (Cetus example):
/// struct Pool<CoinTypeA, CoinTypeB> has key, store {
///     id: UID,                          // 32 bytes
///     coin_a: Balance<CoinTypeA>,       // 8 bytes (value only)
///     coin_b: Balance<CoinTypeB>,       // 8 bytes (value only)
///     tick_spacing: u32,                // 4 bytes
///     fee_rate: u64,                    // 8 bytes
///     liquidity: u128,                  // 16 bytes
///     current_sqrt_price: u128,         // 16 bytes
///     current_tick_index: I32,          // 4 bytes (signed)
///     ...
/// }
fn try_extract_pool_data(object: &Object) -> Option<ExtractedPoolData> {
    let move_obj = object.data.try_as_move()?;
    let type_ = move_obj.type_();

    let module = type_.module().as_str();
    let name = type_.name().as_str();

    if !is_pool_type(module, name) {
        return None;
    }

    let contents = move_obj.contents();
    let pool_id = object.id();

    // Try to extract fields from the BCS bytes
    // This is a best-effort extraction that works with common CLMM pool layouts
    if let Some((sqrt_price, tick_index, liquidity)) = try_parse_pool_fields(contents) {
        debug!(
            "Extracted pool data: pool_id={}, sqrt_price={}, tick_index={}, liquidity={}",
            pool_id, sqrt_price, tick_index, liquidity
        );
        return Some(ExtractedPoolData {
            pool_id,
            sqrt_price,
            tick_index,
            liquidity,
        });
    }

    // Fallback: return with placeholder values if we can identify it's a pool
    // but can't parse the exact fields
    warn!(
        "Identified pool {} but couldn't parse fields (module={}, name={})",
        pool_id, module, name
    );
    None
}

/// Try to parse pool fields from BCS bytes
/// Returns (sqrt_price, tick_index, liquidity) if successful
fn try_parse_pool_fields(contents: &[u8]) -> Option<(u128, i32, u128)> {
    // Minimum size check: UID (32) + at least some fields
    if contents.len() < 80 {
        return None;
    }

    // Strategy: scan for patterns that look like sqrt_price (u128), tick_index (i32), liquidity (u128)
    // Common offsets in CLMM pools vary, so we try multiple strategies

    // Strategy 1: Cetus-style layout
    // After UID (32 bytes), there may be:
    // - coin_a balance (8 bytes)
    // - coin_b balance (8 bytes)
    // - tick_spacing (4 bytes)
    // - fee_rate (8 bytes)
    // - liquidity (16 bytes) at offset 60
    // - current_sqrt_price (16 bytes) at offset 76
    // - current_tick_index (4 bytes) at offset 92

    if contents.len() >= 96 {
        // Try Cetus-style offsets
        let liquidity = try_read_u128(contents, 60)?;
        let sqrt_price = try_read_u128(contents, 76)?;
        let tick_index = try_read_i32(contents, 92)?;

        // Sanity checks for CLMM values
        if sqrt_price > 0 && liquidity < u128::MAX / 2 {
            return Some((sqrt_price, tick_index, liquidity));
        }
    }

    // Strategy 2: Alternative layout - search for sqrt_price pattern
    // sqrt_price in Q64.64 format is typically between 2^32 and 2^192
    // Look for consecutive u128 values that look reasonable
    for offset in (32..contents.len().saturating_sub(40)).step_by(8) {
        if let Some(sqrt_price) = try_read_u128(contents, offset)
            && sqrt_price >= (1u128 << 32)
            && sqrt_price <= (1u128 << 127)
            && offset >= 16
            && let Some(liquidity) = try_read_u128(contents, offset - 16)
            && offset + 16 + 4 <= contents.len()
            && let Some(tick_index) = try_read_i32(contents, offset + 16)
            && tick_index.abs() < 1_000_000
        {
            return Some((sqrt_price, tick_index, liquidity));
        }
    }

    None
}

fn try_read_u128(data: &[u8], offset: usize) -> Option<u128> {
    if offset + 16 > data.len() {
        return None;
    }
    let bytes: [u8; 16] = data[offset..offset + 16].try_into().ok()?;
    Some(u128::from_le_bytes(bytes))
}

fn try_read_i32(data: &[u8], offset: usize) -> Option<i32> {
    if offset + 4 > data.len() {
        return None;
    }
    let bytes: [u8; 4] = data[offset..offset + 4].try_into().ok()?;
    Some(i32::from_le_bytes(bytes))
}

/// Try to extract tick data from a dynamic field object
fn try_extract_tick_data(object: &Object) -> Option<ExtractedTickData> {
    let move_obj = object.data.try_as_move()?;
    let type_ = move_obj.type_();

    let module = type_.module().as_str();
    let name = type_.name().as_str();

    // Check if this is a dynamic field containing tick info
    // Dynamic fields have type: 0x2::dynamic_field::Field<K, V>
    if module == "dynamic_field" && name == "Field" {
        // Try to parse the inner value type to see if it's a tick
        let type_str = type_.to_string();
        if type_str.contains("TickInfo") || type_str.contains("::tick::") {
            return try_parse_tick_from_dynamic_field(object);
        }
    }

    // Direct tick object (less common)
    if is_tick_type(module, name) {
        return try_parse_direct_tick(object);
    }

    None
}

fn try_parse_tick_from_dynamic_field(object: &Object) -> Option<ExtractedTickData> {
    let move_obj = object.data.try_as_move()?;
    let contents = move_obj.contents();

    // Dynamic field layout:
    // - UID (32 bytes)
    // - name/key (variable, but for I32 tick index it's typically 4 bytes)
    // - value (TickInfo struct)

    if contents.len() < 56 {
        return None;
    }

    // Try to extract tick index from the key portion (after UID)
    let tick_index = try_read_i32(contents, 32)?;

    // Try to find liquidity values in the remaining bytes
    // TickInfo typically has: liquidity_gross (u128), liquidity_net (i128), ...
    let offset = 36; // After UID + I32 key

    if contents.len() >= offset + 32 {
        let liquidity_gross = try_read_u128(contents, offset)?;
        let liquidity_net_bytes = try_read_u128(contents, offset + 16)?;
        let liquidity_net = liquidity_net_bytes as i128;

        // Get parent ID from object reference or use object's own ID as placeholder
        let pool_id = object.id();

        return Some(ExtractedTickData {
            pool_id,
            tick_index,
            liquidity_gross,
            liquidity_net,
        });
    }

    None
}

fn try_parse_direct_tick(object: &Object) -> Option<ExtractedTickData> {
    let move_obj = object.data.try_as_move()?;
    let contents = move_obj.contents();

    if contents.len() < 48 {
        return None;
    }

    // Direct TickInfo layout (after UID):
    // - tick_index or other identifier
    // - liquidity_gross (u128)
    // - liquidity_net (i128)

    let pool_id = object.id();
    let tick_index = try_read_i32(contents, 32)?;
    let liquidity_gross = try_read_u128(contents, 36)?;
    let liquidity_net = try_read_u128(contents, 52)? as i128;

    Some(ExtractedTickData {
        pool_id,
        tick_index,
        liquidity_gross,
        liquidity_net,
    })
}

/// Extract pool and tick changes from transaction outputs
pub fn extract_pool_tick_changes(
    outputs: &TransactionOutputs,
) -> Option<TxPoolStatePayload> {
    let tx_digest = outputs.transaction.digest().to_string();
    let ts_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let mut pool_updates: Vec<PoolStateUpdate> = Vec::new();
    let mut tick_updates_by_pool: std::collections::HashMap<ObjectID, Vec<TickUpdate>> =
        std::collections::HashMap::new();

    // First pass: extract all pool and tick data from written objects
    for object in outputs.written.values() {
        // Try to extract pool data
        if let Some(pool_data) = try_extract_pool_data(object) {
            pool_updates.push(PoolStateUpdate {
                pool_id: pool_data.pool_id.to_string(),
                sqrt_price: pool_data.sqrt_price.to_string(),
                tick_index: pool_data.tick_index,
                liquidity: pool_data.liquidity.to_string(),
                ticks: Vec::new(), // Will be populated in second pass
            });
        }

        // Try to extract tick data
        if let Some(tick_data) = try_extract_tick_data(object) {
            tick_updates_by_pool
                .entry(tick_data.pool_id)
                .or_default()
                .push(TickUpdate {
                    tick_index: tick_data.tick_index,
                    liquidity_gross: tick_data.liquidity_gross.to_string(),
                    liquidity_net: tick_data.liquidity_net.to_string(),
                });
        }
    }

    // Second pass: attach tick updates to their pools
    for update in &mut pool_updates {
        if let Ok(pool_id) = update.pool_id.parse::<ObjectID>()
            && let Some(ticks) = tick_updates_by_pool.remove(&pool_id)
        {
            update.ticks = ticks;
        }
    }

    // Return None if no pool updates found
    if pool_updates.is_empty() {
        return None;
    }

    Some(TxPoolStatePayload {
        ts_ms,
        tx_digest,
        checkpoint_seq: None, // Not available before commit
        updates: pool_updates,
    })
}

// --- Broadcaster State ---

struct AppState {
    tx: broadcast::Sender<Arc<TransactionOutputs>>,
}

// --- Main Broadcaster Logic ---

pub struct CustomBroadcaster;

impl CustomBroadcaster {
    pub fn spawn(mut rx: mpsc::Receiver<Arc<TransactionOutputs>>, port: u16) {
        // Create a broadcast channel for all connected websocket clients
        // Capacity 10000 to handle bursts without blocking
        let (tx, _) = broadcast::channel(10000);
        let tx_clone = tx.clone();

        // 1. Spawn the ingestion loop
        tokio::spawn(async move {
            info!("CustomBroadcaster: Ingestion loop started");
            while let Some(outputs) = rx.recv().await {
                // Broadcast the Arc directly to avoid cloning the heavy data structure
                // Serialization happens in the client handling task
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
        let app_state = Arc::new(AppState { tx });

        tokio::spawn(async move {
            let app = Router::new()
                .route("/ws", get(ws_handler))
                .with_state(app_state);

            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            info!("CustomBroadcaster: WebSocket server listening on {}", addr);

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

    let mut subscriptions_pools: HashSet<ObjectID> = HashSet::new();
    let mut subscriptions_accounts: HashSet<SuiAddress> = HashSet::new();
    let mut subscribe_all = false;

    info!("CustomBroadcaster: New WebSocket connection established");

    loop {
        tokio::select! {
            // Outbound: Send updates to client
            res = rx.recv() => {
                match res {
                    Ok(outputs) => {
                        let digest = outputs.transaction.digest();
                        let sender = outputs.transaction.sender_address();

                        debug!(
                            "CustomBroadcaster: Processing Tx {} from Sender {} (AccSubs: {}, PoolSubs: {})",
                            digest, sender, subscriptions_accounts.len(), subscriptions_pools.len()
                        );

                        // 1. Try to extract and send pool state updates
                        if let Some(pool_payload) = extract_pool_tick_changes(&outputs) {
                            // Check if any pool in the update matches our subscriptions
                            let should_send = subscribe_all || pool_payload.updates.iter().any(|u| {
                                u.pool_id.parse::<ObjectID>()
                                    .map(|id| subscriptions_pools.contains(&id))
                                    .unwrap_or(false)
                            });

                            if should_send {
                                let msg = StreamMessage::TxPoolState(pool_payload);
                                if send_json(&mut socket, &msg).await.is_err() {
                                    break;
                                }
                            }
                        }

                        // 2. Send legacy pool updates for subscribed pools (raw bytes)
                        for (id, object) in &outputs.written {
                            if subscriptions_pools.contains(id) {
                                let object_bytes = object.data.try_as_move().map(|o| o.contents().to_vec());
                                let msg = StreamMessage::PoolUpdate {
                                    pool_id: id.to_string(),
                                    digest: digest.to_string(),
                                    object: object_bytes,
                                };
                                if send_json(&mut socket, &msg).await.is_err() {
                                    break;
                                }
                            }
                        }

                        // 3. Account activity for subscribe_all
                        if subscribe_all {
                            let msg = StreamMessage::AccountActivity {
                                account: sender.to_string(),
                                digest: digest.to_string(),
                                kind: "Transaction".to_string(),
                            };
                            if send_json(&mut socket, &msg).await.is_err() {
                                break;
                            }

                            // Send all events
                            for event in &outputs.events.data {
                                let msg = StreamMessage::Event {
                                    package_id: event.package_id.to_string(),
                                    transaction_module: event.transaction_module.to_string(),
                                    sender: event.sender.to_string(),
                                    type_: event.type_.to_string(),
                                    contents: event.contents.clone(),
                                    digest: digest.to_string(),
                                };
                                if send_json(&mut socket, &msg).await.is_err() {
                                    break;
                                }
                            }
                        }

                        // 4. Account activity for subscribed accounts
                        if subscriptions_accounts.contains(&sender) {
                            info!("CustomBroadcaster: Match found for Account {}", sender);
                            let msg = StreamMessage::AccountActivity {
                                account: sender.to_string(),
                                digest: digest.to_string(),
                                kind: "Transaction".to_string(),
                            };
                            if send_json(&mut socket, &msg).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("CustomBroadcaster: Client lagged, skipped {} messages", n);
                        // Continue processing, don't disconnect
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("CustomBroadcaster: Broadcast channel closed");
                        break;
                    }
                }
            }

            // Inbound: Handle subscriptions
            res = socket.recv() => {
                match res {
                    Some(Ok(msg)) => {
                        match msg {
                            Message::Text(text) => {
                                match serde_json::from_str::<SubscriptionRequest>(&text) {
                                    Ok(req) => {
                                        match req {
                                            SubscriptionRequest::SubscribePool { pool_id } => {
                                                match pool_id.parse::<ObjectID>() {
                                                    Ok(id) => {
                                                        info!("CustomBroadcaster: Client subscribed to Pool {}", id);
                                                        subscriptions_pools.insert(id);
                                                    }
                                                    Err(_) => {
                                                        let err_msg = StreamMessage::Error {
                                                            message: format!("Invalid pool_id: {}", pool_id),
                                                        };
                                                        let _ = send_json(&mut socket, &err_msg).await;
                                                    }
                                                }
                                            }
                                            SubscriptionRequest::SubscribeAccount { address } => {
                                                match address.parse::<SuiAddress>() {
                                                    Ok(addr) => {
                                                        info!("CustomBroadcaster: Client subscribed to Account {}", addr);
                                                        subscriptions_accounts.insert(addr);
                                                    }
                                                    Err(_) => {
                                                        let err_msg = StreamMessage::Error {
                                                            message: format!("Invalid address: {}", address),
                                                        };
                                                        let _ = send_json(&mut socket, &err_msg).await;
                                                    }
                                                }
                                            }
                                            SubscriptionRequest::SubscribeAll => {
                                                info!("CustomBroadcaster: Client subscribed to all updates");
                                                subscribe_all = true;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let err_msg = StreamMessage::Error {
                                            message: format!("Invalid subscription request: {}", e),
                                        };
                                        let _ = send_json(&mut socket, &err_msg).await;
                                    }
                                }
                            }
                            Message::Close(_) => {
                                info!("CustomBroadcaster: Client sent close frame");
                                break;
                            }
                            Message::Ping(data) => {
                                // Respond to ping with pong
                                let _ = socket.send(Message::Pong(data)).await;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => {
                        warn!("CustomBroadcaster: WebSocket error: {}", e);
                        break;
                    }
                    None => {
                        info!("CustomBroadcaster: WebSocket stream ended");
                        break;
                    }
                }
            }
        }
    }

    info!("CustomBroadcaster: WebSocket connection closed");
}

async fn send_json<T: Serialize>(socket: &mut WebSocket, msg: &T) -> Result<(), ()> {
    let text = serde_json::to_string(msg).map_err(|e| {
        error!("CustomBroadcaster: Failed to serialize message: {}", e);
    })?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|e| {
            debug!("CustomBroadcaster: Failed to send message: {}", e);
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_request_parsing() {
        // Test subscribe_pool
        let json = r#"{"type":"subscribe_pool","pool_id":"0x1234"}"#;
        let req: SubscriptionRequest = serde_json::from_str(json).unwrap();
        match req {
            SubscriptionRequest::SubscribePool { pool_id } => {
                assert_eq!(pool_id, "0x1234");
            }
            _ => panic!("Expected SubscribePool"),
        }

        // Test subscribe_account
        let json = r#"{"type":"subscribe_account","address":"0xabcd"}"#;
        let req: SubscriptionRequest = serde_json::from_str(json).unwrap();
        match req {
            SubscriptionRequest::SubscribeAccount { address } => {
                assert_eq!(address, "0xabcd");
            }
            _ => panic!("Expected SubscribeAccount"),
        }

        // Test subscribe_all
        let json = r#"{"type":"subscribe_all"}"#;
        let req: SubscriptionRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, SubscriptionRequest::SubscribeAll));
    }

    #[test]
    fn test_stream_message_serialization() {
        let payload = TxPoolStatePayload {
            ts_ms: 1730000000000,
            tx_digest: "9r...==".to_string(),
            checkpoint_seq: None,
            updates: vec![PoolStateUpdate {
                pool_id: "0x1234".to_string(),
                sqrt_price: "123456789".to_string(),
                tick_index: 12345,
                liquidity: "999999999999".to_string(),
                ticks: vec![
                    TickUpdate {
                        tick_index: 12000,
                        liquidity_gross: "1000".to_string(),
                        liquidity_net: "-1000".to_string(),
                    },
                    TickUpdate {
                        tick_index: 13000,
                        liquidity_gross: "1000".to_string(),
                        liquidity_net: "1000".to_string(),
                    },
                ],
            }],
        };

        let msg = StreamMessage::TxPoolState(payload);
        let json = serde_json::to_string(&msg).unwrap();

        // Verify it can be parsed back
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "tx_pool_state");
        assert_eq!(parsed["updates"][0]["tick_index"], 12345);
    }

    #[test]
    fn test_is_pool_type() {
        assert!(is_pool_type("pool", "Pool"));
        assert!(is_pool_type("clmm_pool", "Pool"));
        assert!(!is_pool_type("coin", "Coin"));
        assert!(!is_pool_type("pool", "Position"));
    }

    #[test]
    fn test_is_tick_type() {
        assert!(is_tick_type("tick", "TickInfo"));
        assert!(is_tick_type("tick_manager", "Tick"));
        assert!(!is_tick_type("pool", "Pool"));
    }

    #[test]
    fn test_try_read_u128() {
        let data: Vec<u8> = vec![0; 32];
        assert_eq!(try_read_u128(&data, 0), Some(0));
        assert_eq!(try_read_u128(&data, 16), Some(0));
        assert_eq!(try_read_u128(&data, 17), None); // Out of bounds

        let mut data = vec![0u8; 16];
        data[0] = 1;
        assert_eq!(try_read_u128(&data, 0), Some(1));
    }

    #[test]
    fn test_try_read_i32() {
        let data: Vec<u8> = vec![0xFF, 0xFF, 0xFF, 0xFF]; // -1 in little endian
        assert_eq!(try_read_i32(&data, 0), Some(-1));

        let data: Vec<u8> = vec![0x01, 0x00, 0x00, 0x00]; // 1 in little endian
        assert_eq!(try_read_i32(&data, 0), Some(1));
    }
}
