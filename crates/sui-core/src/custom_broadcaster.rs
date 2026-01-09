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
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::SystemTime,
};
use sui_types::object::Owner;
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    object::Object,
    storage::ObjectStore,
};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};
use crate::authority::AuthorityStore;
use crate::authority::authority_store_tables::LiveObject;
// --- Configuration ---

/// Known CLMM pool package IDs (Cetus, etc.)
/// These are the package addresses that contain pool::Pool types
const KNOWN_POOL_MODULES: &[&str] = &["pool", "clmm_pool"];

const KNOWN_POOL_NAMES: &[&str] = &["Pool"];

const KNOWN_TICK_MODULES: &[&str] = &["tick", "tick_manager"];

const KNOWN_TICK_NAMES: &[&str] = &["TickInfo", "Tick"];

// --- Subscription Request Types (JSON format matching user spec) ---

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(tag = "type")]
pub enum SubscriptionRequest {
    #[serde(rename = "subscribe_pool")]
    SubscribePool { pool_id: String },

    #[serde(rename = "subscribe_account")]
    SubscribeAccount { address: String },

    #[serde(rename = "subscribe_all")]
    #[serde(rename = "subscribe_pool")]
    SubscribePool { pool_id: String },

    #[serde(rename = "subscribe_account")]
    SubscribeAccount { address: String },

    #[serde(rename = "subscribe_all")]
    SubscribeAll,

    #[serde(rename = "query_all_fields")]
    QueryAllFields { table_id: ObjectID },
}

#[derive(Clone, Debug, Serialize)]
pub struct SerializableOutput {
    pub digest: String,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct FieldData {
    pub field_id: ObjectID,
    pub object_bytes: Vec<u8>,
    pub object_type: String,
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
    /// Pool state update with liquidity, sqrt_price, tick_index, and tick updates
    #[serde(rename = "tx_pool_state")]
    TxPoolState(TxPoolStatePayload),

    /// Legacy pool update (raw bytes)
    #[serde(rename = "pool_update")]
    PoolUpdate {
        pool_id: String,
        pool_id: String,
        digest: String,
        object: Option<Vec<u8>>,
        object_type: Option<String>,
    },

    FieldUpdate {
        pool_id: ObjectID,  // Parent table ID
        field_id: ObjectID, // Field object ID
        digest: String,
        object: Option<Vec<u8>>, // Field<K,V> 的序列化資料
        object_type: Option<String>,
    },

    /// Account activity notification
    #[serde(rename = "account_activity")]
    AccountActivity {
        account: String,
        account: String,
        digest: String,
        kind: String,
        kind: String,
    },

    /// Balance change notification
    #[serde(rename = "balance_change")]

    /// Balance change notification
    #[serde(rename = "balance_change")]
    BalanceChange {
        account: String,
        account: String,
        coin_type: String,
        new_balance: String,
        new_balance: String,
    },

    /// Event notification
    #[serde(rename = "event")]

    /// Event notification
    #[serde(rename = "event")]
    Event {
        package_id: String,
        package_id: String,
        transaction_module: String,
        sender: String,
        sender: String,
        type_: String,
        contents: Vec<u8>,
        digest: String,
    },

    #[serde(rename = "all_fields_response")]
    AllFieldsResponse {
        table_id: ObjectID,
        fields: Vec<FieldData>,
        total_count: usize,
    },

    /// Error message
    #[serde(rename = "error")]
    Error { message: String },

    /// Raw output (for debugging)
    #[serde(rename = "raw")]
    Raw(SerializableOutput),

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
/// Uses SuiAddress for parent_owner since Owner::ObjectOwner contains SuiAddress
#[derive(Debug, Clone)]
pub struct ExtractedTickData {
    pub parent_owner: SuiAddress,
    pub tick_index: i32,
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
}

fn is_momentum_tickinfo(ty: &str) -> bool {
    ty.ends_with("::tick::TickInfo")
}

fn is_cetus_tick(ty: &str) -> bool {
    ty.ends_with("::tick::Tick")
}

/// Check if a type looks like a CLMM pool based on module/name patterns
fn is_pool_type(module: &str, name: &str) -> bool {
    KNOWN_POOL_MODULES.iter().any(|m| module.contains(m)) && KNOWN_POOL_NAMES.contains(&name)
}

/// Check if a type looks like a tick info based on module/name patterns
fn is_tick_type(module: &str, name: &str) -> bool {
    KNOWN_TICK_MODULES.iter().any(|m| module.contains(m)) && KNOWN_TICK_NAMES.contains(&name)
}

/// Try to extract pool data from a Move object
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

    warn!(
        "Identified pool {} but couldn't parse fields (module={}, name={})",
        pool_id, module, name
    );
    None
}

/// Try to parse pool fields from BCS bytes
/// Returns (sqrt_price, tick_index, liquidity) if successful
fn try_parse_pool_fields(contents: &[u8]) -> Option<(u128, i32, u128)> {
    if contents.len() < 80 {
        return None;
    }

    // Strategy 1: Cetus-style layout
    if contents.len() >= 96 {
        let liquidity = try_read_u128(contents, 60)?;
        let sqrt_price = try_read_u128(contents, 76)?;
        let tick_index = try_read_i32(contents, 92)?;

        if sqrt_price > 0 && liquidity < u128::MAX / 2 {
            return Some((sqrt_price, tick_index, liquidity));
        }
    }

    // Strategy 2: Alternative layout - search for sqrt_price pattern
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

fn try_read_i128_le(data: &[u8], offset: usize) -> Option<i128> {
    if offset + 16 > data.len() {
        return None;
    }
    let bytes: [u8; 16] = data[offset..offset + 16].try_into().ok()?;
    Some(i128::from_le_bytes(bytes))
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
    // Get parent owner address - Owner::ObjectOwner contains SuiAddress
    let parent_owner: SuiAddress = match object.owner() {
        Owner::ObjectOwner(addr) => *addr,
        _ => return None,
    };

    let move_obj = object.data.try_as_move()?;
    let type_ = move_obj.type_();
    let module = type_.module().as_str();
    let name = type_.name().as_str();

    // Case A: dynamic_field::Field<...>
    if module == "dynamic_field" && name == "Field" {
        let type_str = type_.to_string();
        if type_str.contains("TickInfo") || type_str.contains("::tick::") {
            return try_parse_tick_from_dynamic_field(parent_owner, object);
        }
    }

    // Case B: Direct Tick / TickInfo
    if is_tick_type(module, name)
        || is_momentum_tickinfo(&type_.to_string())
        || is_cetus_tick(&type_.to_string())
    {
        return try_parse_direct_tick(parent_owner, object);
    }

    None
}

fn try_parse_tick_from_dynamic_field(
    parent_owner: SuiAddress,
    object: &Object,
) -> Option<ExtractedTickData> {
    let move_obj = object.data.try_as_move()?;
    let contents = move_obj.contents();

    // Minimum size: UID(32) + key(4) + liquidity_gross(16) + liquidity_net(16)
    if contents.len() < 32 + 4 + 16 + 16 {
        return None;
    }

    let tick_index = try_read_i32(contents, 32)?;

    // value: TickInfo { liquidity_gross: u128, liquidity_net: i128, ... }
    let value_offset = 36;
    let liquidity_gross = try_read_u128(contents, value_offset)?;
    let liquidity_net = try_read_i128_le(contents, value_offset + 16)?;

    Some(ExtractedTickData {
        parent_owner,
        tick_index,
        liquidity_gross,
        liquidity_net,
    })
}

fn try_parse_direct_tick(parent_owner: SuiAddress, object: &Object) -> Option<ExtractedTickData> {
    let move_obj = object.data.try_as_move()?;
    let contents = move_obj.contents();

    if contents.len() < 32 + 4 + 16 + 16 {
        return None;
    }

    let tick_index = try_read_i32(contents, 32)?;
    let liquidity_gross = try_read_u128(contents, 36)?;
    let liquidity_net = try_read_i128_le(contents, 52)?;

    Some(ExtractedTickData {
        parent_owner,
        tick_index,
        liquidity_gross,
        liquidity_net,
    })
}

/// Convert SuiAddress to 32 bytes for pattern matching in pool contents
fn sui_address_to_bytes(addr: &SuiAddress) -> [u8; 32] {
    let s = addr.to_string();
    let s = s.strip_prefix("0x").unwrap_or(&s);
    hex_decode_to_32_bytes(s)
}

/// Manual hex decode without external crate - returns 32 bytes (zero-padded on left)
fn hex_decode_to_32_bytes(hex_str: &str) -> [u8; 32] {
    fn hex_val(c: u8) -> u8 {
        match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => 0,
        }
    }

    let mut out = [0u8; 32];
    let sb = hex_str.as_bytes();
    let bytes_len = sb.len() / 2;
    let start = 32usize.saturating_sub(bytes_len);

    let mut i = 0usize;
    let mut o = start;

    while i + 1 < sb.len() && o < 32 {
        let hi = hex_val(sb[i]);
        let lo = hex_val(sb[i + 1]);
        out[o] = (hi << 4) | lo;
        i += 2;
        o += 1;
    }

    out
}

/// Extract pool and tick changes from transaction outputs
pub fn extract_pool_tick_changes(outputs: &TransactionOutputs) -> Option<TxPoolStatePayload> {
    let tx_digest = outputs.transaction.digest().to_string();
    let ts_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    // 1) Collect pool updates + pool BCS contents for owner->pool mapping
    let mut pool_updates: Vec<PoolStateUpdate> = Vec::new();
    let mut pool_contents_by_id: HashMap<ObjectID, Vec<u8>> = HashMap::new();

    // 2) Collect tick data
    let mut extracted_ticks: Vec<ExtractedTickData> = Vec::new();
    let mut tick_owner_candidates: HashSet<SuiAddress> = HashSet::new();

    let mut pool_count = 0usize;
    let mut tick_count = 0usize;

    for object in outputs.written.values() {
        // Try pool extraction
        if let Some(pool_data) = try_extract_pool_data(object) {
            pool_count += 1;
            pool_updates.push(PoolStateUpdate {
                pool_id: pool_data.pool_id.to_string(),
                sqrt_price: pool_data.sqrt_price.to_string(),
                tick_index: pool_data.tick_index,
                liquidity: pool_data.liquidity.to_string(),
                ticks: Vec::new(),
            });

            if let Some(m) = object.data.try_as_move() {
                pool_contents_by_id.insert(pool_data.pool_id, m.contents().to_vec());
            }
        }

        // Try tick extraction
        if let Some(tick) = try_extract_tick_data(object) {
            tick_count += 1;
            tick_owner_candidates.insert(tick.parent_owner);
            extracted_ticks.push(tick);
        }
    }

    debug!(
        "extract_pool_tick_changes: found {} pools, {} ticks",
        pool_count, tick_count
    );

    // If no pool updates, don't send
    if pool_updates.is_empty() {
        return None;
    }

    // 3) Build owner->pool mapping (best-effort)
    let owner_to_pool = build_owner_to_pool_map(&pool_contents_by_id, &tick_owner_candidates);

    let mapping_success = owner_to_pool.len();
    let mapping_total = tick_owner_candidates.len();
    debug!(
        "owner->pool mapping: {}/{} successful",
        mapping_success, mapping_total
    );

    // 4) Attach ticks to their pools
    let mut tick_updates_by_pool: HashMap<ObjectID, Vec<TickUpdate>> = HashMap::new();

    for t in extracted_ticks {
        // Try to find the pool this tick belongs to
        let pool_id_opt = owner_to_pool.get(&t.parent_owner).copied();

        if let Some(pool_id) = pool_id_opt {
            tick_updates_by_pool
                .entry(pool_id)
                .or_default()
                .push(TickUpdate {
                    tick_index: t.tick_index,
                    liquidity_gross: t.liquidity_gross.to_string(),
                    liquidity_net: t.liquidity_net.to_string(),
                });
        }
        // If no mapping found, we skip this tick (can't reliably attribute it)
    }

    for update in &mut pool_updates {
        if let Ok(pool_id) = update.pool_id.parse::<ObjectID>() {
            if let Some(ticks) = tick_updates_by_pool.remove(&pool_id) {
                update.ticks = ticks;
            }
        }
    }

    Some(TxPoolStatePayload {
        ts_ms,
        tx_digest,
        checkpoint_seq: None,
        updates: pool_updates,
    })
}

/// Build best-effort owner->pool mapping:
/// If tick entry's owner (usually table object) bytes are found in pool contents,
/// we assume that table belongs to that pool.
fn build_owner_to_pool_map(
    pool_contents_by_id: &HashMap<ObjectID, Vec<u8>>,
    candidate_owners: &HashSet<SuiAddress>,
) -> HashMap<SuiAddress, ObjectID> {
    let mut map = HashMap::new();

    for owner_addr in candidate_owners {
        let needle = sui_address_to_bytes(owner_addr);

        // Find first pool that contains this address bytes
        for (pool_id, contents) in pool_contents_by_id {
            if contains_subslice(contents, &needle) {
                map.insert(*owner_addr, *pool_id);
                break;
            }
        }
    }

    map
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

// --- Broadcaster State ---

/// Cache entry status for a table
enum CacheStatus {
    /// Cache is being populated (first scan in progress)
    Loading,
    /// Cache is ready and populated
    Ready,
}

/// Write-through cache for table fields
/// Maps table_id -> (cache_status, field_id -> FieldData)
type TableFieldsCache = Arc<DashMap<ObjectID, (CacheStatus, HashMap<ObjectID, FieldData>)>>;

/// Tracks which tables are currently being scanned to avoid duplicate scans
type ScanInProgress = Arc<DashMap<ObjectID, Arc<tokio::sync::Notify>>>;

struct AppState {
    tx: broadcast::Sender<Arc<TransactionOutputs>>,
    store: Arc<AuthorityStore>,
}

// --- Main Broadcaster Logic ---

pub struct CustomBroadcaster;

impl CustomBroadcaster {
    pub fn spawn(mut rx: mpsc::Receiver<Arc<TransactionOutputs>>, authority_store: Arc<AuthorityStore>, port: u16) {
        let (tx, _) = broadcast::channel(10000);
        let tx_clone = tx.clone();

        // Spawn the ingestion loop
        tokio::spawn(async move {
            info!("CustomBroadcaster: Ingestion loop started with write-through cache");
            while let Some(outputs) = rx.recv().await {
                if let Err(e) = tx_clone.send(outputs) {
                    debug!(
                        "CustomBroadcaster: No active subscribers, dropped message: {}",
                        e
                    );
                }
            }
            info!("CustomBroadcaster: Ingestion loop ended");
        });

        // Spawn the WebServer
        let app_state = Arc::new(AppState { tx, store: authority_store });

        tokio::spawn(async move {
            let app = Router::new()
                .route("/ws", get(ws_handler))
                .with_state(app_state);

            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            info!("CustomBroadcaster: WebSocket server listening on {}", addr);
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

    /// Update cache from transaction outputs (write-through)
    /// Called for every transaction to keep cache in sync
    fn update_cache_from_outputs(outputs: &TransactionOutputs, cache: &TableFieldsCache) {
        // Handle written objects - insert/update in cache
        for (object_id, object) in &outputs.written {
            // Check if this object is owned by another object (dynamic field)
            if let Owner::ObjectOwner(parent_addr) = object.owner() {
                let parent_id = ObjectID::from(*parent_addr);

                // Only update if this table is already cached
                if let Some(mut entry) = cache.get_mut(&parent_id) {
                    if let (CacheStatus::Ready, fields) = entry.value_mut() {
                        if let Some(move_obj) = object.data.try_as_move() {
                            let field_data = FieldData {
                                field_id: *object_id,
                                object_bytes: move_obj.contents().to_vec(),
                                object_type: move_obj.type_().to_string(),
                            };
                            fields.insert(*object_id, field_data);
                            debug!(
                                "Cache updated: inserted/updated field {} for table {}",
                                object_id, parent_id
                            );
                        }
                    }
                }
            }
        }

        // Handle deleted objects - remove from cache
        for deleted_key in &outputs.deleted {
            let deleted_id = deleted_key.0;
            // We don't know the parent directly from ObjectKey, so we check all cached tables
            // This is O(n) but deletion is rare compared to updates
            for mut entry in cache.iter_mut() {
                if let (CacheStatus::Ready, fields) = entry.value_mut() {
                    if fields.remove(&deleted_id).is_some() {
                        debug!(
                            "Cache updated: removed deleted field {} from table {}",
                            deleted_id,
                            entry.key()
                        );
                        break;
                    }
                }
            }
        }

        // Handle wrapped objects similarly (they're effectively removed)
        for wrapped_key in &outputs.wrapped {
            let wrapped_id = wrapped_key.0;
            for mut entry in cache.iter_mut() {
                if let (CacheStatus::Ready, fields) = entry.value_mut() {
                    if fields.remove(&wrapped_id).is_some() {
                        debug!(
                            "Cache updated: removed wrapped field {} from table {}",
                            wrapped_id,
                            entry.key()
                        );
                        break;
                    }
                }
            }
        }
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
    let mut subscriptions_pools: HashSet<ObjectID> = HashSet::new();
    let mut subscriptions_accounts: HashSet<SuiAddress> = HashSet::new();
    let mut subscribe_all = false;

    info!("CustomBroadcaster: New WebSocket connection established");

    info!("CustomBroadcaster: New WebSocket connection established");

    loop {
        tokio::select! {
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
                let (object_bytes, object_type) = if let Some(move_obj) = object.data.try_as_move() {
                    (
                        Some(move_obj.contents().to_vec()),
                        Some(move_obj.type_().to_string()),  // 獲取完整型別
                    )
                } else {
                    (None, None)
                };

                let msg = StreamMessage::PoolUpdate {
                    pool_id: id.to_string(),
                    digest: digest.to_string(),
                    object: object_bytes,
                    object_type,  // 傳送型別資訊
                };
                if let Err(_) = send_json(&mut socket, &msg).await { break; }
            }

            // Dynamic field 檢查
            if let sui_types::object::Owner::ObjectOwner(parent_addr) = &object.owner {
                let parent_id = ObjectID::from(*parent_addr);
                if subscriptions_pools.contains(&parent_id) {
                    let (object_bytes, object_type) = if let Some(move_obj) = object.data.try_as_move() {
                        (
                            Some(move_obj.contents().to_vec()),
                            Some(move_obj.type_().to_string()),
                        )
                    } else {
                        (None, None)
                    };

                    let msg = StreamMessage::FieldUpdate {
                        pool_id: parent_id,
                        field_id: *id,
                        digest: digest.to_string(),
                        object: object_bytes,
                        object_type,
                    };
                    if let Err(_) = send_json(&mut socket, &msg).await { break; }
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
                                    }
                                    Err(broadcast::error::RecvError::Closed) => {
                                        info!("CustomBroadcaster: Broadcast channel closed");
                                        break;
                                    }
                                }
                            }

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

                                                            SubscriptionRequest::QueryAllFields { table_id } => {
                                                                info!("Client querying all fields for table: {}", table_id);
                                                                
                                                                // 執行掃描並直接發送（不要 clone socket）
                                                                let store = state.store.clone();
                                                                
                                                                match scan_all_fields(&store, table_id).await {
                                                                    Ok(fields) => {
                                                                        let msg = StreamMessage::AllFieldsResponse {
                                                                            table_id,
                                                                            total_count: fields.len(),
                                                                            fields,
                                                                        };
                                                                        if send_json(&mut socket, &msg).await.is_err() {
                                                                            break;
                                                                        }
                                                                    }
                                                                    Err(e) => {
                                                                        error!("Failed to scan fields: {:?}", e);
                                                                        let err_msg = StreamMessage::Error {
                                                                            message: format!("Failed to scan fields: {}", e),
                                                                        };
                                                                        let _ = send_json(&mut socket, &err_msg).await;
                                                                    }
                                                                }
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
    socket.send(Message::Text(text.into())).await.map_err(|e| {
        debug!("CustomBroadcaster: Failed to send message: {}", e);
    })
}

async fn scan_all_fields(
    store: &Arc<AuthorityStore>,
    table_id: ObjectID,
) -> Result<Vec<FieldData>, anyhow::Error> {
    // 在獨立任務中執行，避免阻塞
    let store = store.clone();
    let table_id_copy = table_id;
    
tokio::task::spawn_blocking(move || {
    let mut fields = Vec::new();
    let mut scanned = 0usize;
    let mut matched = 0usize;

    let iter = store.perpetual_tables.iter_live_object_set(false);

    for live_obj in iter {
        scanned += 1;

        if scanned % 100 == 0 {
            info!(
                "scan_all_fields: scanned {} objects so far, matched {}",
                scanned, matched
            );
        }

        if let LiveObject::Normal(obj) = live_obj {
            if let Owner::ObjectOwner(parent_addr) = obj.owner {
                if ObjectID::from(parent_addr) == table_id_copy {
                    matched += 1;

                    if let Some(move_obj) = obj.data.try_as_move() {
                        fields.push(FieldData {
                            field_id: obj.id(),
                            object_bytes: move_obj.contents().to_vec(),
                            object_type: move_obj.type_().to_string(),
                        });
                    }

                    // 命中 table 時印一次（重要）
                    debug!(
                        "Matched dynamic field: field_id={}",
                        obj.id()
                    );
                }
            }
        }
    }

    info!(
        "scan_all_fields finished: scanned {} objects, found {} fields for table {}",
        scanned, matched, table_id_copy
    );

    Ok(fields)
})
.await?
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_request_parsing() {
        let json = r#"{"type":"subscribe_pool","pool_id":"0x1234"}"#;
        let req: SubscriptionRequest = serde_json::from_str(json).unwrap();
        match req {
            SubscriptionRequest::SubscribePool { pool_id } => {
                assert_eq!(pool_id, "0x1234");
            }
            _ => panic!("Expected SubscribePool"),
        }

        let json = r#"{"type":"subscribe_account","address":"0xabcd"}"#;
        let req: SubscriptionRequest = serde_json::from_str(json).unwrap();
        match req {
            SubscriptionRequest::SubscribeAccount { address } => {
                assert_eq!(address, "0xabcd");
            }
            _ => panic!("Expected SubscribeAccount"),
        }

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
        assert_eq!(try_read_u128(&data, 17), None);

        let mut data = vec![0u8; 16];
        data[0] = 1;
        assert_eq!(try_read_u128(&data, 0), Some(1));
    }

    #[test]
    fn test_try_read_i32() {
        let data: Vec<u8> = vec![0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(try_read_i32(&data, 0), Some(-1));

        let data: Vec<u8> = vec![0x01, 0x00, 0x00, 0x00];
        assert_eq!(try_read_i32(&data, 0), Some(1));
    }

    #[test]
    fn test_hex_decode_to_32_bytes() {
        let result = hex_decode_to_32_bytes(
            "0000000000000000000000000000000000000000000000000000000000001234",
        );
        assert_eq!(result[30], 0x12);
        assert_eq!(result[31], 0x34);

        let result = hex_decode_to_32_bytes("1234");
        assert_eq!(result[30], 0x12);
        assert_eq!(result[31], 0x34);
        assert_eq!(result[0], 0x00);
    }

    #[test]
    fn test_contains_subslice() {
        assert!(contains_subslice(&[1, 2, 3, 4, 5], &[2, 3]));
        assert!(contains_subslice(&[1, 2, 3, 4, 5], &[1]));
        assert!(contains_subslice(&[1, 2, 3, 4, 5], &[5]));
        assert!(!contains_subslice(&[1, 2, 3, 4, 5], &[6]));
        assert!(!contains_subslice(&[1, 2], &[1, 2, 3]));
    }
}
