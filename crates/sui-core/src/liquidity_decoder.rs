// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Liquidity pool detection and decoding utilities.
//!
//! This module provides functionality to identify and decode DEX/AMM liquidity pool
//! objects from Sui transaction outputs, enabling real-time streaming of pool state
//! changes before they are written to RocksDB.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sui_types::object::MoveObject;

/// Known liquidity pool protocol patterns.
/// Each tuple contains (type pattern, protocol name).
pub static KNOWN_POOL_PATTERNS: &[(&str, &str)] = &[
    ("cetus_clmm::pool::Pool", "cetus"),
    ("turbos_clmm::pool::Pool", "turbos"),
    ("suiswap::pool::Pool", "suiswap"),
    ("deepbook::clob_v2::Pool", "deepbook"),
    ("deepbook::clob::Pool", "deepbook"),
    ("kriya::pool::Pool", "kriya"),
    ("aftermath::amm::Pool", "aftermath"),
    ("flowx::pool::Pool", "flowx"),
    ("bluemove::pool::Pool", "bluemove"),
];

/// Decoded liquidity pool state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiquidityPoolState {
    /// Pool protocol identifier (cetus, turbos, suiswap, etc.)
    pub protocol: String,
    /// Token A type string
    pub token_a_type: String,
    /// Token B type string
    pub token_b_type: String,
    /// Generic decoded fields as JSON (protocol-specific)
    pub fields: Value,
}

/// Liquidity subscription configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiquiditySubscription {
    /// Type patterns for Move type matching, e.g., "cetus_clmm::pool::Pool"
    pub type_patterns: Vec<String>,
    /// Optional specific pool IDs to subscribe to (as hex strings)
    #[serde(default)]
    pub pool_ids: Option<Vec<String>>,
    /// Include raw object bytes in response
    #[serde(default)]
    pub include_raw_bytes: bool,
}

impl Default for LiquiditySubscription {
    fn default() -> Self {
        Self {
            // Default to all known pool types
            type_patterns: KNOWN_POOL_PATTERNS
                .iter()
                .map(|(p, _)| p.to_string())
                .collect(),
            pool_ids: None,
            include_raw_bytes: false,
        }
    }
}

/// Check if a Move type matches any of the provided liquidity pool patterns.
pub fn matches_liquidity_pattern(type_str: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|p| type_str.contains(p))
}

/// Detect protocol from type string using known patterns.
pub fn detect_protocol(type_str: &str) -> Option<&'static str> {
    KNOWN_POOL_PATTERNS
        .iter()
        .find(|(pattern, _)| type_str.contains(pattern))
        .map(|(_, name)| *name)
}

/// Extract token type parameters from pool type string.
///
/// Parses generic parameters from type like:
/// `0x...::pool::Pool<0x2::sui::SUI, 0x...::usdc::USDC>`
///
/// Returns `(token_a_type, token_b_type)` if successful.
pub fn extract_pool_tokens(type_str: &str) -> Option<(String, String)> {
    let start = type_str.find('<')?;
    let end = type_str.rfind('>')?;

    if start >= end {
        return None;
    }

    let params = &type_str[start + 1..end];

    // Handle nested generics by tracking angle bracket depth
    let mut depth = 0;
    let mut split_idx = None;

    for (i, c) in params.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                split_idx = Some(i);
                break;
            }
            _ => {}
        }
    }

    let split_idx = split_idx?;
    let token_a = params[..split_idx].trim().to_string();
    let token_b = params[split_idx + 1..].trim().to_string();

    // Handle case where there might be more type params (take first two)
    let token_b = if let Some(comma_idx) = find_top_level_comma(&token_b) {
        token_b[..comma_idx].trim().to_string()
    } else {
        token_b
    };

    Some((token_a, token_b))
}

/// Find the index of the first top-level comma (not inside angle brackets).
fn find_top_level_comma(s: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Decode liquidity pool state from Move object.
///
/// Returns a generic JSON structure since each protocol has different fields.
/// Full BCS decoding would require protocol-specific struct definitions.
pub fn decode_liquidity_state(obj: &MoveObject, type_str: &str) -> Option<LiquidityPoolState> {
    let protocol = detect_protocol(type_str)?;

    // Extract type parameters (token types)
    let (token_a, token_b) = extract_pool_tokens(type_str)
        .unwrap_or_else(|| ("unknown".to_string(), "unknown".to_string()));

    // For now, return basic metadata.
    // Full BCS decoding would require protocol-specific struct definitions.
    let fields = json!({
        "raw_content_length": obj.contents().len(),
        "version": obj.version().value(),
    });

    Some(LiquidityPoolState {
        protocol: protocol.to_string(),
        token_a_type: token_a,
        token_b_type: token_b,
        fields,
    })
}

/// Check if an object type string represents a known liquidity pool.
pub fn is_liquidity_pool(type_str: &str) -> bool {
    KNOWN_POOL_PATTERNS
        .iter()
        .any(|(pattern, _)| type_str.contains(pattern))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_pool_tokens_simple() {
        let type_str = "0x123::pool::Pool<0x2::sui::SUI, 0x456::usdc::USDC>";
        let result = extract_pool_tokens(type_str);
        assert_eq!(
            result,
            Some(("0x2::sui::SUI".to_string(), "0x456::usdc::USDC".to_string()))
        );
    }

    #[test]
    fn test_extract_pool_tokens_nested() {
        let type_str = "0x123::pool::Pool<0x2::coin::Coin<0x2::sui::SUI>, 0x456::usdc::USDC>";
        let result = extract_pool_tokens(type_str);
        assert_eq!(
            result,
            Some((
                "0x2::coin::Coin<0x2::sui::SUI>".to_string(),
                "0x456::usdc::USDC".to_string()
            ))
        );
    }

    #[test]
    fn test_extract_pool_tokens_three_params() {
        let type_str = "0x123::pool::Pool<0x2::sui::SUI, 0x456::usdc::USDC, 0x789::extra::Extra>";
        let result = extract_pool_tokens(type_str);
        assert_eq!(
            result,
            Some(("0x2::sui::SUI".to_string(), "0x456::usdc::USDC".to_string()))
        );
    }

    #[test]
    fn test_detect_protocol() {
        assert_eq!(
            detect_protocol("0x123::cetus_clmm::pool::Pool<A, B>"),
            Some("cetus")
        );
        assert_eq!(
            detect_protocol("0x456::turbos_clmm::pool::Pool<X, Y>"),
            Some("turbos")
        );
        assert_eq!(detect_protocol("0x789::unknown::Type<A, B>"), None);
    }

    #[test]
    fn test_matches_liquidity_pattern() {
        let patterns = vec![
            "cetus_clmm::pool::Pool".to_string(),
            "turbos_clmm::pool::Pool".to_string(),
        ];

        assert!(matches_liquidity_pattern(
            "0x123::cetus_clmm::pool::Pool<A, B>",
            &patterns
        ));
        assert!(!matches_liquidity_pattern(
            "0x123::unknown::Pool<A, B>",
            &patterns
        ));
    }

    #[test]
    fn test_is_liquidity_pool() {
        assert!(is_liquidity_pool("0x123::cetus_clmm::pool::Pool<A, B>"));
        assert!(is_liquidity_pool("0x456::deepbook::clob_v2::Pool<X, Y>"));
        assert!(!is_liquidity_pool("0x789::my_module::MyStruct"));
    }
}
