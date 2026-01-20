// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Query dynamic field data from RocksDB based on table_id (parent_id) and index range

use std::collections::HashMap;
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    dynamic_field::derive_dynamic_field_id,
    error::SuiResult,
    TypeTag,
};

use crate::authority::authority_store_tables::AuthorityPerpetualTables;

/// Query result containing the index and its corresponding field data
#[derive(Debug, Clone)]
pub struct FieldData {
    pub index: u64,
    pub field_id: ObjectID,
    pub bcs_bytes: Vec<u8>,
    pub version: SequenceNumber,
}

/// Query dynamic field objects in a range around the current_index
///
/// # Arguments
/// * `store` - The RocksDB store (AuthorityPerpetualTables)
/// * `table_id` - The parent object ID (table ID)
/// * `current_index` - The current tick index
/// * `range` - The range to query (e.g., 100000 for ±100000 ticks)
/// * `parent_version` - The parent version to use as upper bound for child lookups
/// * `key_type` - The TypeTag for the key (e.g., TypeTag::U64 for u64 keys)
///
/// # Returns
/// A HashMap mapping index to FieldData
pub fn query_field_data_range(
    store: &AuthorityPerpetualTables,
    table_id: ObjectID,
    current_index: u64,
    range: u64,
    parent_version: SequenceNumber,
    key_type: &TypeTag,
) -> SuiResult<HashMap<u64, FieldData>> {
    let lower_index = current_index.saturating_sub(range);
    let upper_index = current_index.saturating_add(range);

    let mut results = HashMap::new();

    // Iterate through all indices in the range
    for index in lower_index..=upper_index {
        // Serialize the index as BCS bytes
        let key_bytes = bcs::to_bytes(&index)
            .map_err(|e| {
                sui_types::error::SuiErrorKind::ObjectSerializationError {
                    error: format!("Failed to serialize index {}: {}", index, e),
                }
            })?;

        // Derive the field ID using the same hash function as Move
        let field_id = derive_dynamic_field_id(
            table_id,
            key_type,
            &key_bytes,
        ).map_err(|e| {
            sui_types::error::SuiErrorKind::ObjectSerializationError {
                error: format!("BCS error: {}", e),
            }
        })?;

        // Try to find the object at or before parent_version
        // This uses the reversed iterator to find the highest version <= parent_version
        if let Some(obj) = store.find_object_lt_or_eq_version(field_id, parent_version)? {
            // Verify the object is owned by the parent (validation happens in read_child_object)
            // Extract BCS bytes from the object
            if let Some(move_obj) = obj.data.try_as_move() {
                let field_data = FieldData {
                    index,
                    field_id,
                    bcs_bytes: move_obj.contents().to_vec(),
                    version: obj.version(),
                };
                results.insert(index, field_data);
            }
        }
    }

    Ok(results)
}

/// Alternative implementation using the ChildObjectResolver trait
/// This provides the parent-child ownership validation
pub fn query_field_data_range_validated(
    resolver: &impl sui_types::storage::ChildObjectResolver,
    table_id: ObjectID,
    current_index: u64,
    range: u64,
    parent_version: SequenceNumber,
    key_type: &TypeTag,
) -> SuiResult<HashMap<u64, FieldData>> {
    let lower_index = current_index.saturating_sub(range);
    let upper_index = current_index.saturating_add(range);

    let mut results = HashMap::new();

    for index in lower_index..=upper_index {
        let key_bytes = bcs::to_bytes(&index)
            .map_err(|e| {
                sui_types::error::SuiErrorKind::ObjectSerializationError {
                    error: format!("BCS error: {}", e),
                }
            })?;

        let field_id = derive_dynamic_field_id(
            table_id,
            key_type,
            &key_bytes,
        ).map_err(|e| {
            sui_types::error::SuiErrorKind::ObjectSerializationError {
                error: format!("BCS error: {}", e),
            }
        })?;

        // Use read_child_object which validates parent-child relationship
        if let Some(obj) = resolver.read_child_object(&table_id, &field_id, parent_version)? {
            if let Some(move_obj) = obj.data.try_as_move() {
                let field_data = FieldData {
                    index,
                    field_id,
                    bcs_bytes: move_obj.contents().to_vec(),
                    version: obj.version(),
                };
                results.insert(index, field_data);
            }
        }
    }

    Ok(results)
}

/// Batch query with early termination on consecutive misses
/// Useful when you expect sparse data
pub fn query_field_data_range_sparse(
    store: &AuthorityPerpetualTables,
    table_id: ObjectID,
    current_index: u64,
    range: u64,
    parent_version: SequenceNumber,
    key_type: &TypeTag,
    max_consecutive_misses: usize,
) -> SuiResult<HashMap<u64, FieldData>> {
    let lower_index = current_index.saturating_sub(range);
    let upper_index = current_index.saturating_add(range);

    let mut results = HashMap::new();
    let mut consecutive_misses = 0;

    for index in lower_index..=upper_index {
        let key_bytes = bcs::to_bytes(&index)
            .map_err(|e| {
                sui_types::error::SuiErrorKind::ObjectSerializationError {
                    error: format!("BCS error: {}", e),
                }
            })?;

        let field_id = derive_dynamic_field_id(
            table_id,
            key_type,
            &key_bytes,
        ).map_err(|e| {
            sui_types::error::SuiErrorKind::ObjectSerializationError {
                error: format!("BCS error: {}", e),
            }
        })?;

        if let Some(obj) = store.find_object_lt_or_eq_version(field_id, parent_version)? {
            if let Some(move_obj) = obj.data.try_as_move() {
                let field_data = FieldData {
                    index,
                    field_id,
                    bcs_bytes: move_obj.contents().to_vec(),
                    version: obj.version(),
                };
                results.insert(index, field_data);
                consecutive_misses = 0; // Reset on success
            }
        } else {
            consecutive_misses += 1;
            if consecutive_misses >= max_consecutive_misses {
                // Early termination if too many consecutive misses
                break;
            }
        }
    }

    Ok(results)
}

/// Decode BCS bytes into a concrete type
///
/// # Example
/// ```ignore
/// // If your field stores a simple value like u64
/// let value: u64 = decode_field_value(&field_data.bcs_bytes)?;
///
/// // If your field stores a struct, define it and derive Deserialize
/// #[derive(serde::Deserialize)]
/// struct TickData {
///     price: u64,
///     volume: u64,
/// }
/// let tick: TickData = decode_field_value(&field_data.bcs_bytes)?;
/// ```
pub fn decode_field_value<'de, T: serde::Deserialize<'de>>(
    bcs_bytes: &'de [u8],
) -> Result<T, bcs::Error> {
    bcs::from_bytes(bcs_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_id_derivation() {
        // Test that field ID derivation is consistent
        let table_id = ObjectID::random();
        let index = 12345u64;
        let key_bytes = bcs::to_bytes(&index).unwrap();
        let key_type = TypeTag::U64;

        let field_id1 = derive_dynamic_field_id(table_id, &key_type, &key_bytes).unwrap();
        let field_id2 = derive_dynamic_field_id(table_id, &key_type, &key_bytes).unwrap();

        assert_eq!(field_id1, field_id2, "Field ID derivation should be deterministic");
    }
}
