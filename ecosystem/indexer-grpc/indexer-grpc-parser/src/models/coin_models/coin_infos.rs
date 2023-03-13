// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::coin_utils::{CoinInfoType, CoinResource};
use crate::{
    schema::coin_infos,
    utils::{database::PgPoolConnection, util::standardize_address},
};
use aptos_protos::transaction::testing1::v1::WriteResource;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl};
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(coin_type_hash))]
#[diesel(table_name = coin_infos)]
pub struct CoinInfo {
    pub coin_type_hash: String,
    pub coin_type: String,
    pub transaction_version_created: i64,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub transaction_created_timestamp: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle: Option<String>,
    pub supply_aggregator_table_key: Option<String>,
}

#[derive(Debug, Deserialize, Identifiable, Queryable, Serialize)]
#[diesel(primary_key(coin_type_hash))]
#[diesel(table_name = coin_infos)]
pub struct CoinInfoQuery {
    pub coin_type_hash: String,
    pub coin_type: String,
    pub transaction_version_created: i64,
    pub creator_address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i32,
    pub transaction_created_timestamp: chrono::NaiveDateTime,
    pub inserted_at: chrono::NaiveDateTime,
    pub supply_aggregator_table_handle: Option<String>,
    pub supply_aggregator_table_key: Option<String>,
}

impl CoinInfo {
    /// We can find coin info from resources. If the coin info appears multiple times we will only keep the first transaction because it can't be modified.
    pub fn from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        txn_timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<Option<Self>> {
        match &CoinResource::from_write_resource(write_resource, txn_version)? {
            Some(CoinResource::CoinInfoResource(inner)) => {
                let coin_info_type = &CoinInfoType::from_move_type(
                    &write_resource.r#type.as_ref().unwrap().generic_type_params[0],
                    write_resource.r#type.as_ref().unwrap().address.as_str(),
                    txn_version,
                );
                let (supply_aggregator_table_handle, supply_aggregator_table_key) = inner
                    .get_aggregator_metadata()
                    .map(|agg| (Some(agg.handle), Some(agg.key)))
                    .unwrap_or((None, None));

                Ok(Some(Self {
                    coin_type_hash: coin_info_type.to_hash(),
                    coin_type: coin_info_type.get_coin_type_trunc(),
                    transaction_version_created: txn_version,
                    creator_address: standardize_address(coin_info_type.creator_address.as_str()),
                    name: inner.get_name_trunc(),
                    symbol: inner.get_symbol_trunc(),
                    decimals: inner.decimals,
                    transaction_created_timestamp: txn_timestamp,
                    supply_aggregator_table_handle,
                    supply_aggregator_table_key,
                }))
            },
            _ => Ok(None),
        }
    }
}

impl CoinInfoQuery {
    pub fn get_by_coin_type(
        coin_type: String,
        conn: &mut PgPoolConnection,
    ) -> diesel::QueryResult<Option<Self>> {
        coin_infos::table
            .filter(coin_infos::coin_type.eq(coin_type))
            .first::<Self>(conn)
            .optional()
    }
}
