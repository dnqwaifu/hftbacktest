use std::collections::HashMap;

use serde::{
    Deserialize, Serialize,
};

use super::{
    deserialize_i32_from_string_or_int, from_str_to_side, from_str_to_status, from_str_to_tif,
    from_str_to_type,
};


use crate::{
    connector::util::{from_str_to_f64, from_str_to_f64_opt, from_str_to_i64},
    types::{OrdType, Side, Status, TimeInForce},
};


#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct JsonRpcArgs {
    pub resource: String,
    pub params: Vec<Param>,
    pub topics: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Param {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum OrderbookStreamMsg {
    JsonRpc(JsonRpc),
    BullishWebSocketResponse(BullishWebSocketResponse),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum TradesStreamMsg {
    JsonRpc(JsonRpc),
    BullishWebSocketResponse(BullishWebSocketResponse),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum PrivateStreamMsg {
    JsonRpc(JsonRpc),
    BullishWebSocketResponse(BullishWebSocketResponse),
}

#[derive(Deserialize, Debug, Clone)]
pub struct JsonRpc {
    jsonrpc: String,
    id: String,
    result: serde_json::Value, // placeholder to parse later
}

#[derive(Deserialize, Debug, Clone)]
pub struct JsonRpcResult {
    #[serde(rename = "responseCodeName", default)]
    response_code_name: String,
    #[serde(
        rename = "responseCode",
        default,
        deserialize_with = "deserialize_i32_from_string_or_int"
    )]
    response_code: i32,
    message: String,
}

#[derive(Debug, Deserialize)]
pub enum EventType {
    Update(PrivateTrade),
    Snapshot(Vec<PrivateTrade>),
}

// V1TAAnonymousTradeUpdate Embedded
#[derive(Deserialize, Debug, Clone)]
pub struct Trade {
    #[serde(rename = "isTaker", default)]
    pub is_taker: bool,
    pub side: String,
    pub price: String,
    pub quantity: String,
    pub symbol: String,
    #[serde(rename = "publishedAtTimestamp", default)]
    pub published_at_timestamp: String,
    #[serde(rename = "createdAtTimestamp", default, deserialize_with = "from_str_to_i64")]
    pub created_at_timestamp: i64,
    #[serde(rename = "tradeId", default)]
    pub trade_id: String,
}

// V1TAAnonymousTradeUpdate
#[derive(Deserialize, Debug, Clone)]
pub struct AnonymousTrades {
    pub trades: Vec<Trade>,
    pub symbol: String,
    #[serde(rename = "publishedAtTimestamp", default)]
    pub published_at_timestamp: String,
    #[serde(rename = "createdAtTimestamp", deserialize_with = "from_str_to_i64")]
    pub created_at_timestamp: i64,
    #[serde(rename = "sequenceNumber", default)]
    pub sequence_number: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BullishWebSocketResponse {
    #[serde(
        rename = "tradingAccountId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub trading_account_id: Option<String>,
    #[serde(rename = "type", default)]
    pub event_type: String, // "update" OR "snapshot"
    #[serde(rename = "dataType", default)]
    pub event_topic: String, // "V1TALevel2" or ...
    pub data: serde_json::Value, // placeholder to parse later
}

// V1TATrade
#[derive(Debug, Deserialize, Clone)]
pub struct PrivateTrade {
    #[serde(rename = "tradeId", default)]
    pub trade_id: String,
    #[serde(rename = "orderId", default)]
    pub order_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    pub symbol: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub quantity: f64,
    #[serde(rename = "quoteAmount", default)]
    pub quote_amount: String,
    #[serde(rename = "baseFee", default)]
    pub base_fee: String,
    #[serde(rename = "quoteFee", default)]
    pub quote_fee: String,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "isTaker", default)]
    pub is_taker: bool,
    #[serde(rename = "createdAtDatetime", default)]
    pub created_at_datetime: String,
    #[serde(rename = "createdAtTimestamp", deserialize_with = "from_str_to_i64")]
    pub created_at_timestamp: i64,
    #[serde(rename = "publishedAtTimestamp", deserialize_with = "from_str_to_i64")]
    pub published_at_timestamp: i64,
}

// V1TAOrder
#[derive(Debug, Deserialize)]
pub struct PrivateOrder {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    #[serde(
        rename = "clientOrderId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub client_order_id: Option<String>,
    #[serde(rename = "orderId", default)]
    pub order_id: String,
    pub symbol: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(
        rename = "averageFillPrice",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "from_str_to_f64_opt"
    )]
    pub average_fill_price: Option<f64>,
    #[serde(
        rename = "stopPrice",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "from_str_to_f64_opt"
    )]
    pub stop_price: Option<f64>,
    pub margin: bool,
    #[serde(rename = "quantity", deserialize_with = "from_str_to_f64")]
    pub quantity: f64,
    #[serde(rename = "quantityFilled", deserialize_with = "from_str_to_f64_opt")]
    pub quantity_filled: Option<f64>,
    #[serde(
        rename = "quoteAmount",
        default,
        deserialize_with = "from_str_to_f64_opt"
    )]
    pub quote_amount: Option<f64>,
    #[serde(
        rename = "baseFee",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "from_str_to_f64_opt"
    )]
    pub base_fee: Option<f64>,
    #[serde(rename = "quoteFee", default, deserialize_with = "from_str_to_f64")]
    pub quote_fee: f64,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(
        rename = "borrowedQuantity",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "from_str_to_f64_opt"
    )]
    pub borrowed_quantity: Option<f64>,
    #[serde(
        rename = "borrowedBaseQuantity",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "from_str_to_f64_opt"
    )]
    pub borrowed_base_quantity: Option<f64>,
    #[serde(
        rename = "borrowedQuoteQuantity",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "from_str_to_f64_opt"
    )]
    pub borrowed_quote_quantity: Option<f64>,
    #[serde(rename = "isLiquidation", default)]
    pub is_liquidation: bool,
    #[serde(rename = "type", deserialize_with = "from_str_to_type")]
    pub order_type: OrdType,
    #[serde(rename = "timeInForce", deserialize_with = "from_str_to_tif")]
    pub time_in_force: TimeInForce,
    #[serde(default)]
    pub status: String,
    #[serde(rename = "statusReason", deserialize_with = "from_str_to_status")]
    pub status_reason: Status,
    #[serde(rename = "statusReasonCode", default)]
    pub status_reason_code: i64,
    #[serde(rename = "createdAtDatetime", default)]
    pub created_at_datetime: String,
    #[serde(rename = "createdAtTimestamp", deserialize_with = "from_str_to_i64")]
    pub created_at_timestamp: i64,
    #[serde(rename = "publishedAtTimestamp", deserialize_with = "from_str_to_i64")]
    pub published_at_timestamp: i64,
}

//V1TAAssetAccount
#[derive(Debug, Deserialize)]
pub struct PrivateAssetAccount {
    #[serde(rename = "tradingAccountId", default)]
    pub trading_account_id: String,
    #[serde(rename = "assetId", default)]
    pub asset_id: String,
    #[serde(rename = "assetSymbol", default)]
    pub asset_symbol: String,
    #[serde(rename = "availableQuantity", default)]
    pub available_quantity: String,
    #[serde(rename = "borrowedQuantity", default)]
    pub borrowed_quantity: String,
    #[serde(rename = "lockedQuantity", default)]
    pub locked_quantity: String,
    #[serde(rename = "loadnedQuantity", default)]
    pub loaned_quantity: String,
    #[serde(rename = "updatedAtDatetime", default)]
    pub updated_at_datetime: String,
    #[serde(rename = "updatedAtTimestamp", default)]
    pub updated_at_timestamp: String,
    #[serde(rename = "publishedAtTimestamp", default)]
    pub published_at_timestamp: String,
}

// V1TATradingAccount
#[derive(Debug, Deserialize)]
pub struct PrivateTradingAccount {
    #[serde(rename = "tradingAccountId")]
    pub trading_account_id: String,
    #[serde(rename = "totalBorrowedQuantity")]
    pub total_borrowed_quantity: String,
    #[serde(rename = "totalCollateralQuantity")]
    pub total_collateral_quantity: String,
    #[serde(rename = "totalBorrowedUSD")]
    pub total_borrowed_usd: String,
    #[serde(rename = "totalCollateralUSD")]
    pub total_collateral_usd: String,
    #[serde(rename = "referenceAssetSymbol")]
    pub reference_asset_symbol: String,
    #[serde(rename = "initialMarginUSD")]
    pub initial_margin_usd: String,
    #[serde(rename = "warningMarginUSD")]
    pub warning_margin_usd: String,
    #[serde(rename = "liquidationMarginUSD")]
    pub liquidation_margin_usd: String,
    #[serde(rename = "fullLiquidationMarginUSD")]
    pub full_liquidation_margin_usd: String,
    #[serde(rename = "endCustomerId", skip_serializing_if = "Option::is_none")]
    pub end_customer_id: Option<String>,
    #[serde(rename = "defaultedMarginUSD")]
    pub defaulted_margin_usd: String,
    #[serde(rename = "riskLimitUSD")]
    pub risk_limit_usd: String,
    #[serde(rename = "maxInitialLeverage")]
    pub max_initial_leverage: String,
    #[serde(rename = "isPrimaryAccount")]
    pub is_primary_account: bool,
    #[serde(rename = "isBorrowing")]
    pub is_borrowing: bool,
    #[serde(rename = "isLending")]
    pub is_lending: bool,
    #[serde(rename = "isDefaulted")]
    pub is_defaulted: bool,
    #[serde(rename = "takerFee", skip_serializing_if = "Option::is_none")]
    pub taker_fee: Option<String>,
    #[serde(rename = "makerFee", skip_serializing_if = "Option::is_none")]
    pub maker_fee: Option<String>,
    #[serde(rename = "updatedAtDatetime")]
    pub updated_at_datetime: String,
    #[serde(rename = "updatedAtTimestamp")]
    pub updated_at_timestamp: String,
    #[serde(rename = "publishedAtTimestamp")]
    pub published_at_timestamp: String,
}

// V1TAPerpetualPosition
#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrivatePerpetualPosition {
    pub trading_account_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub notional: String,
    pub entry_notional: String,
    pub mtm_pnl: String,
    pub reported_mtm_pnl: String,
    pub reported_funding_pnl: String,
    pub realized_pnl: String,
    pub created_at_datetime: String,
    pub created_at_timestamp: String,
    pub updated_at_datetime: String,
    pub updated_at_timestamp: String,
    pub published_at_timestamp: String,
}
// V1TAErrorResponse
#[derive(Deserialize, Serialize, Debug)]
pub struct PrivateErrorResponse {
    pub message: String,
    #[serde(rename = "errorCode", default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<i64>,
    #[serde(
        rename = "errorCodeName",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub error_code_name: Option<String>,
    #[serde(rename = "requestId", default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    #[serde(rename = "orderId", default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

// V1TALevel1
#[derive(Deserialize, Debug)]
pub struct OrderBookL1 {
    pub symbol: String,
    pub datetime: String,
    #[serde(deserialize_with = "from_str_to_i64")]
    pub timestamp: i64,
    #[serde(rename = "publishedAtTimestamp", default)]
    pub published_at_timestamp: String,
    #[serde(rename = "sequenceNumber", default)]
    pub sequence_number: String,
    pub bid: Vec<String>,
    pub ask: Vec<String>,
}

// V1TAHeartBeat
#[derive(Deserialize, Debug)]
pub struct PrivateHeartBeat {
    #[serde(rename = "createdAtTimestamp", default, deserialize_with = "from_str_to_i64")]
    pub created_at_timestamp: i64,
    #[serde(rename = "sequenceNumber", default)]
    pub sequence_number: String,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Subscription {
    pub jsonrpc: &'static str,
    pub method: &'static str,
    pub r#type: &'static str,
    pub id: String,
    pub params: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Ping {
    pub jsonrpc: &'static str,
    pub method: &'static str,
    pub r#type: &'static str,
    pub id: String,
    pub params: HashMap<(), ()>,
}

// V1TALevel2
#[derive(Deserialize, Debug, Clone)]
pub struct OrderBookL2 {
    //#[serde(rename = "u")]
    #[serde(deserialize_with = "from_str_to_i64")]
    pub timestamp: i64,
    //#[serde(rename = "b")]
    pub bids: Vec<String>,
    //#[serde(rename = "a")]
    pub asks: Vec<String>,
    //#[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "sequenceNumberRange", default)]
    pub sequence_number_range: Vec<i64>,
    pub datetime: String,
    #[serde(rename = "publishedAtTimestamp", default)]
    pub published_at_timestamp: String,
}
