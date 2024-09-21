use serde::{
    de::{Error, Unexpected, Visitor},
    Deserialize, Deserializer, Serialize,
};

use super::{
    from_str_to_side, from_str_to_status, from_str_to_tif, from_str_to_type, BullishOrder,
};

use crate::{
    connector::util::{from_str_to_f64, from_str_to_f64_opt, from_str_to_i64},
    types::{OrdType, Side, Status, TimeInForce, Value},
};

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum L2OrderBookResponseResult {
    Ok(L2OrderbookResponse),
    Err(ErrorResponse),
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum HmacResponseResult {
    Ok(HmacResponse),
    Err(ErrorResponse),
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(untagged)]
pub enum CommandResponseResult {
    Ok(CommandResponse),
    Err(ErrorResponse),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CommandResponse {
    pub message: String,
    #[serde(rename = "requestId", default)]
    pub request_id: String,
    #[serde(rename = "orderId", default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(
        rename = "clientOrderId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub client_order_id: Option<String>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct HmacResponse {
    pub token: String,
    pub authorizer: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct NonceResponse {
    #[serde(rename = "lowerBound", default)]
    pub lower_bound: u64,
    #[serde(rename = "upperBound", default)]
    pub upper_bound: u64,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum NonceResponseResult {
    Ok(NonceResponse),
    Err(ErrorResponse),
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum TradingAccountResponseResult {
    Ok(TradingAccountResponse),
    Err(ErrorResponse),
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum AssetAccountResponseResult {
    Ok(AssetAccountResponse),
    Err(ErrorResponse),
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum PerpetualPositionResponseResult {
    Ok(PerpetualPositionResponse),
    Err(ErrorResponse),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BullishCommand {
    #[serde(rename = "commandType", default)]
    pub command_type: String,
    #[serde(
        rename = "tradingAccountId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub trading_account_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BullishCommandV3 {
    #[serde(rename = "commandType", default)]
    pub command_type: String,
    #[serde(
        rename = "clientOrderId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub client_order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>, // e.g. "LIMIT", "MARKET"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity: Option<String>,
    #[serde(
        rename = "timeInForce",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub time_in_force: Option<String>,
    #[serde(
        rename = "allowBorrow",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub allow_borrow: Option<bool>,
    #[serde(
        rename = "tradingAccountId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub trading_account_id: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct L2OrderbookResponse {
    pub symbol: String,
    pub datetime: String,
    pub timestamp: String,
    #[serde(rename = "publishedAtTimestamp", default)]
    pub published_at_timestamp: String,
    #[serde(rename = "sequenceNumber", default)]
    pub sequence_number: u64,
    pub bids: Vec<BullishOrder>,
    pub asks: Vec<BullishOrder>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ErrorResponse {
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
}

impl Into<Value> for ErrorResponse {
    fn into(self) -> Value {
        // todo!: improve this to deliver detailed error information.
        Value::String(self.message.to_string())
    }
}

pub type TradingAccountResponse = Vec<TradingAccount>;
#[derive(Serialize, Deserialize, Debug)]
pub struct TradingAccount {
    #[serde(rename = "isBorrowing", skip_serializing_if = "Option::is_none")]
    pub is_borrowing: Option<String>,
    #[serde(rename = "isLending", skip_serializing_if = "Option::is_none")]
    pub is_lending: Option<String>,
    #[serde(rename = "makerFee", skip_serializing_if = "Option::is_none")]
    pub maker_fee: Option<String>,
    #[serde(rename = "takerFee", skip_serializing_if = "Option::is_none")]
    pub taker_fee: Option<String>,
    #[serde(rename = "maxInitialLeverage")]
    pub max_initial_leverage: String,
    #[serde(rename = "tradingAccountId")]
    pub trading_account_id: String,
    #[serde(rename = "tradingAccountName")]
    pub trading_account_name: String,
    #[serde(
        rename = "tradingAccountDescription",
        skip_serializing_if = "Option::is_none"
    )]
    pub trading_account_description: Option<String>,
    #[serde(rename = "isPrimaryAccount")]
    pub is_primary_account: String,
    #[serde(rename = "rateLimitToken")]
    pub rate_limit_token: String,
    #[serde(rename = "isDefaulted")]
    pub is_defaulted: String,
    #[serde(rename = "riskLimitUSD")]
    pub risk_limit_usd: String,
    #[serde(rename = "totalBorrowedUSD")]
    pub total_borrowed_usd: String,
    #[serde(rename = "totalCollateralUSD")]
    pub total_collateral_usd: String,
    #[serde(rename = "initialMarginUSD")]
    pub initial_margin_usd: String,
    #[serde(rename = "warningMarginUSD")]
    pub warning_margin_usd: String,
    #[serde(rename = "liquidationMarginUSD")]
    pub liquidation_margin_usd: String,
    #[serde(rename = "fullLiquidationMarginUSD")]
    pub full_liquidation_margin_usd: String,
    #[serde(rename = "defaultedMarginUSD")]
    pub defaulted_margin_usd: String,
    #[serde(rename = "endCustomerId")]
    pub end_customer_id: String,
    #[serde(rename = "isConcentrationRiskEnabled")]
    pub is_concentration_risk_enabled: String,
}

pub type PerpetualPositionResponse = Vec<PerpetualPosition>;
// V1TAPerpetualPosition
#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PerpetualPosition {
    pub trading_account_id: String,
    pub symbol: String,
    pub side: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub quantity: f64,
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
}

pub type AssetAccountResponse = Vec<AssetAccount>;
#[derive(Debug, Deserialize)]
pub struct AssetAccount {
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
}
