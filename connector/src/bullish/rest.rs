use std::{sync::Arc, time::SystemTime};

use chrono::Utc;
use reqwest::Response;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use std::{fmt::Write, num::ParseIntError};

use crate::{
    connector::{
        bullish::{
            msg::{
                rest::{
                    AssetAccountResponse, AssetAccountResponseResult, BullishCommandV3,
                    CommandResponse, CommandResponseResult, ErrorResponse, HmacResponse,
                    HmacResponseResult, L2OrderBookResponseResult, L2OrderbookResponse,
                    NonceResponse, NonceResponseResult, PerpetualPositionResponse,
                    PerpetualPositionResponseResult, TradingAccount, TradingAccountResponseResult,
                },
                BullishOrder,
            },
            BullishError,
        },
        util::sign_hmac_sha256,
    },
    types::{OrdType, Side, TimeInForce},
};

#[derive(Clone)]
pub struct BullishClient {
    client: reqwest::Client,
    url: String,
    jwt: String,
    authorizer: String,
    api_key: String,
    trading_account_id: String,
    secret: String,
    nonce: String,
}

impl BullishClient {
    pub fn new(url: &str, api_key: &str, secret: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.to_string(),
            secret: secret.to_string(),
            api_key: api_key.to_string(),
            jwt: String::default(),
            trading_account_id: String::default(),
            authorizer: String::default(),
            nonce: String::default(),
        }
    }

    async fn get<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        query: &str,
    ) -> Result<T, reqwest::Error> {
        let resp = self
            .client
            .get(&format!("{}{}?{}", self.url, path, query))
            .header("Accept", "application/json")
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    async fn post_signed<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: String,
        jwt: &str,
        nonce: &str,
        signature: &str,
        timestamp: &str,
    ) -> Result<T, reqwest::Error> {
        tracing::info!(?body, "CommandV3");
        let resp = self
            .client
            .post(&format!("{}{}", self.url, path))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {jwt}"))
            .header("BX-NONCE", nonce)
            .header("BX-SIGNATURE", signature)
            .header("BX-TIMESTAMP", timestamp)
            .header("BX-NONCE-WINDOW-ENABLED", "true".to_string())
            .body(body)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    // get but with a bearer token authorization header
    async fn get_jwted<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        jwt: &str,
    ) -> Result<T, reqwest::Error> {
        let resp = self
            .client
            .get(&format!("{}{}", self.url, path))
            .header("Authorization", format!("Bearer {jwt}"))
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    // get but with a signature
    async fn get_signed<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        api_key: &str,
        nonce: &str,
        signature: &str,
        timestamp: &str,
    ) -> Result<T, reqwest::Error> {
        let resp = self
            .client
            .get(&format!("{}{}", self.url, path))
            .header("BX-PUBLIC-KEY", api_key)
            .header("BX-NONCE", nonce)
            .header("BX-SIGNATURE", signature)
            .header("BX-TIMESTAMP", timestamp)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    // fetch valid nonce window, not really needed as we can compute it
    pub async fn get_nonce(&self) -> Result<NonceResponse, ErrorResponse> {
        let resp: NonceResponseResult = self
            .get(&format!("/trading-api/v1/nonce"), &format!(""))
            .await
            .unwrap();
        match resp {
            /* data did not match any variant of untagged enum OrderBookResponseResult */
            NonceResponseResult::Ok(resp) => Ok(resp),
            NonceResponseResult::Err(resp) => Err(resp),
        }
    }

    pub async fn submit_command(
        &self,
        cmd: BullishCommandV3,
    ) -> Result<CommandResponse, ErrorResponse> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let next_nonce = now.as_micros().to_string();
        let ts = now.as_millis().to_string();
        let body = serde_json::to_string(&cmd).unwrap();
        let message = format!("{ts}{next_nonce}POST/trading-api/v2/command{body}");
        let mut hasher = Sha256::new();
        hasher.update(message.as_bytes());
        let digest = encode_hex(hasher.finalize().as_slice());
        let sig = sign_hmac_sha256(&self.secret, &digest);

        let resp = self
            .post_signed(
                &format!("/trading-api/v2/command"),
                body,
                &self.jwt,
                &next_nonce,
                &sig,
                &ts,
            )
            .await;
        let resp = resp.unwrap();
        match resp {
            CommandResponseResult::Ok(resp) => Ok(resp),
            CommandResponseResult::Err(resp) => Err(resp),
        }
    }

    pub async fn cancel_order(
        &self,
        client_order_id: &str,
        symbol: &str,
    ) -> Result<CommandResponse, BullishError> {
        let cmd = BullishCommandV3 {
            command_type: "V3CancelOrder".to_string(),
            client_order_id: Some(client_order_id.to_owned()),
            symbol: Some(symbol.to_owned()),
            r#type: None,
            side: None,
            price: None,
            quantity: None,
            time_in_force: None,
            allow_borrow: None,
            trading_account_id: Some(self.trading_account_id.clone()),
        };
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        // let mics_since_midnight = now.as_micros() % 86400000000;
        // let nonce = now.as_micros() - mics_since_midnight;
        let next_nonce = now.as_micros().to_string();
        let ts = now.as_millis().to_string();
        let body = serde_json::to_string(&cmd).unwrap();
        let message = format!("{ts}{next_nonce}POST/trading-api/v2/command{body}");
        let mut hasher = Sha256::new();
        hasher.update(message.as_bytes());
        let digest = encode_hex(hasher.finalize().as_slice());
        let sig = sign_hmac_sha256(&self.secret, &digest);

        let resp = self
            .post_signed(
                &format!("/trading-api/v2/command"),
                body,
                &self.jwt,
                &next_nonce,
                &sig,
                &ts,
            )
            .await;
        let resp = resp.unwrap();
        match resp {
            /* data did not match any variant of untagged enum OrderBookResponseResult */
            CommandResponseResult::Ok(resp) => Ok(resp),
            CommandResponseResult::Err(resp) => Err(BullishError::OrderError(
                resp.error_code.unwrap_or(-1),
                resp.message,
            )),
        }
    }

    pub async fn submit_order(
        mut self,
        trading_account_id: &str,
        client_order_id: &str,
        symbol: &str,
        side: Side,
        price: f64,
        price_prec: usize,
        qty: f64,
        order_type: OrdType,
        time_in_force: TimeInForce,
    ) -> Result<CommandResponse, BullishError> {
        let price = Some(format!("{:.prec$}", price, prec = price_prec));
        let side_str: &str = side.as_ref();
        // Bullish does not support GTX so we can use
        // POST_ONLY 
        let cmd = BullishCommandV3 {
            command_type: "V3CreateOrder".to_string(),
            client_order_id: Some(client_order_id.to_owned()),
            symbol: Some(symbol.to_owned()),
            r#type: Some(order_type.as_ref().to_string()),
            side: Some(side_str.to_string()),
            price,
            quantity: Some(format!("{:.5}", qty)),
            time_in_force: Some(
                match time_in_force {
                    TimeInForce::GTC => "GTC".to_string(),
                    //  Bullish has no GTX
                    TimeInForce::GTX => "GTC".to_string(),
                    TimeInForce::FOK => "FOK".to_string(),
                    TimeInForce::IOC => "IOC".to_string(),
                    TimeInForce::Unsupported => {
                        return Err(BullishError::InvalidRequest)
                    }
                }
            ),
            allow_borrow: Some(false),
            trading_account_id: Some(trading_account_id.to_owned()),
        };

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        // let mics_since_midnight = now.as_micros() % 86400000000;
        // let nonce = now.as_micros() - mics_since_midnight;
        let next_nonce = now.as_micros().to_string();
        let ts = now.as_millis().to_string();
        let body = serde_json::to_string(&cmd).unwrap();
        let message = format!("{ts}{next_nonce}POST/trading-api/v2/orders{body}");
        let mut hasher = Sha256::new();
        hasher.update(message.as_bytes());
        let digest = encode_hex(hasher.finalize().as_slice());
        let sig = sign_hmac_sha256(&self.secret, &digest);

        let resp = self
            .post_signed(
                &format!("/trading-api/v2/orders"),
                body,
                &self.jwt,
                &next_nonce,
                &sig,
                &ts,
            )
            .await?;
        match resp {
            CommandResponseResult::Ok(resp) => Ok(resp),
            CommandResponseResult::Err(resp) => Err(BullishError::OrderError(
                resp.error_code.unwrap_or(-1),
                resp.message,
            )),
        }
    }

    pub async fn cancel_all_orders(&self, symbol: &str) -> Result<CommandResponse, ErrorResponse> {
        let cmd = BullishCommandV3 {
            command_type: "V1CancelAllOrders".to_string(),
            client_order_id: None,
            symbol: Some(symbol.to_string()),
            r#type: None,
            side: None,
            price: None,
            quantity: None,
            time_in_force: None,
            allow_borrow: None,
            trading_account_id: Some(self.trading_account_id.clone()),
        };
        let resp = self.submit_command(cmd).await;
        resp
    }

    pub async fn get_balance_information(&self) -> Result<AssetAccountResponse, ErrorResponse> {
        let resp = self
            .get_jwted(&format!("/trading-api/v1/accounts/assets"), &self.jwt)
            .await;
        let resp = resp.unwrap();
        match resp {
            /* data did not match any variant of untagged enum OrderBookResponseResult */
            AssetAccountResponseResult::Ok(resp) => Ok(resp),
            AssetAccountResponseResult::Err(resp) => Err(resp),
        }
    }

    pub async fn get_position_information(
        &self,
    ) -> Result<PerpetualPositionResponse, ErrorResponse> {
        let resp = self
            .get_jwted(&format!("/trading-api/v1/accounts/assets"), &self.jwt)
            .await;
        let resp = resp.unwrap();
        match resp {
            /* data did not match any variant of untagged enum OrderBookResponseResult */
            PerpetualPositionResponseResult::Ok(resp) => Ok(resp),
            PerpetualPositionResponseResult::Err(err) => Err(err),
        }
    }

    pub async fn configure_trading_account(
        &mut self,
    ) -> Result<Vec<TradingAccount>, ErrorResponse> {
        let resp = self
            .get_jwted(
                &format!("/trading-api/v1/accounts/trading-accounts"),
                &self.jwt,
            )
            .await;
        let resp = resp.unwrap();
        match resp {
            /* data did not match any variant of untagged enum OrderBookResponseResult */
            TradingAccountResponseResult::Ok(resp) => {
                if let Some(trading_account) = resp.first() {
                    self.trading_account_id = trading_account.trading_account_id.clone();
                    Ok(resp)
                } else {
                    tracing::error!("No trading accounts");
                    Ok(resp)
                }
            }
            TradingAccountResponseResult::Err(resp) => Err(resp),
        }
    }

    pub async fn refresh_jwt(&mut self) -> Result<HmacResponse, ErrorResponse> {
        tracing::debug!("Refreshing jwt...");
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        let message = &format!("{ts}{ts}GET/trading-api/v1/users/hmac/login");
        let sig = sign_hmac_sha256(&self.secret, message);

        let resp = self
            .get_signed(
                &format!("/trading-api/v1/users/hmac/login"),
                &self.api_key,
                ts.as_str(),
                sig.as_str(),
                ts.as_str(),
            )
            .await;
        let resp = resp.unwrap();
        match resp {
            /* data did not match any variant of untagged enum OrderBookResponseResult */
            HmacResponseResult::Ok(resp) => {
                self.jwt = resp.token.clone();
                self.authorizer = resp.authorizer.clone();
                Ok(resp)
            }
            HmacResponseResult::Err(resp) => Err(resp),
        }
    }
}

// for hmac
pub fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

// for hmac
pub fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        write!(&mut s, "{:02x}", b).unwrap();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration};

    #[tokio::test]
    async fn test_nonce_window_can_be_computed() {
        let client =
            BullishClient::new(&"https://api.simnext.bullish-test.com", "api_key", "secret");
        let got = client.get_nonce().await.unwrap().lower_bound;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let mics_since_midnight = now.as_micros() % 86400000000;
        let want = now.as_micros() - mics_since_midnight;
        assert_eq!(got, want as u64);
    }
}
