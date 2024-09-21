use std::{
    collections::{HashMap, HashSet},
    sync::{
        mpsc::{channel, Sender},
        Arc, Mutex,
    },
    time::Duration,
};

use thiserror::Error;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;
use ws::{connect_orderbook, connect_private, connect_trades};

use crate::{
    connector::{
        bullish::{
            ordermanager::{OrderManager, OrderManagerWrapper},
            rest::BullishClient,
        },
        Connector,
    },
    live::Asset,
    types::{BuildError, ErrorKind, LiveError, LiveEvent, Order, Status, Value},
    utils::get_precision,
};

mod msg;
mod ordermanager;
mod rest;
mod ws;

#[derive(Clone)]
pub enum Endpoint {
    Public,
    Testnet,
    Custom(String),
}

impl From<String> for Endpoint {
    fn from(value: String) -> Self {
        Endpoint::Custom(value)
    }
}

impl From<&'static str> for Endpoint {
    fn from(value: &'static str) -> Self {
        Endpoint::Custom(value.to_string())
    }
}

/// Bullish connector [`Bullish`] builder.
/// Currently only `public` is supported.
pub struct BullishBuilder {
    orderbook_url: String,
    trades_url: String,
    private_url: String,
    rest_url: String,
    api_key: String,
    secret: String,
    topics: HashSet<String>,
    order_prefix: String,
}

impl BullishBuilder {
    /// Sets an endpoint to connect.
    pub fn endpoint(self, endpoint: Endpoint) -> Self {
        if let Endpoint::Custom(_) = endpoint {
            panic!("Use `rest_url` to set a custom endpoint instead");
        }
        self.rest_url(endpoint)
    }

    /// Sets the REST API endpoint url.
    pub fn rest_url<E: Into<Endpoint>>(self, endpoint: E) -> Self {
        match endpoint.into() {
            Endpoint::Public => Self {
                rest_url: "https://api.exchange.bullish.com".to_string(),
                ..self
            },
            Endpoint::Testnet => Self {
                rest_url: "https://api.simnext.bullish-test.com".to_string(),
                ..self
            },
            Endpoint::Custom(rest_url) => Self { rest_url, ..self },
        }
    }

    /// Sets the pubic orderbook websocket url (Unified)
    pub fn orderbook_url<E: Into<Endpoint>>(self, endpoint: E) -> Self {
        match endpoint.into() {
            Endpoint::Public => Self {
                orderbook_url: "wss://api.bullish.com/trading-api/v1/market-data/orderbook"
                    .to_string(),
                ..self
            },
            Endpoint::Testnet => Self {
                orderbook_url:
                    "wss://api.simnext.bullish-test.com/trading-api/v1/market-data/orderbook"
                        .to_string(),
                ..self
            },
            Endpoint::Custom(orderbook_url) => Self {
                orderbook_url: orderbook_url,
                ..self
            },
        }
    }

    /// Sets the public trades websocket
    pub fn trades_url<E: Into<Endpoint>>(self, endpoint: E) -> Self {
        match endpoint.into() {
            Endpoint::Public => Self {
                trades_url: "wss://api.bullish.com/trading-api/v1/market-data/trades".to_string(),
                ..self
            },
            Endpoint::Testnet => Self {
                trades_url: "wss://api.simnext.bullish-test.com/trading-api/v1/market-data/trades"
                    .to_string(),
                ..self
            },
            Endpoint::Custom(trades_url) => Self {
                trades_url: trades_url,
                ..self
            },
        }
    }

    /// Sets the public trades websocket
    /// /trading-api/v1/private-data
    pub fn private_url<E: Into<Endpoint>>(self, endpoint: E) -> Self {
        match endpoint.into() {
            Endpoint::Public => Self {
                private_url: "wss://api.bullish.com/trading-api/v1/private-data".to_string(),
                ..self
            },
            Endpoint::Testnet => Self {
                private_url: "wss://api.simnext.bullish-test.com/trading-api/v1/private-data"
                    .to_string(),
                ..self
            },
            Endpoint::Custom(private_url) => Self {
                private_url: private_url,
                ..self
            },
        }
    }

    /// Sets the API key
    pub fn api_key(self, api_key: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            ..self
        }
    }

    /// Sets the secret key
    pub fn secret(self, secret: &str) -> Self {
        Self {
            secret: secret.to_string(),
            ..self
        }
    }

    /// Adds an additional topic to receive through the public WebSocket stream.
    pub fn add_topic(mut self, topic: &str) -> Self {
        self.topics.insert(topic.to_string());
        self
    }

    /// Sets the order prefix, which is used to differentiate the orders submitted through this
    /// connector.
    pub fn order_prefix(self, order_prefix: &str) -> Self {
        Self {
            order_prefix: order_prefix.to_string(),
            ..self
        }
    }

    /// Builds [`Bullish`] connector.
    pub fn build(self) -> Result<Bullish, BuildError> {
        if self.orderbook_url.is_empty() {
            return Err(BuildError::BuilderIncomplete("orderbook_url"));
        }
        if self.trades_url.is_empty() {
            return Err(BuildError::BuilderIncomplete("trades_url"));
        }
        if self.private_url.is_empty() {
            return Err(BuildError::BuilderIncomplete("private_url"));
        }
        if self.rest_url.is_empty() {
            return Err(BuildError::BuilderIncomplete("rest_url"));
        }
        if self.api_key.is_empty() {
            return Err(BuildError::BuilderIncomplete("api_key"));
        }
        if self.secret.is_empty() {
            return Err(BuildError::BuilderIncomplete("secret"));
        }
        if self.order_prefix.contains("/") {
            panic!("order prefix cannot include '/'.");
        }
        if self.order_prefix.len() > 8 {
            panic!("order prefix length should be not greater than 8.");
        }
        let rt = Runtime::new().unwrap();
        let mut client = BullishClient::new(&self.rest_url, &self.api_key, &self.secret);
        let jwt = rt.block_on(client.refresh_jwt()).unwrap();
        let trading_accounts = rt.block_on(client.configure_trading_account()).unwrap();
        let mut trading_account_id = String::default();
        if let Some(trading_account) = trading_accounts.first() {
            trading_account_id = trading_account.trading_account_id.clone();
        }

        Ok(Bullish {
            orderbook_url: self.orderbook_url,
            trades_url: self.trades_url,
            private_url: self.private_url,
            api_key: self.api_key.clone(),
            secret: self.secret.clone(),
            jwt: jwt.token,
            trading_account_id,
            inv_assets: Default::default(),
            order_tx: None,
            order_man: Arc::new(Mutex::new(OrderManager::new(&self.order_prefix))),
            client: client,
            assets: Default::default(),
            topics: self.topics,
            order_prefix: self.order_prefix,
        })
    }
}

pub struct Bullish {
    orderbook_url: String,
    trades_url: String,
    private_url: String,
    api_key: String,
    secret: String,
    jwt: String,
    trading_account_id: String,
    order_tx: Option<UnboundedSender<Order>>,
    inv_assets: HashMap<usize, Asset>,
    client: BullishClient,
    order_man: OrderManagerWrapper,
    assets: HashMap<String, Asset>,
    topics: HashSet<String>,
    order_prefix: String,
}

impl Bullish {
    pub fn builder() -> BullishBuilder {
        BullishBuilder {
            orderbook_url: "".to_string(),
            trades_url: "".to_string(),
            private_url: "".to_string(),
            rest_url: "".to_string(),
            api_key: "".to_string(),
            secret: "".to_string(),
            topics: Default::default(),
            order_prefix: "".to_string(),
        }
    }
}

impl Connector for Bullish {
    /// Runs the connector, establishing the connection and preparing to exchange information such
    /// as data feed and orders. This method should not block, and any response should be returned
    /// through the channel using [`LiveEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn add(
        &mut self,
        asset_no: usize,
        symbol: String,
        tick_size: f64,
        lot_size: f64,
    ) -> Result<(), anyhow::Error> {
        let asset_info = Asset {
            asset_no,
            symbol: symbol.clone(),
            tick_size,
            lot_size,
        };
        self.assets.insert(symbol, asset_info.clone());
        self.inv_assets.insert(asset_no, asset_info);
        Ok(())
    }

    /// Submits a new order. This method should not block, and the response should be returned
    /// through the channel using [`LiveEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn submit(
        &self,
        asset_no: usize,
        mut order: Order,
        tx: Sender<LiveEvent>,
    ) -> Result<(), anyhow::Error> {
        let asset_info = self
            .inv_assets
            .get(&asset_no)
            .ok_or(BullishError::AssetNotFound)?;
        let symbol = asset_info.symbol.clone();
        let client = self.client.clone();
        let order_man = self.order_man.clone();
        let trading_account_id = self.trading_account_id.clone();
        tokio::spawn(async move {
            let client_order_id = order_man
                .lock()
                .unwrap()
                .prepare_client_order_id(asset_no, order.clone());

            match client_order_id {
                Some(client_order_id) => {
                    match client
                        .submit_order(
                            &trading_account_id,
                            &client_order_id,
                            &symbol,
                            order.side,
                            order.price_tick as f64 * order.tick_size,
                            get_precision(order.tick_size),
                            order.qty,
                            order.order_type,
                            order.time_in_force,
                        )
                        .await
                    {
                        Ok(resp) => {
                            tracing::debug!(?resp, "SubmitOrderResponse");
                            if !resp.message.starts_with("Command acknowledged") {
                                error!(?resp.message, "message");
                                // todo: find a better way to propagate this error
                                let err = BullishError::OrderError(1, resp.message);
                                let order = order_man.lock().unwrap().update_submit_fail(
                                    asset_no,
                                    order,
                                    &err,
                                    client_order_id,
                                );
                                if let Some(order) = order {
                                    tx.send(LiveEvent::Order {
                                        asset_no,
                                        order: Order {
                                            status: Status::Expired,
                                            req: Status::None,
                                            ..order
                                        },
                                    })
                                    .unwrap();
                                }
                                tx.send(LiveEvent::Error(LiveError::with(
                                    ErrorKind::OrderError,
                                    err.into(),
                                )))
                                .unwrap();
                            } else {
                                let order = order_man.lock().unwrap().update_submit_success(
                                    asset_no,
                                    order,
                                    client_order_id,
                                );
                                if let Some(order) = order {
                                    tx.send(LiveEvent::Order { asset_no, order }).unwrap();
                                }
                            }
                        }
                        Err(error) => {
                            tracing::error!(?error, "Error submiting order");
                            let order = order_man.lock().unwrap().update_submit_fail(
                                asset_no,
                                order,
                                &error,
                                client_order_id,
                            );
                            if let Some(order) = order {
                                tx.send(LiveEvent::Order { asset_no, order }).unwrap();
                            }
                            tx.send(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.into(),
                            )))
                            .unwrap();
                        }
                    };
                }
                None => {
                    tracing::trace!(
                        ?order,
                        "Coincidentally, creates a duplicated client order id. \
                        This order request will be expired."
                    );
                    order.req = Status::None;
                    order.status = Status::Expired;
                    tx.send(LiveEvent::Order { asset_no, order }).unwrap();
                }
            }
        });
        Ok(())
    }

    /// Cancels an open order. This method should not block, and the response should be returned
    /// through the channel using [`LiveEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn cancel(
        &self,
        asset_no: usize,
        mut order: Order,
        tx: Sender<LiveEvent>,
    ) -> Result<(), anyhow::Error> {
        let asset_info = self
            .inv_assets
            .get(&asset_no)
            .ok_or(BullishError::AssetNotFound)?;
        let symbol = asset_info.symbol.clone();
        let client = self.client.clone();
        let orders = self.order_man.clone();
        tokio::spawn(async move {
            let client_order_id = orders.lock().unwrap().get_client_order_id(order.order_id);

            match client_order_id {
                Some(client_order_id) => {
                    match client.cancel_order(&client_order_id, &symbol).await {
                        Ok(resp) => {
                            let order = orders.lock().unwrap().update_cancel_success(
                                asset_no,
                                order,
                                client_order_id,
                            );
                            if let Some(order) = order {
                                tx.send(LiveEvent::Order { asset_no, order }).unwrap();
                            }
                        }
                        Err(error) => {
                            let order = orders.lock().unwrap().update_cancel_fail(
                                asset_no,
                                order,
                                &error,
                                client_order_id,
                            );
                            if let Some(order) = order {
                                tx.send(LiveEvent::Order { asset_no, order }).unwrap();
                            }

                            tx.send(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.into(),
                            )))
                            .unwrap();
                        }
                    }
                }
                None => {
                    tracing::debug!(
                        order_id = order.order_id,
                        "client_order_id corresponding to order_id is not found; \
                        this may be due to the order already being canceled or filled."
                    );
                    // order.req = Status::None;
                    // order.status = Status::Expired;
                    // tx.send(Event::Order(OrderResponse { asset_no, order }))
                    //     .unwrap();
                }
            }
        });
        Ok(())
    }

    fn run(&mut self, ev_tx: Sender<LiveEvent>) -> Result<(), anyhow::Error> {
        let private_url = self.private_url.clone();
        let ev_tx_private = ev_tx.clone();
        let assets_private = self.assets.clone();
        let api_key_private = self.api_key.clone();
        let secret_private = self.secret.clone();
        let prefix_private = self.order_prefix.clone();
        let order_manager_private = self.order_man.clone();
        let assets_private = self.assets.clone();
        let jwt_private = self.jwt.clone();
        let trading_account_id = self.trading_account_id.clone();

        let mut topics = vec![
            "orders".to_string(),
            "trades".to_string(),
            "assetAccounts".to_string(),
            "tradingAccounts".to_string(),
            "heartbeat".to_string(),
        ];
        let mut client_private = self.client.clone();
        for topic in self.topics.iter() {
            topics.push(topic.clone());
        }

        // Private data setup
        let _ = tokio::spawn(async move {
            let mut error_count = 0;

            'connection: loop {
                if error_count > 0 {
                    error!(?error_count, "ErrorCount");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                // Cancel all orders before connecting to the stream
                // in order to start with the clean state
                for (symbol, _) in assets_private.iter() {
                    if let Err(error) = client_private.cancel_all_orders(symbol).await {
                        error!(?error, %symbol, "Couldn't cancel all open orders.");
                        ev_tx_private
                            .send(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.into(),
                            )))
                            .unwrap();
                        error_count += 1;
                        continue 'connection;
                    }
                }
                {
                    let orders = order_manager_private.lock().unwrap().clear_orders();
                    for (asset_no, order) in orders {
                        ev_tx_private
                            .send(LiveEvent::Order { asset_no, order })
                            .unwrap();
                    }
                }

                // Bullish does not have risk position by market besides perpetuals
                // so send zero position to bot for everything else
                {
                    for (symbol, asset_info) in assets_private.iter() {
                        if !asset_info.symbol.ends_with("PERP") {
                            ev_tx_private
                                .send(LiveEvent::Position {
                                    asset_no: asset_info.asset_no,
                                    qty: 0.0,
                                })
                                .unwrap();
                        }
                    }
                }
                tracing::info!("connecting to private data...");
                if let Err(error) = connect_private(
                    &private_url,
                    jwt_private.clone(),
                    ev_tx_private.clone(),
                    assets_private.clone(),
                    topics.clone(),
                    &prefix_private,
                    order_manager_private.clone(),
                )
                .await
                {
                    error!(?error, "A connection error occurred.");
                    ev_tx_private
                        .send(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        )))
                        .unwrap();
                } else {
                    ev_tx_private
                        .send(LiveEvent::Error(LiveError::new(
                            ErrorKind::ConnectionInterrupted,
                        )))
                        .unwrap();
                }
                error_count += 1;
            }
        });

        // Connect to anonymous (public) orderbook data websocket
        let orderbook_url = self.orderbook_url.clone();
        let ev_tx_orderbook = ev_tx.clone();
        let assets_orderbook = self.assets.clone();
        let mut topics = vec!["l2Orderbook".to_string(), "l1Orderbook".to_string()];
        for topic in self.topics.iter() {
            topics.push(topic.clone());
        }

        let _ = tokio::spawn(async move {
            let mut error_count = 0;
            loop {
                if error_count > 0 {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }

                if let Err(error) = connect_orderbook(
                    &orderbook_url,
                    ev_tx_orderbook.clone(),
                    assets_orderbook.clone(),
                    topics.clone(),
                )
                .await
                {
                    error!(?error, "A connection error occurred.");
                    ev_tx_orderbook
                        .send(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        )))
                        .unwrap();
                } else {
                    ev_tx_orderbook
                        .send(LiveEvent::Error(LiveError::new(
                            ErrorKind::ConnectionInterrupted,
                        )))
                        .unwrap();
                }
                error_count += 1;
            }
        });

        // Connect to anonymous (public) trades data websocket
        let trades_url = self.trades_url.clone();
        let ev_tx_trades = ev_tx.clone();
        let assets_trades = self.assets.clone();
        let client_trades = self.client.clone();

        let mut topics = vec!["anonymousTrades".to_string()];
        for topic in self.topics.iter() {
            topics.push(topic.clone());
        }

        let _ = tokio::spawn(async move {
            let mut error_count = 0;
            loop {
                if error_count > 0 {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                if let Err(error) = connect_trades(
                    &trades_url,
                    ev_tx_trades.clone(),
                    assets_trades.clone(),
                    topics.clone(),
                )
                .await
                {
                    error!(?error, "A connection error occurred.");
                    ev_tx_trades
                        .send(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        )))
                        .unwrap();
                } else {
                    ev_tx_trades
                        .send(LiveEvent::Error(LiveError::new(
                            ErrorKind::ConnectionInterrupted,
                        )))
                        .unwrap();
                }
                error_count += 1;
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple() {
        let mut bullish = Bullish::builder()
            .endpoint(Endpoint::Testnet)
            .build()
            .unwrap();

        let (ev_tx, _) = channel();

        bullish.run(ev_tx.clone()).unwrap();
    }
}

#[derive(Error, Debug)]
pub enum BullishError {
    #[error("asset not found")]
    AssetNotFound,
    #[error("invalid request")]
    InvalidRequest,
    #[error("http error: {0:?}")]
    ReqError(#[from] reqwest::Error),
    #[error("error({1}) at order_id({0})")]
    OrderError(i64, String),
}

impl Into<Value> for BullishError {
    fn into(self) -> Value {
        match self {
            BullishError::AssetNotFound => Value::String(self.to_string()),
            BullishError::InvalidRequest => Value::String(self.to_string()),
            BullishError::ReqError(err) => err.into(),
            BullishError::OrderError(code, msg) => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(code));
                map.insert("msg".to_string(), Value::String(msg));
                map
            }),
        }
    }
}

