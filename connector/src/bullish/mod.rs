use serde::Deserialize;
use tokio_tungstenite::tungstenite;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
    },
};

use tokio::sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender};
use thiserror::Error;
use tracing::{warn,error};
// use ws::{connect_orderbook, connect_private, connect_trades};

use crate::{
    bullish::{
        ordermanager::{OrderManager, SharedOrderManager},
        rest::BullishClient,
    },
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent}, 
    utils::{ExponentialBackoff, Retry},
};

use hftbacktest::{
    prelude::get_precision, types::{ErrorKind, LiveError, LiveEvent, Order, Status, Value}
};

mod msg;
mod ordermanager;
mod rest;
mod market_data_stream;
mod trade_data_stream;
mod private_data_stream;

#[derive(Error, Debug)]
pub enum BullishError {
    #[error("asset not found")]
    AssetNotFound,
    #[error("invalid request")]
    InvalidRequest,
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("Feed Error")]
    FeedError(#[from] serde_json::Error),
    #[error("ReqError: {0:?}")]
    ReqError(#[from] reqwest::Error),
    #[error("error({1}) at order_id({0})")]
    OrderError(i64, String),
    #[error("Tunstenite: {0:?}")]
    Tunstenite(#[from] tungstenite::Error),
    #[error("PrefixUnmatched")]
    PrefixUnmatched,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}


impl From<BullishError> for Value {
    fn from(value: BullishError) -> Value {
        match value {
            BullishError::AssetNotFound => Value::String(value.to_string()),
            BullishError::InvalidRequest => Value::String(value.to_string()),
            BullishError::ReqError(error) => {
                let mut map = HashMap::new();
                if let Some(code) = error.status() {
                    map.insert("status_code".to_string(), Value::String(code.to_string()));
                }
                map.insert("msg".to_string(), Value::String(error.to_string()));
                Value::Map(map)
            }
            BullishError::OrderError(code, msg) => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(code));
                map.insert("msg".to_string(), Value::String(msg));
                map
            }),
            BullishError::Tunstenite(error) => Value::String(format!("{error}")),
            BullishError::ConnectionInterrupted => Value::String(value.to_string()),
            BullishError::ConnectionAbort(_) => Value::String(value.to_string()),
            BullishError::FeedError(error) => Value::String(error.to_string()),
            BullishError::Config(_) => Value::String(value.to_string()),
            BullishError::PrefixUnmatched => Value::String(value.to_string()),
            BullishError::OrderNotFound => Value::String(value.to_string()),
        }
    }
}


#[derive(Deserialize)]
pub struct Config {
    orderbook_url: String,
    trades_url: String,
    private_url: String,
    rest_url: String,
    #[serde(default)]
    api_key: String,
    #[serde(default)]
    secret: String,
    #[serde(default)]
    order_prefix: String,
}

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

impl ConnectorBuilder for Bullish {
    type Error = BullishError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        // FIXME 
        let config: Config = toml::from_str(config).unwrap();
        if config.order_prefix.contains("/") {
            panic!("order prefix cannot include '/'.");
        }
        if config.order_prefix.len() > 8 {
            panic!("order prefix length should be not greater than 8.");
        }
        let (symbol_tx, _) = broadcast::channel(500);
        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        // If we want to efficiently use JWT tokens and not hit max sessions, we want to log in once and share that.
        // FIXME we can maybe use signaling to login once and update the tokens on the different instances?
        // let rt = Runtime::new().unwrap();
        let client = BullishClient::new(&config.rest_url, &config.api_key, &config.secret);
        // let jwt = rt.block_on(client.refresh_jwt()).unwrap();
        // let trading_accounts = rt.block_on(client.configure_trading_account()).unwrap();
        // let mut trading_account_id = String::default();
        // if let Some(trading_account) = trading_accounts.first() {
        //     trading_account_id = trading_account.trading_account_id.clone();
        // }

        Ok(Bullish {
            // stuff like url & secrets
            config,
            // http/json api client
            client,
            // oms
            order_manager,
            // what to trade
            symbols: Default::default(),
            // ??
            symbol_tx,
        })
    }
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

pub struct Bullish {
    config: Config,
    symbols: SharedSymbolSet,
    order_manager: SharedOrderManager,
    client: BullishClient,
    symbol_tx: Sender<String>,
}

impl Bullish {

    pub fn connect_market_data_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let base_url = self.config.orderbook_url.clone();
        let client = self.client.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BullishError| {
                    error!(
                        ?error,
                        "An error occurred in the market data stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = market_data_stream::MarketDataStream::new(
                        client.clone(),
                        ev_tx.clone(),
                        symbol_tx.subscribe(),
                    );
                    stream.connect(&base_url).await?;
                    Ok(())
                })
                .await;
        });
    }

    pub fn connect_trade_data_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let base_url = self.config.trades_url.clone();
        let client = self.client.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BullishError| {
                    error!(
                        ?error,
                        "An error occurred in the trade data stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = trade_data_stream::TradeDataStream::new(
                        client.clone(),
                        ev_tx.clone(),
                        symbol_tx.subscribe(),
                    );
                    stream.connect(&base_url).await?;
                    Ok(())
                })
                .await;
        });
    }



    pub fn connect_private_date_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        // Connects to the private stream for the position and order data.
        let private_url = self.config.private_url.clone();
        let api_key = self.config.api_key.clone();
        let secret = self.config.secret.clone();
        let order_manager = self.order_manager.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BullishError| {
                    error!(
                        ?error,
                        "An error occurred in the private data stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = private_data_stream::PrivateDataStream::new(
                        api_key.clone(),
                        secret.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        client.clone(),
                    );
                    stream.connect(&private_url).await?;
                    Ok(())
                })
                .await;
        });
    }


}

impl Connector for Bullish {

    fn register(&mut self, symbol: String) {
        // Binance futures symbols must be lowercase to subscribe to the WebSocket stream.
        let symbol = symbol.to_lowercase();
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            self.symbol_tx.send(symbol).unwrap();
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        self.connect_market_data_stream(ev_tx.clone());
        self.connect_trade_data_stream(ev_tx.clone());
        // Connects to the user stream only if the API key and secret are provided.
        if !self.config.api_key.is_empty() && !self.config.secret.is_empty() {
            self.connect_private_date_stream(ev_tx.clone());
        }
    }
 
    /// Submits a new order. This method should not block, and the response should be returned
    /// through the channel using [`LiveEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn submit(&self, 
        symbol: String, 
        mut order: Order, 
        tx: UnboundedSender<crate::connector::PublishEvent>
    ) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();

        tokio::spawn(async move {
            let client_order_id = order_manager
                .lock()
                .unwrap()
                .prepare_client_order_id(symbol.clone(), order.clone());

            match client_order_id {
                Some(client_order_id) => {
                    let result = client
                        .submit_order(
                            &client_order_id,
                            &symbol,
                            order.side,
                            order.price_tick as f64 * order.tick_size,
                            get_precision(order.tick_size),
                            order.qty,
                            order.order_type,
                            order.time_in_force,
                        )
                        .await;
                    match result {
                        Ok(resp) => {
                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_rest(&client_order_id, &resp)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }
                        }
                        Err(error) => {
                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_submit_fail(&client_order_id, &error)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }

                            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.into(),
                            ))))
                            .unwrap();
                        }
                    }
                }
                None => {
                    warn!(
                        ?order,
                        "Coincidentally, creates a duplicated client order id. \
                        This order request will be expired."
                    );
                    order.req = Status::None;
                    order.status = Status::Expired;
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                        .unwrap();
                }
            }
        });
    }

    /// Cancels an open order. This method should not block, and the response should be returned
    /// through the channel using [`LiveEvent`]. The returned error should not be related to the
    /// exchange; instead, it should indicate a connector internal error.
    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();

        tokio::spawn(async move {
            let client_order_id = order_manager
                .lock()
                .unwrap()
                .get_client_order_id(&symbol, order.order_id);

            match client_order_id {
                Some(client_order_id) => {
                    let result = client.cancel_order(&client_order_id, &symbol).await;
                    match result {
                        Ok(resp) => {
                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_rest(&client_order_id, &resp)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }
                        }
                        Err(error) => {
                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_cancel_fail(&client_order_id, &error)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                                    symbol,
                                    order,
                                }))
                                .unwrap();
                            }

                            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.into(),
                            ))))
                            .unwrap();
                        }
                    }
                }
                None => {
                    warn!(
                        order_id = order.order_id,
                        "client_order_id corresponding to order_id is not found; \
                        this may be due to the order already being canceled or filled."
                    );
                }
            }
        });
    }

}