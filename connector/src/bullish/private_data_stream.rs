
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::types::LiveEvent;
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::UnboundedSender,
    },
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{debug, error, trace};

use crate::{
    bullish::{
        msg::ws::{BullishWebSocketResponse, PrivateResponse, PrivateAssetAccount, PrivateHeartBeat, PrivateOrder, PrivatePerpetualPosition, PrivateStreamMsg, PrivateTrade, PrivateTradingAccount},
        ordermanager::SharedOrderManager,
        rest::BullishClient,
        BullishError,
    },
    connector::PublishEvent,
};

use super::Bullish;


pub struct PrivateDataStream {
    api_key: String,
    secret: String,
    ev_tx: UnboundedSender<PublishEvent>,
    order_manager: SharedOrderManager,
    //symbols: SharedSymbolSet,
    client: BullishClient,
    symbol_rx: Receiver<String>,
}

impl PrivateDataStream {
    pub fn new(
        api_key: String,
        secret: String,
        ev_tx: UnboundedSender<PublishEvent>,
        order_manager: SharedOrderManager,
        //symbols: SharedSymbolSet,
        client: BullishClient,
        symbol_rx: Receiver<String>,
    ) -> Self {
        Self {
            api_key,
            secret,
            ev_tx,
            order_manager,
            //symbols,
            client,
            symbol_rx,
        }
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BullishError> {

        // TODO: fix this error handling
        debug!("Authenticating...");
        let jwt= self.client.refresh_jwt().await.unwrap();
        let token = jwt.token.clone();
        // TODO: Trading account configuration
        let trading_accounts = self.client.configure_trading_account().await.unwrap();
        let mut trading_account_id = String::default();
        if let Some(trading_account) = trading_accounts.first() {
            trading_account_id = trading_account.trading_account_id.clone();
        }
        debug!(?token, "JWT");
        debug!(?trading_account_id, "TA ID");


        debug!("Connecting to private data feed...");
        let mut request = url.into_client_request()?;
        let headers = request.headers_mut();
        // wonky auth requires header set f"cookie=JWT_COOKIE={JWT_TOKEN};";
        headers.append("cookie", format!("JWT_COOKIE={token}").parse().unwrap());
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(15));
        debug!("Connected to private data feed...");

        let id = Utc::now().timestamp_micros().to_string();
        let sub = format!(r#"{{
            "jsonrpc":"2.0",
            "method":"subscribe",
            "type":"command",
            "params": {{
                "topic": "heartbeat"
            }},
            "id":"{id}"
        }}"#).into();
        write.send(Message::Text(sub)).await?;

        let id = Utc::now().timestamp_micros().to_string();
        let sub = format!(r#"{{
            "jsonrpc": "2.0",
            "method": "subscribe",
            "type": "command",
            "id": "{id}",
            "params": {{
                "topic": "orders",
                "tradingAccountId": "{trading_account_id}"
            }}
        }}"#).into();
        write.send(Message::Text(sub)).await?;

        let id = Utc::now().timestamp_micros().to_string();
        write.send(Message::Text(format!(r#"{{
            "jsonrpc":"2.0",
            "method":"subscribe",
            "type":"command",
            "params": {{
                "topic":"trades",
                "tradingAccountId":"{trading_account_id}"
            }},
            "id":"{id}"
        }}"#).into())).await?;

        // let id = Utc::now().timestamp_micros().to_string();
        // write.send(Message::Text(format!(r#"{{
        //     "jsonrpc":"2.0",
        //     "method":"subscribe",
        //     "type":"command",
        //     "params": {{
        //         "topic":"assetAccounts",
        //         "tradingAccountId":"{trading_account_id}"
        //     }},
        //     "id":"{id}"
        // }}"#).into())).await?;

        // let id = Utc::now().timestamp_micros().to_string();
        // write.send(Message::Text(format!(r#"{{
        //     "jsonrpc":"2.0",
        //     "method":"subscribe",
        //     "type":"command",
        //     "params": {{
        //         "topic":"tradingAccounts",
        //         "tradingAccountId":"{trading_account_id}"
        //     }},
        //     "id":"{id}"
        // }}"#).into())).await?;

        // let id = Utc::now().timestamp_micros().to_string();
        // write.send(Message::Text(format!(r#"{{
        //     "jsonrpc":"2.0",
        //     "method":"subscribe",
        //     "type":"command",
        //     "params": {{
        //         "topic":"derivativesPositionsV2",
        //         "tradingAccountId":"{trading_account_id}"
        //     }},
        //     "id":"{id}"
        // }}"#).into())).await?;

        // let id = Utc::now().timestamp_micros().to_string();
        // write.send(Message::Text(format!(r#"{{
        //     "jsonrpc":"2.0",
        //     "method":"subscribe",
        //     "type":"command",
        //     "params": {{
        //         "topic":"ammInstructions",
        //         "tradingAccountId":"{trading_account_id}"
        //     }},
        //     "id":"{id}"
        // }}"#).into())).await?;

        loop {
            select! {
                _ = interval.tick() => {
                    let id = Utc::now().timestamp_micros().to_string();
                    let keep_alive = format!(r#"{{
                        "jsonrpc": "2.0",
                        "type": "command",
                        "method": "keepalivePing",
                        "params": {{}},
                        "id": "{id}"
                    }}"#).into();
                    write.send(Message::Text(keep_alive)).await?;
                },
                msg = self.symbol_rx.recv() => {
                    match msg {
                        Ok(symbol) => {
                            tracing::info!(?symbol, "New symbol registered");
                            let client = self.client.clone();
                            let order_manager = self.order_manager.clone();
                            let ev_tx = self.ev_tx.clone();

                            tokio::spawn(async move {
                                // Cancel all orders in order to start with the clean state.
                                if let Err(error) = cancel_all(
                                    client.clone(),
                                    symbol.clone(),
                                    order_manager.clone(),
                                    ev_tx.clone()
                                ).await {
                                    error!(
                                        ?error,
                                        %symbol,
                                        "Couldn't cancel all orders."
                                    );
                                }

                                // Fetches the initial states such as positions and open orders.
                                if let Err(error) = get_position(
                                    client.clone(),
                                    symbol.clone(),
                                    ev_tx.clone()
                                ).await {
                                    error!(
                                        ?error,
                                        %symbol,
                                        "Couldn't get position"
                                    );
                                }
                            });
                        }
                        Err(RecvError::Closed) => {
                            return Ok(());
                        }
                        Err(RecvError::Lagged(num)) => {
                            error!("{num} subscription requests were missed.");
                        }
                    }
                },
                //PrivateStreamMsg::BullishWebSocketResponse
                message = read.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<PrivateStreamMsg>(&text) {
                            Ok(PrivateStreamMsg::BullishWebSocketResponse(stream)) => {
                                self.handle_private_stream(stream);
                            }
                            Ok(PrivateStreamMsg::JsonRpc(result)) => {
                                trace!(?result, "Subscription request response is received.");
                            },
                            Ok(PrivateStreamMsg::JsonRpcError(result)) => {
                                error!(?result, "Subscription error");
                            },
                            Ok(PrivateStreamMsg::PrivateErrorResponse(error))  => {
                                error!(?error, %text, "Got an error response");
                            },
                            Err(error) => {
                                error!(?error, %text, "Couldn't parse Stream.");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Close(close_frame))) => {
                        return Err(BullishError::ConnectionAbort(
                            close_frame.map(|f| f.to_string()).unwrap_or(String::new())
                        ));
                    }
                    Some(Ok(Message::Binary(_)))
                    | Some(Ok(Message::Frame(_)))
                    | Some(Ok(Message::Pong(_))) => {}
                    Some(Err(error)) => {
                        return Err(BullishError::from(error));
                    }
                    None => {
                        return Err(BullishError::ConnectionInterrupted);
                    }
                }
            }
        }       

        // TODO logout w/ JWT
    }

    fn handle_private_stream(
        &self,
        stream: BullishWebSocketResponse
    ) -> Result<(), BullishError> {
        if stream.event_topic.starts_with("V1TAOrder") && stream.event_type.starts_with("update") {
            let data: PrivateOrder = serde_json::from_value(stream.data).unwrap();
            match self.order_manager.lock().unwrap().update_from_ws(&data) {
                Ok(Some(order)) => {
                    tracing::info!(?order);
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Order {
                            symbol: data.symbol,
                            order,
                        }))
                        .unwrap();
                        Ok(())
                }
                Ok(None) => {
                    // This order is already deleted.
                    Ok(())
                }
                Err(BullishError::PrefixUnmatched) => {
                    // This order is not created by this connector.
                    Err(BullishError::PrefixUnmatched)
                }
                Err(error) => {
                    error!(
                        ?error,
                        ?data,
                        "Couldn't update the order from OrderTradeUpdate message."
                    );
                    Err(error)
                }
            }
        } else if stream.event_topic.starts_with("V1TATrade") && stream.event_type.starts_with("update") {
            let data: PrivateTrade = serde_json::from_value(stream.data).unwrap();
            return Ok(());
        } else if stream.event_topic.starts_with("V1TAAssetAccount") && stream.event_type.starts_with("update") {
            let data: PrivateAssetAccount = serde_json::from_value(stream.data).unwrap();
            return Ok(());
        } else if stream.event_topic.starts_with("V1TADerivativesPosition") && stream.event_type.starts_with("update") {
            let data: PrivatePerpetualPosition = serde_json::from_value(stream.data).unwrap();
            return Ok(());
        } else if stream.event_topic.starts_with("V1TATradingAccount") && stream.event_type.starts_with("update") {
            let data: PrivateTradingAccount = serde_json::from_value(stream.data).unwrap();
            return Ok(());
        } else if stream.event_topic.starts_with("V1TAHeartbeat") && stream.event_type.starts_with("update") {
            let data: PrivateHeartBeat = serde_json::from_value(stream.data).unwrap();
            tracing::debug!(?data, "V1TAHeartbeat");
            return Ok(());
        } else if stream.event_topic.starts_with("V1TAResponse") && stream.event_type.starts_with("response") {
            let data: PrivateResponse = serde_json::from_value(stream.data).unwrap();
            return Ok(());
        } else {
            tracing::trace!(?stream.data, "Unknown Private Data");
            return Ok(());
        }
    }

}

pub async fn get_position(
    client: BullishClient,
    symbol: String,
    ev_tx: UnboundedSender<PublishEvent>,
) -> Result<(), BullishError> {
    // todo: rate-limit throttling.
    let position = client.get_position_information(&symbol).await.unwrap();
    position.into_iter().for_each(|position| {
        let qty = match position.side.as_str() {
            "Buy" => position.quantity,
            "Sell" => -position.quantity,
            _ => {
                if position.quantity!= 0.0 {
                    panic!("Unknown position side. position={position:?}");
                }
                0.0
            }
        };
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Position {
                symbol: symbol.to_string(),
                qty,
                exch_ts: position.updated_at_timestamp,
            }))
            .unwrap();
    });
    Ok(())
}

pub async fn cancel_all(
    client: BullishClient,
    symbol: String,
    order_manager: SharedOrderManager,
    ev_tx: UnboundedSender<PublishEvent>,
) -> Result<(), BullishError> {
    // todo: rate-limit throttling.
    client.cancel_all_orders(&symbol).await.unwrap();
    let orders = order_manager.lock().unwrap().cancel_all(&symbol);
    for order in orders {
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Order {
                symbol: symbol.clone(),
                order,
            }))
            .unwrap();
    }
    Ok(())
}