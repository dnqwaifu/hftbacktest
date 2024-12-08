
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    select,
    sync::mpsc::UnboundedSender,
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


pub struct PrivateDataStream {
    api_key: String,
    secret: String,
    ev_tx: UnboundedSender<PublishEvent>,
    order_manager: SharedOrderManager,
    //symbols: SharedSymbolSet,
    client: BullishClient,
}

impl PrivateDataStream {
    pub fn new(
        api_key: String,
        secret: String,
        ev_tx: UnboundedSender<PublishEvent>,
        order_manager: SharedOrderManager,
        //symbols: SharedSymbolSet,
        client: BullishClient,
    ) -> Self {
        Self {
            api_key,
            secret,
            ev_tx,
            order_manager,
            //symbols,
            client,
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


        debug!("Connecting to public orderbook...");
        let mut request = url.into_client_request()?;
        let headers = request.headers_mut();
        // wonky auth requires header set f"cookie=JWT_COOKIE={JWT_TOKEN};";
        headers.append("cookie", format!("JWT_COOKIE={token}").parse().unwrap());
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(15));
        debug!("Connected to public orderbook stream...");

        let id = Utc::now().timestamp_micros().to_string();
        let sub = format!(r#"{{
            "jsonrpc":"2.0",
            "method":"subscribe",
            "type":"command",
            "params": {{
                "topic": "heartbeat"
            }},
            "id":"{id}"
        }}"#);
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
        }}"#);
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
        }}"#))).await?;

        let id = Utc::now().timestamp_micros().to_string();
        write.send(Message::Text(format!(r#"{{
            "jsonrpc":"2.0",
            "method":"subscribe",
            "type":"command",
            "params": {{
                "topic":"assetAccounts",
                "tradingAccountId":"{trading_account_id}"
            }},
            "id":"{id}"
        }}"#))).await?;

        let id = Utc::now().timestamp_micros().to_string();
        write.send(Message::Text(format!(r#"{{
            "jsonrpc":"2.0",
            "method":"subscribe",
            "type":"command",
            "params": {{
                "topic":"tradingAccounts",
                "tradingAccountId":"{trading_account_id}"
            }},
            "id":"{id}"
        }}"#))).await?;

        let id = Utc::now().timestamp_micros().to_string();
        write.send(Message::Text(format!(r#"{{
            "jsonrpc":"2.0",
            "method":"subscribe",
            "type":"command",
            "params": {{
                "topic":"derivativesPositionsV2",
                "tradingAccountId":"{trading_account_id}"
            }},
            "id":"{id}"
        }}"#))).await?;

        let id = Utc::now().timestamp_micros().to_string();
        write.send(Message::Text(format!(r#"{{
            "jsonrpc":"2.0",
            "method":"subscribe",
            "type":"command",
            "params": {{
                "topic":"ammInstructions",
                "tradingAccountId":"{trading_account_id}"
            }},
            "id":"{id}"
        }}"#))).await?;

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
                    }}"#);
                    write.send(Message::Text(keep_alive)).await?;
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
        &mut self,
        stream: BullishWebSocketResponse
    ) -> Result<(), BullishError> {
        if stream.event_topic.starts_with("V1TAOrder") && stream.event_type.starts_with("update") {
            let data: PrivateOrder = serde_json::from_value(stream.data).unwrap();
            return Ok(());
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
            tracing::trace!(?data, "V1TAHeartbeat");
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

/*
pub async fn connect_private(
    url: &str,
    jwt: String,
    ev_tx: Sender<LiveEvent>,
    assets: HashMap<String, Asset>,
    topics: Vec<String>,
    prefix: &str,
    order_man: SharedOrderManager,
) -> Result<(), HandleError> {
    let mut request = url.into_client_request()?;
    let mut headers = request.headers_mut();
    headers.append("cookie", format!("JWT_COOKIE={jwt}").parse().unwrap());

    //f"cookie=JWT_COOKIE={JWT_TOKEN};";
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    let mut interval = time::interval(Duration::from_secs(10));

    for topic in topics {
        let sub = Subscription {
            jsonrpc: "2.0",
            r#type: "command",
            method: "subscribe",
            id: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            params: HashMap::from([("topic".to_string(), topic.to_string())]),
        };

        trace!(?sub, "Sending Subscription Message");
        let s = serde_json::to_string(&sub).unwrap();
        write.send(Message::Text(s)).await?;
    }

    loop {
        select! {
            _ = interval.tick() => {
                let op = Ping {
                  jsonrpc: "2.0",
                  r#type: "command",
                  method: "keepalivePing",
                  id: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string(),
                  params: HashMap::with_capacity(0),
                };
                let s = serde_json::to_string(&op).unwrap();
                write.send(Message::Text(s)).await?;
            },
            message = read.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(error) = handle_private_stream(&text, &ev_tx, &assets, &prefix, &order_man).await {
                            error!("Couldn't handle PrivateStreamMsg. {:?} {:?}", error, text);
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {}
                    Some(Ok(Message::Ping(_))) => {
                        order_man.lock()
                            .unwrap()
                            .gc();
                        write.send(Message::Pong(Vec::new())).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(close_frame))) => {
                        info!(?close_frame, "close");
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(e)) => {
                        return Err(HandleError::from(e));
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_private_stream(
    text: &str,
    ev_tx: &Sender<LiveEvent>,
    assets: &HashMap<String, Asset>,
    prefix: &str,
    order_man: &SharedOrderManager,
) -> Result<(), anyhow::Error> {
    let stream = serde_json::from_str::<PrivateStreamMsg>(text)?;
    match stream {
        PrivateStreamMsg::JsonRpc(stream) => {
            trace!(?stream, "JsonRpc");
        }
        PrivateStreamMsg::BullishWebSocketResponse(stream) => {
            /* Filter by event_topic (exch "dataType") and make updates to order_manager and emit events to ev_tx*/
            if stream.event_topic.starts_with("V1TAOrder") {
                let mut order_man = order_man.lock().unwrap();
                if stream.event_type.starts_with("update") {
                    let update: PrivateOrder = serde_json::from_value(stream.data)?;
                    if let Some(asset_info) = assets.get(&update.symbol) {
                        if let Some(client_order_id) = update.handle.as_ref() {
                            if let Some(order_id) =
                                OrderManager::parse_client_order_id(&client_order_id, &prefix)
                            {
                                trace!(?update, "PrivateOrder");
                                let order = Order {
                                    qty: update.quantity,
                                    leaves_qty: update.quantity - update.quantity_filled.unwrap(),
                                    price_tick: (update.price / asset_info.tick_size).round() as i64,
                                    tick_size: asset_info.tick_size,
                                    side: update.side,
                                    time_in_force: update.time_in_force,
                                    exch_timestamp: update.created_at_timestamp * 1_000_000,
                                    status: update.status_reason,
                                    local_timestamp: 0,
                                    req: Status::None,
                                    exec_price_tick: 0,
                                    exec_qty: update.quantity_filled.unwrap(),
                                    order_id,
                                    order_type: update.order_type,
                                    // Invalid information
                                    q: Box::new(()),
                                    maker: false,
                                };

                                let client_order_id = update.handle.unwrap().clone();
                                let order = order_man.update_from_ws(
                                    asset_info.asset_no,
                                    client_order_id,
                                    order,
                                );
                                if let Some(order) = order {
                                    ev_tx
                                        .send(LiveEvent::Order {
                                            asset_no: asset_info.asset_no,
                                            order: order,
                                        })
                                        .unwrap();
                                }
                            }
                        }
                    }
                } else if stream.event_type.starts_with("snapshot") {
                    let snapshot: Vec<PrivateOrder> = serde_json::from_value(stream.data)?;
                    trace!(?snapshot, "PrivateOrderSnapshot");
                    for item in &snapshot {
                        if let Some(asset_info) = assets.get(&item.symbol) {
                            // TODO this should be item.handle but because we're on a shared account, the bot produces a lot of noise
                            if let Some(client_order_id) = item.handle.as_ref() {
                                if let Some(order_id) =
                                    OrderManager::parse_client_order_id(&client_order_id, &prefix)
                                {
                                    let order = Order {
                                        qty: item.quantity,
                                        leaves_qty: item.quantity - item.quantity_filled.unwrap(),
                                        price_tick: (item.price / asset_info.tick_size).round()
                                            as i64,
                                        tick_size: asset_info.tick_size,
                                        side: item.side,
                                        time_in_force: item.time_in_force,
                                        exch_timestamp: item.created_at_timestamp * 1_000_000,
                                        status: item.status_reason,
                                        local_timestamp: 0,
                                        req: Status::None,
                                        exec_price_tick: (item.average_fill_price.unwrap_or(0.0)
                                            / asset_info.tick_size)
                                            .round()
                                            as i64,
                                        exec_qty: item.quantity_filled.unwrap(),
                                        order_id,
                                        order_type: item.order_type,
                                        // Invalid information
                                        q: Box::new(()),
                                        maker: false,
                                    };

                                    let client_order_id = item.handle.as_ref().unwrap().clone();
                                    let order = order_man.update_from_ws(
                                        asset_info.asset_no,
                                        client_order_id,
                                        order,
                                    );
                                    if let Some(order) = order {
                                        ev_tx
                                            .send(LiveEvent::Order {
                                                asset_no: asset_info.asset_no,
                                                order: order,
                                            })
                                            .unwrap();
                                    }
                                }
                            }
                        }
                    }
                }
            } else if stream.event_topic.starts_with("V1TATrade") {
                // Treat the private trade events as extra market data
                let mut order_man = order_man.lock().unwrap();
                if stream.event_type.starts_with("update") {
                    let update: PrivateTrade = serde_json::from_value(stream.data)?;
                    if let Some(asset_info) = assets.get(&update.symbol) {
                        if let Some(client_order_id) = update.handle.as_ref() {
                            if let Some(order_id) =
                                OrderManager::parse_client_order_id(&client_order_id, &prefix)
                            {
                                info!(?update, "PrivateTrade");
                                let asset_info = assets
                                    .get(&update.symbol)
                                    .ok_or(BullishError::AssetNotFound)?;
                                ev_tx
                                    .send(LiveEvent::Feed {
                                        asset_no: asset_info.asset_no,
                                        event: Event {
                                            ev: {
                                                if update.side == Side::Sell {
                                                    LOCAL_SELL_TRADE_EVENT
                                                } else {
                                                    LOCAL_BUY_TRADE_EVENT
                                                }
                                            },
                                            exch_ts: update.created_at_timestamp * 1_000_000,
                                            local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                            px: update.price,
                                            qty: update.quantity,
                                            order_id,
                                            ival: 0,
                                            fval: 0.0,
                                        },
                                    })
                                    .unwrap();
                            }
                        }
                    }
                } else if stream.event_type.starts_with("snapshot") {
                    let snapshot: Vec<PrivateTrade> = serde_json::from_value(stream.data)?;
                    trace!(?snapshot, "PrivateTradeSnapshot");
                    for item in &snapshot {
                        if let Some(asset_info) = assets.get(&item.symbol) {
                            if let Some(client_order_id) = &item.handle.as_ref() {
                                if let Some(order_id) = OrderManager::parse_client_order_id(
                                    &item.handle.as_ref().unwrap(),
                                    &prefix,
                                ) {
                                    debug!(?item, "PrivateTrade");
                                    let asset_info = assets
                                        .get(&item.symbol)
                                        .ok_or(BullishError::AssetNotFound)?;
                                    ev_tx
                                        .send(LiveEvent::Feed {
                                            asset_no: asset_info.asset_no,
                                            event: Event {
                                                ev: {
                                                    if item.side == Side::Sell {
                                                        LOCAL_SELL_TRADE_EVENT
                                                    } else {
                                                        LOCAL_BUY_TRADE_EVENT
                                                    }
                                                },
                                                exch_ts: item.created_at_timestamp * 1_000_000,
                                                local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                                px: item.price,
                                                qty: item.quantity,
                                                order_id,
                                                ival: 0,
                                                fval: 0.0,
                                            },
                                        })
                                        .unwrap();
                                }
                            }
                        }
                    }
                }
            } else if stream.event_topic.starts_with("V1TAAssetAccount") {
                if stream.event_type.starts_with("update") {
                    let update: PrivateAssetAccount = serde_json::from_value(stream.data)?;
                    debug!(?update, "PrivateAssetAccountUpdate")
                } else if stream.event_type.starts_with("snapshot") {
                    let snapshot: Vec<PrivateAssetAccount> = serde_json::from_value(stream.data)?;
                    debug!(?snapshot, "PrivateAssetAccountUpdate")
                }
            } else if stream.event_topic.starts_with("V1TATradingAccount") {
                if stream.event_type.starts_with("update") {
                    let update: PrivateTradingAccount = serde_json::from_value(stream.data)?;
                    debug!(?update, "PrivateTradingAccountUpdate")
                } else if stream.event_type.starts_with("snapshot") {
                    let snapshot: Vec<PrivateTradingAccount> = serde_json::from_value(stream.data)?;
                    debug!(?snapshot, "PrivateTradingAccountSnapshot")
                }
            } else if stream.event_topic.starts_with("V1TAPerpetualPosition") {
                if stream.event_type.starts_with("update") {
                    let update: PrivatePerpetualPosition = serde_json::from_value(stream.data)?;
                    debug!(?update, "PrivateTradingAccountUpdate")
                } else if stream.event_type.starts_with("snapshot") {
                    let snapshot: Vec<PrivateTradingAccount> = serde_json::from_value(stream.data)?;
                    debug!(?snapshot, "PrivateTradingAccountSnapshot")
                }
            } else if stream.event_topic.starts_with("V1TAHeartbeat") {
                if stream.event_type.starts_with("update") {
                    let update: PrivateHeartBeat = serde_json::from_value(stream.data)?;
                    info!(?update, "PrivateHeartBeatUpdate")
                } else {
                    let snapshot: Vec<PrivateHeartBeat> = serde_json::from_value(stream.data)?;
                    info!(?snapshot, "PrivateHeartBeatSnapshot")
                }
            } else if stream.event_topic.starts_with("V1TAErrorResponse") {
                let mut order_man = order_man.lock().unwrap();
                let error: PrivateErrorResponse = serde_json::from_value(stream.data)?;
                tracing::warn!(?error, "V1TAErrorResponse");
                if let Some(symbol) = error.symbol {
                    if let Some(asset_info) = assets.get(&symbol) {
                        if let Some(client_order_id) = error.handle.as_ref() {
                            if let Some(order_id) = OrderManager::parse_client_order_id(
                                error.handle.as_ref().unwrap(),
                                &prefix,
                            ) {
                                let order = order_man
                                    .update_from_ws_error(asset_info.asset_no, client_order_id);
                                if let Some(order) = order {
                                    ev_tx
                                        .send(LiveEvent::Order {
                                            asset_no: asset_info.asset_no,
                                            order: order,
                                        })
                                        .unwrap();
                                }
                            }
                        }
                    }
                }
            } else {
                warn!(?stream.event_topic, ?stream.data, "Unrecognized topic on private order");
            }
        }
    };
    Ok(())
}
 */