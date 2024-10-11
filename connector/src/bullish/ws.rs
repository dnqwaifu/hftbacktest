use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Result;
use std::sync::mpsc::Sender;
use tokio::select;
use tokio::time;
use tokio_tungstenite::tungstenite::Message;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{
    connect_async, tungstenite::client::IntoClientRequest,
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    connector::{
        bullish::{
            msg::ws::{
                AnonymousTrades, OrderBookL1, OrderBookL2, OrderbookStreamMsg, Ping, 
                PrivateErrorResponse, PrivateHeartBeat, PrivateOrder, PrivatePerpetualPosition,
                PrivateStreamMsg, PrivateTrade, PrivateTradingAccount, Subscription,
                TradesStreamMsg, PrivateAssetAccount,
            },
            ordermanager::{HandleError, OrderManager, OrderManagerWrapper},
            BullishError,
        },
        util::{gen_random_string, sign_hmac_sha256},
    },
    live::Asset,
    types::{
        ErrorKind, Event, LiveError, LiveEvent, Order, Side, Status, LOCAL_ASK_DEPTH_BBO_EVENT,
        LOCAL_ASK_DEPTH_EVENT, LOCAL_BID_DEPTH_BBO_EVENT, LOCAL_BID_DEPTH_EVENT,
        LOCAL_BUY_TRADE_EVENT, LOCAL_SELL_TRADE_EVENT,
    },
};

fn parse_depth(
    bids: Vec<(String)>,
    asks: Vec<(String)>,
) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), HandleError> {
    let mut bids_ = Vec::with_capacity(bids.len());
    for slice in bids.iter().collect::<Vec<_>>().chunks(2) {
        bids_.push(parse_px_qty_tup(slice[0], slice[1])?);
        trace!("{:?}", slice);
    }
    let mut asks_ = Vec::with_capacity(asks.len());
    for slice in asks.iter().collect::<Vec<_>>().chunks(2) {
        asks_.push(parse_px_qty_tup(slice[0], slice[1])?);
        trace!("{:?}", slice);
    }
    Ok((bids_, asks_))
}

fn parse_px_qty_tup(px: &String, qty: &String) -> Result<(f64, f64), HandleError> {
    Ok((px.parse()?, qty.parse()?))
}

async fn handle_orderbook_stream(
    text: &str,
    ev_tx: &Sender<LiveEvent>,
    assets: &HashMap<String, Asset>,
) -> Result<(), HandleError> {
    let stream = serde_json::from_str::<OrderbookStreamMsg>(&text)?;
    match stream {
        OrderbookStreamMsg::JsonRpc(stream) => {
            trace!(?stream, "JsonRpc");
        }
        OrderbookStreamMsg::BullishWebSocketResponse(stream) => {
            if stream.event_topic.starts_with("V1TALevel1") {
                let data: OrderBookL1 = serde_json::from_value(stream.data)?;
                let (bids, asks) = parse_depth(data.bid, data.ask).unwrap();
                assert_eq!(bids.len(), 1);
                let asset = assets.get(&data.symbol).ok_or(HandleError::AssetNotFound)?;
                let mut bid_events: Vec<_> = bids
                    .iter()
                    .map(|&(px, qty)| Event {
                            ev: LOCAL_BID_DEPTH_EVENT,
                            exch_ts: data.timestamp * 1_000_000,
                            local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                            order_id: 0,
                            px,
                            qty,
                            ival: 0,
                            fval: 0.0,
                        })
                    .collect();

                    // .map(|&(px, qty)| Event {
                    //     order_id
                    //     ev: LOCAL_BID_DEPTH_BBO_EVENT,
                    //     exch_ts: data.timestamp.parse::<i64>().unwrap() * 1_000_000,
                    //     local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                    //     px,
                    //     qty,
                    // })
                    // .collect();
                assert_eq!(asks.len(), 1);
                let mut ask_events: Vec<_> = asks
                    .iter()
                    .map(|&(px, qty)| Event {
                            ev: LOCAL_ASK_DEPTH_EVENT,
                            exch_ts: data.timestamp * 1_000_000,
                            local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                            order_id: 0,
                            px,
                            qty,
                            ival: 0,
                            fval: 0.0,
                        })
                    .collect();
                let mut events = Vec::new();
                events.append(&mut bid_events);
                events.append(&mut ask_events);
                ev_tx
                    .send(LiveEvent::FeedBatch {
                        asset_no: asset.asset_no,
                        events,
                    })
                    .unwrap();
            } else if stream.event_topic.starts_with("V1TALevel2") {
                let data: OrderBookL2 = serde_json::from_value(stream.data)?;
                let (bids, asks) = parse_depth(data.bids, data.asks).unwrap();
                //assert_eq!(bids.len(), 1);
                let asset = assets.get(&data.symbol).ok_or(HandleError::AssetNotFound)?;
                let mut bid_events: Vec<_> = bids
                    .iter()
                    .map(|&(px, qty)| Event {
                        ev: LOCAL_BID_DEPTH_EVENT,
                        exch_ts: data.timestamp * 1_000_000,
                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                        order_id: 0,
                        px,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    })
                    .collect();
                let mut ask_events: Vec<_> = asks
                    .iter()
                    .map(|&(px, qty)| Event {
                        ev: LOCAL_ASK_DEPTH_EVENT,
                        exch_ts: data.timestamp * 1_000_000,
                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                        px,
                        qty,
                        order_id: 0,
                        ival: 0,
                        fval: 0.0,
                    })
                    .collect();
                let mut events = Vec::new();
                events.append(&mut bid_events);
                events.append(&mut ask_events);
                ev_tx
                    .send(LiveEvent::FeedBatch {
                        asset_no: asset.asset_no,
                        events,
                    })
                    .unwrap();
            } else if stream.event_topic.starts_with("V1TAHeartbeat") {
                trace!(?stream, "Heartbeat");
            }
        }
    }
    Ok(())
}

pub async fn connect_orderbook(
    url: &str,
    ev_tx: Sender<LiveEvent>,
    assets: HashMap<String, Asset>,
    topics: Vec<String>,
) -> Result<(), HandleError> {
    let mut request = url.into_client_request()?;
    let _ = request.headers_mut();
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    tracing::trace!("Connected to public orderbook...");

    let mut interval = time::interval(Duration::from_secs(15));

    let mut params = Vec::new();
    for topic in topics {
        let mut topics_ = assets
            .keys()
            .map(|symbol| {
                HashMap::from([
                    ("topic".to_string(), topic.to_string()),
                    ("symbol".to_string(), symbol.to_string()),
                ])
            })
            .collect();
        params.append(&mut topics_);
    }
    trace!(?params, "Params");

    for param in params {
        let sub = Subscription {
            jsonrpc: "2.0",
            r#type: "command",
            method: "subscribe",
            id: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            params: param,
        };
        let sub = serde_json::to_string(&sub).unwrap();
        trace!(?sub, "Sending Subscription Message");
        write.send(Message::Text(sub)).await?;
    }

    let heartbeat = Subscription {
        jsonrpc: "2.0",
        r#type: "command",
        method: "subscribe",
        id: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string(),
        params: HashMap::from([("topic".to_string(), "heartbeat".to_string())]),
    };
    let s = serde_json::to_string(&heartbeat).unwrap();
    write.send(Message::Text(s)).await?;

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
                        if let Err(error) = handle_orderbook_stream(&text, &ev_tx, &assets).await {
                            error!("Couldn't handle PublicStreamMsg. {:?} {:?}", error, text);
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {}
                    Some(Ok(Message::Ping(_))) => {
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
    tracing::info!("Exiting");
    Ok(())
}

async fn handle_trades_stream(
    text: &str,
    ev_tx: &Sender<LiveEvent>,
    assets: &HashMap<String, Asset>,
) -> Result<(), anyhow::Error> {
    let stream = serde_json::from_str::<TradesStreamMsg>(text)?;
    match stream {
        TradesStreamMsg::JsonRpc(stream) => {
            trace!(?stream, "JsonRpc");
        }
        TradesStreamMsg::BullishWebSocketResponse(stream) => {
            if stream.event_topic.starts_with("V1TAAnonymousTradeUpdate") {
                let data: AnonymousTrades = serde_json::from_value(stream.data)?;
                trace!(?data, "Anonymous trades");
                let asset = assets.get(&data.symbol).ok_or(HandleError::AssetNotFound)?;
                for trade in data.trades {
                    match parse_px_qty_tup(&trade.price, &trade.quantity) {
                        Ok((px, qty)) => {
                            let asset_info = assets
                                .get(&data.symbol)
                                .ok_or(BullishError::AssetNotFound)?;
                            ev_tx
                                .send(LiveEvent::FeedBatch {
                                    asset_no: asset_info.asset_no,
                                    events: vec![Event {
                                        // TODO, what's up with the taker and side relationship?
                                        ev: {
                                            if trade.side.starts_with("BUY") {
                                                LOCAL_BUY_TRADE_EVENT
                                            } else {
                                                LOCAL_SELL_TRADE_EVENT
                                            }
                                        },
                                        exch_ts: data.created_at_timestamp * 1_000_000,
                                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                        order_id: 0,
                                        px,
                                        qty,
                                        ival: 0,
                                        fval: 0.0,
                                    }],
                                })
                                .unwrap();
                        }
                        Err(e) => {
                            error!(error = ?e, "Couldn't parse trade stream.");
                        }
                    }
                }
            }
        }
    };
    Ok(())
}

pub async fn connect_trades(
    url: &str,
    ev_tx: Sender<LiveEvent>,
    assets: HashMap<String, Asset>,
    topics: Vec<String>,
) -> Result<(), HandleError> {
    let mut request = url.into_client_request()?;
    let _ = request.headers_mut();
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    tracing::trace!("Connected to public trades...");

    let mut interval = time::interval(Duration::from_secs(15));

    let mut params = Vec::new();
    for topic in topics {
        let mut topics_ = assets
            .keys()
            .map(|symbol| {
                HashMap::from([
                    ("topic".to_string(), topic.to_string()),
                    ("symbol".to_string(), symbol.to_string()),
                ])
            })
            .collect();
        params.append(&mut topics_);
    }
    trace!(?params, "Params");

    for param in params {
        let sub = Subscription {
            jsonrpc: "2.0",
            r#type: "command",
            method: "subscribe",
            id: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
            params: param,
        };
        let s = serde_json::to_string(&sub).unwrap();
        trace!(?sub, "Sending Subscription Message");
        write.send(Message::Text(s)).await?;
    }

    let heartbeat = Subscription {
        jsonrpc: "2.0",
        r#type: "command",
        method: "subscribe",
        id: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string(),
        params: HashMap::from([("topic".to_string(), "heartbeat".to_string())]),
    };
    let s = serde_json::to_string(&heartbeat).unwrap();
    write.send(Message::Text(s)).await?;

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
                        if let Err(error) = handle_trades_stream(&text, &ev_tx, &assets).await {
                            error!("Couldn't handle TradesStreamMsg. {:?} {:?}", error, text);
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {}
                    Some(Ok(Message::Ping(_))) => {
                        write.send(Message::Pong(Vec::new())).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(close_frame))) => {
                        warn!(?close_frame, "close");
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
    tracing::debug!("Exiting");
    Ok(())
}

pub async fn connect_private(
    url: &str,
    jwt: String,
    ev_tx: Sender<LiveEvent>,
    assets: HashMap<String, Asset>,
    topics: Vec<String>,
    prefix: &str,
    order_man: OrderManagerWrapper,
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
    order_man: &OrderManagerWrapper,
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
