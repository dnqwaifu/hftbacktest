use std::{collections::HashMap};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::{live::ipc::TO_ALL, prelude::*};
use tokio::{
    select,
    sync::{
        broadcast::{error::RecvError, Receiver},
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    },
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{debug, error, info, trace};

use crate::{
    bullish::{
        msg::{
            rest,
            ws::{self, BullishWebSocketResponse, OrderBookL1, OrderBookL2, OrderbookStreamMsg, Subscription},
        },
        rest::BullishClient,
        BullishError,
    },
    utils::{generate_rand_string, parse_depth, parse_px_qty_tup},
    connector::PublishEvent,
};

pub struct MarketDataStream {
    client: BullishClient,
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    pending_depth_messages: HashMap<String, Vec<ws::OrderBookL2>>,
    prev_u: HashMap<String, i64>,
    rest_tx: UnboundedSender<(String, rest::L2OrderbookResponse)>,
    rest_rx: UnboundedReceiver<(String, rest::L2OrderbookResponse)>,
}

impl MarketDataStream {
    pub fn new(
        client: BullishClient,
        ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
    ) -> Self {
        let (rest_tx, rest_rx) = unbounded_channel::<(String, rest::L2OrderbookResponse)>();
        Self {
            client,
            ev_tx,
            symbol_rx,
            pending_depth_messages: Default::default(),
            prev_u: Default::default(),
            rest_tx,
            rest_rx,
        }
    }

    async fn handle_market_data_stream(
        &mut self, 
        stream: BullishWebSocketResponse,
    ) {
        if stream.event_topic.starts_with("V1TALevel1") {
            let data: OrderBookL1 = serde_json::from_value(stream.data).unwrap();
            *self.prev_u
            .entry(data.symbol.clone())
            .or_insert(data.sequence_number) = data.sequence_number;

            match parse_depth(data.bid, data.ask) {
                Ok((bids, asks)) => {
                    for( px, qty) in bids {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed { 
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_BID_DEPTH_EVENT,
                                    exch_ts: data.timestamp * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px,
                                    qty,
                                    ival: 0,
                                    fval: 0.0,
                                }
                        })).unwrap();
                    }
                    for( px, qty) in asks {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed { 
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_BID_DEPTH_EVENT,
                                    exch_ts: data.timestamp * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px,
                                    qty,
                                    ival: 0,
                                    fval: 0.0,
                                }
                        })).unwrap();
                    }
                } 
                Err(error) => {
                    error!(?error, "Could not parse V1TALevel1 event");
                }
            }

        } else if stream.event_topic.starts_with("V1TALevel2") {
            let data: OrderBookL2 = serde_json::from_value(stream.data).unwrap();
            let seq_num = data.sequence_number_range.split_last().unwrap();
            *self.prev_u
            .entry(data.symbol.clone())
            .or_insert(*seq_num.0) = *seq_num.0;

            match parse_depth(data.bids, data.asks) {
                Ok((bids, asks)) => {
                    self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();
                    for( px, qty) in bids {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed { 
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_BID_DEPTH_EVENT,
                                    exch_ts: data.timestamp * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px,
                                    qty,
                                    ival: 0,
                                    fval: 0.0,
                                }
                        })).unwrap();
                    }
                    for( px, qty) in asks {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed { 
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_BID_DEPTH_EVENT,
                                    exch_ts: data.timestamp * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px,
                                    qty,
                                    ival: 0,
                                    fval: 0.0,
                                }
                        })).unwrap();
                    }
                    self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
                } 
                Err(error) => {
                    error!(?error, "Could not parse V1TALevel2 event");
                }
            }
        } else {
            tracing::trace!(?stream.data, "Unhandled event.");
        }
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BullishError> {
        debug!("Connecting to public orderbook...");
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(15));

        debug!("Connected to public orderbook stream...");

        let id = Utc::now().timestamp_micros().to_string();
        write.send(Message::Text(format!(r#"{{
            "jsonrpc":"2.0",
            "method":"subscribe",
            "type":"command",
            "params": {{
                "topic":"heartbeat"
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
                Some((symbol, data)) = self.rest_rx.recv() => {
                    todo!("Need to parse the occational rest updates")
                }
                msg = self.symbol_rx.recv() => match msg {
                    Ok(symbol) => {
                        let id = Utc::now().timestamp_micros().to_string();
                        let sub = format!(r#"{{
                            "jsonrpc": "2.0",
                            "type": "command",
                            "method": "subscribe",
                            "params": {{
                                "symbol": "{symbol}",
                                "topic": "l2Orderbook"
                            }},
                            "id": "{id}"
                        }}"#);
                        info!(?sub, "New subscription");
                        write.send(Message::Text(sub)).await?;
                        let id = Utc::now().timestamp_micros().to_string();
                        let sub = format!(r#"{{
                            "jsonrpc": "2.0",
                            "type": "command",
                            "method": "subscribe",
                            "params": {{
                                "symbol": "{symbol}",
                                "topic": "l1Orderbook"
                            }},
                            "id": "{id}"
                        }}"#);
                        info!(?sub, "New subscription");
                        write.send(Message::Text(sub)).await?;
                    }
                    Err(RecvError::Closed) => {
                        return Ok(());
                    }
                    Err(RecvError::Lagged(num)) => {
                        error!("{num} subscription requests were missed.");
                    }
                },
                message = read.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<OrderbookStreamMsg>(&text) {
                            Ok(OrderbookStreamMsg::BullishWebSocketResponse(stream)) => {
                                self.handle_market_data_stream(stream);
                            }
                            Ok(OrderbookStreamMsg::JsonRpc(result)) => {
                                trace!(?result, "Subscription request response is received.");
                            }
                            Ok(OrderbookStreamMsg::JsonRpcError(error)) => {
                                error!(?error, "JsonRpcError");
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
    }

}