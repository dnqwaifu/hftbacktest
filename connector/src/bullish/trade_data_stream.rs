use std::{collections::HashMap};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::{live::ipc::TO_ALL, prelude::*};
use tracing::{debug, error, info, trace};
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

use crate::{
    bullish::{
        msg::{
            rest,
            ws::{self, AnonymousTrades, BullishWebSocketResponse, TradesStreamMsg},
        },
        rest::BullishClient,
        BullishError,
    },
    connector::PublishEvent,
};

pub struct TradeDataStream {
    client: BullishClient,
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    pending_depth_messages: HashMap<String, Vec<ws::OrderBookL2>>,
    prev_u: HashMap<String, i64>,
    rest_tx: UnboundedSender<(String, rest::AnonymousTradesResponse)>,
    rest_rx: UnboundedReceiver<(String, rest::AnonymousTradesResponse)>,
}

impl TradeDataStream {
    pub fn new(
        client: BullishClient,
        ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
    ) -> Self {
        let (rest_tx, rest_rx) = unbounded_channel::<(String, rest::AnonymousTradesResponse)>();
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

    fn process_snapshot(&self, symbol: String, data: rest::AnonymousTradesResponse) {
        todo!("Process batches / snapshots");
    }
 

    fn process_message(&mut self, stream: BullishWebSocketResponse) {
        todo!("Process individual messages");
    }
 
    pub async fn connect(&mut self, url: &str) -> Result<(), BullishError> {

        debug!("Connecting to public trades...");
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let interval = tokio::time::interval(tokio::time::Duration::from_secs(15));

        debug!("Connected to public trades stream...");

        let id = Utc::now().timestamp_micros().to_string();
        let sub = format!(r#"{{
            "jsonrpc":"2.0",
            "method":"subscribe",
            "type":"command",
            "params": {{
                "topic":"heartbeat"
            }},
            "id":"{id}"
        }}"#);
        write.send(Message::Text(sub)).await?;

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(15));

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
                    self.process_snapshot(symbol, data);
                }
                msg = self.symbol_rx.recv() => match msg {
                    Ok(symbol) => {
                        let id = Utc::now().timestamp_micros().to_string();
                        let upper_case_symbol = symbol.to_ascii_uppercase();
                        debug!(?symbol, "Subscribing to orderbook");
                        let sub = format!(r#"{{
                            "jsonrpc": "2.0",
                            "type": "command",
                            "method": "subscribe",
                            "params": {{
                                "symbol": "{upper_case_symbol}",
                                "topic": "anonymousTrades"
                            }},
                            "id": "{id}"
                        }}"#);
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
                        match serde_json::from_str::<TradesStreamMsg>(&text) {
                            Ok(TradesStreamMsg::BullishWebSocketResponse(stream)) => {
                                self.handle_trades_stream(stream);
                            }
                            Ok(TradesStreamMsg::JsonRpc(result)) => {
                                debug!(?result, "Subscription request response is received.");
                            }
                            Ok(TradesStreamMsg::JsonRpcError(error)) => {
                                error!(?error, "JsonRpcError")
                            }
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


    fn handle_trades_stream(
        &mut self, 
        stream: BullishWebSocketResponse,
    ) {
        if stream.event_topic.starts_with("V1TAAnonymousTradeUpdate") {
            let data: AnonymousTrades = serde_json::from_value(stream.data).unwrap();
            trace!(?data, "V1TAAnonymousTradeUpdate");
            self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();
            for trade in data.trades {
                self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: data.symbol.clone(),
                    event: Event {
                        ev: {
                            if trade.side == Side::Sell {
                                LOCAL_SELL_TRADE_EVENT
                            } else {
                                LOCAL_BUY_TRADE_EVENT
                            }
                        },
                        exch_ts: trade.created_at_timestamp * 1_000_000,
                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                        order_id: 0,
                        px: trade.price,
                        qty: trade.quantity,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap();
            }
            self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
    } else {
        debug!(?stream.data, "Unhandled event");
    }

    }

}

